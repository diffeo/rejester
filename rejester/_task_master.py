'''External API for rejester task system.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2013 Diffeo, Inc.

'''
from __future__ import absolute_import
from __future__ import division
import abc
import uuid
import time
import logging
import psutil
import random
import socket
import traceback
import pkg_resources

from rejester._registry import Registry, nice_identifier
from rejester.exceptions import ProgrammerError, LockError, \
    LostLease, EnvironmentError, NoSuchWorkSpecError, NoSuchWorkUnitError

logger = logging.getLogger(__name__)

BUNDLES = 'bundles'
NICE_LEVELS = 'NICE_LEVELS'
WORK_SPECS = 'WORK_SPECS'
WORK_UNITS_ = 'WORK_UNITS_'
_BLOCKED = '_BLOCKED'
_BLOCKS = '_BLOCKS'
_DEPENDS = '_DEPENDS'
_FINISHED = '_FINISHED'
_FAILED = '_FAILED'
WORKER_STATE_ = 'WORKER_STATE_'
WORKER_OBSERVED_MODE = 'WORKER_OBSERVED_MODE'
ACTIVE_LEASES = 'ACTIVE_LEASES'

def build_task_master(config):
    tm_factory_module_name = config.get('task_master_module')
    if tm_factory_module_name is not None:
        import importlib
        tm_factory_module = importlib.import_module(tm_factory_module_name)
        tm_factory_clasname = config.get('task_master_class')
        assert tm_factory_clasname, 'task_master_module set but not task_master_class'
        _task_master_factory = getattr(tm_factory_module, tm_factory_clasname)
        return _task_master_factory(config)
    # by default use implementation later in this file
    return TaskMaster(config)


class Worker(object):
    '''Process that runs rejester jobs.

    Running a worker involves three steps: calling :meth:`register` to
    get a :attr:`worker_id` and record our presence in the data store;
    calling :meth:`run` to actually do work; and calling :meth:`unregister`
    on clean exit.  The :meth:`run` method should periodically call
    :meth:`heartbeat` to update its state and get the current run mode.

    ..automethod:: __init__

    '''

    __metaclass__ = abc.ABCMeta

    def __init__(self, config, task_master=None):
        '''Create a new worker.

        :param dict config: Configuration for the worker, typically
          the contents of the ``rejester`` block in the global config

        '''
        #: Configuration for the worker
        self.config = config
        #: Task interface to talk to the data store
        self.task_master = task_master
        if self.task_master is None:
            self.task_master = build_task_master(self.config)
        #: Worker ID, only valid after :meth:`register`
        self.worker_id = None
        #: Required maximum time between :meth:`heartbeat`
        self.lifetime = 300 * 20 ## 100 minutes

    def environment(self):
        '''Get raw data about this worker.

        This is recorded in the :meth:`heartbeat` info, and can be
        retrieved by :meth:`TaskMaster.get_heartbeat`.  The dictionary
        includes keys ``worker_id``, ``host``, ``fqdn``, ``version``,
        ``working_set``, and ``memory``.

        '''
        hostname, aliases, ipaddrs = socket.gethostbyaddr(socket.gethostname())
        env = dict(
            worker_id = self.worker_id,
            hostname = hostname,
            aliases = tuple(aliases),
            ipaddrs = tuple(ipaddrs),
            fqdn = socket.getfqdn(),
            version = pkg_resources.get_distribution("rejester").version, # pylint: disable=E1103
            working_set = [(dist.key, dist.version) for dist in pkg_resources.WorkingSet()], # pylint: disable=E1103
            #config_hash = self.config['config_hash'],
            #config_json = self.config['config_json'],
            memory = psutil.phymem_usage(),
        )
        return env

    def register(self):
        '''Record the availability of this worker and get a unique identifer.

        This sets :attr:`worker_id` and calls :meth:`heartbeat`.  This
        cannot be called multiple times without calling
        :meth:`unregister` in between.

        '''
        if self.worker_id:
            raise ProgrammerError('Worker.register cannot be called again without first calling unregister; it is not idempotent')
        self.worker_id = nice_identifier()
        self.task_master.worker_id = self.worker_id
        self.heartbeat()
        return self.worker_id

    def unregister(self):
        '''Remove this worker from the list of available workers.

        This requires the worker to already have been :meth:`register()`.

        ''' 
        self.task_master.worker_unregister(self.worker_id)
        self.task_master.worker_id = None
        self.worker_id = None

    def heartbeat(self):
        '''Record the current worker state in the registry.

        This records the worker's current mode, plus the contents of
        :meth:`environment`, in the data store for inspection by others.

        :returns mode: Current mode, as :meth:`TaskMaster.get_mode`

        ''' 
        mode = self.task_master.get_mode()
        self.task_master.worker_heartbeat(self.worker_id, mode, self.lifetime, self.environment())
        return mode

    @abc.abstractmethod
    def run(self):
        '''Run some number of jobs.

        :meth:`register` must have already been called.  This is
        expected to get jobs using :meth:`TaskMaster.get_work` with
        this worker's :attr:`worker_id`.  Depending on the semantics
        of the actual implementing class this may run one job, run
        jobs as long as the worker's mode is :attr:`~TaskMaster.RUN`,
        or any other combination.

        '''
        return


class WorkUnit(object):
    '''A single unit of work being executed.

    These are created by the rejester system; the standard worker
    system will pass objects of this type to the named run function.
    If some code calls:

    .. code-block:: python

        task_master.update_bundle({ 'name': 'work_spec_name', ... },
                                  { 'key': data, ... })

    Then when the work unit is executed, this object will have the
    provided :attr:`work_spec_name`, :attr:`key`, and :attr:`data`,
    with remaining values being provided by the system.

    In the standard worker system, the worker will call :meth:`finish`
    or :meth:`fail` as appropriate.  The run function should examine
    :attr:`spec`, :attr:`key`, and :attr:`data` to figure out what to
    do.

    .. automethod:: __init__

    '''
    def __init__(self, registry, work_spec_name, key, data, worker_id=None,
                 expires=None):
        '''Create a new work unit runtime data.

        In most cases application code will not need to call this directly,
        but should expect to be passed work units created by the system.

        :param registry: Mid-level Redis interface
        :type registry: :class:`rejester.Registry`
        :param str work_spec_name: Name of the work spec
        :param str key: Name of the work unit
        :param dict data: Data provided for the work unit
        :param str worker_id: Worker doing this work unit
        :param int expires: Latest time this work unit can still be running

        '''
        if not worker_id:
            raise ProgrammerError('must specify a worker_id, not: %r' % worker_id)
        #: Worker doing this work unit
        self.worker_id = worker_id
        #: Mid-level Redis interface
        self.registry = registry
        #: Name of the work spec
        self.work_spec_name = work_spec_name
        #: Name of the work unit
        self.key = key
        #: Data provided for the work unit
        self.data = data
        #: Has this work unit called :meth:`finish`?
        self.finished = False
        #: Has this work unit called :meth:`fail`?
        self.failed = False
        #: Time (as :func:`time.time`) when this work unit must finish
        self.expires = expires
        self._spec_cache = None  # storage for lazy getter property
        self._module_cache = None  # storage for lazy getter property

    def __repr__(self):
        return ('WorkUnit(work_spec_name={0.work_spec_name}, key={0.key})'
                .format(self))

    @property
    def spec(self):
        '''Actual work spec.

        This is retrieved from the database on first use, and in some cases
        a worker can be mildly more efficient if it avoids using this.

        '''
        if self._spec_cache is None:
            self._spec_cache = self.registry.get(WORK_SPECS, self.work_spec_name)
        return self._spec_cache

    @property
    def module(self):
        '''Python module to run the job.

        This is used by :func:`run` and the standard worker system.
        If the work spec contains keys ``module``, ``run_function``,
        and ``terminate_function``, then this contains the Python
        module object named as ``module``; otherwise this contains
        :const:`None`.

        '''
        if self._module_cache is None:
            funclist = filter(None, (self.spec.get('run_function'), self.spec.get('terminate_function')))
            if funclist:
                try:
                    self._module_cache = __import__(
                        self.spec['module'], globals(), (), funclist, -1)
                except Exception, exc:
                    logger.error('failed to load spec["module"] = %r',
                                 self.spec['module'], exc_info=True)
                    raise
        return self._module_cache

    def run(self):
        '''Actually runs the work unit.

        This is called by the standard worker system, generally
        once per work unit.  It requires the work spec to contain
        keys ``module``, ``run_function``, and ``terminate_function``.
        It looks up ``run_function`` in :attr:`module` and calls that
        function with :const:`self` as its only parameter.

        '''
        run_function = getattr(self.module, self.spec['run_function'])
        logger.info('running work unit {}'.format(self.key))
        try:
            ret_val = run_function(self)
            self.update()
            logger.info('completed work unit {}'.format(self.key))
            return ret_val
        except Exception, exc:
            logger.error('work unit {} failed'.format(self.key),
                         exc_info=True)
            raise

    def terminate(self):
        '''Kills the work unit.

        This is called by the standard worker system, but only in
        response to an operating system signal.  If the job does setup
        such as creating a child process, its terminate function
        should kill that child process.  More specifically, this
        function requires the work spec to contain the keys
        ``module``, ``run_function``, and ``terminate_function``, and
        calls ``terminate_function`` in :attr:`module` containing
        :const:`self` as its only parameter.

        '''
        terminate_function_name = self.spec.get('terminate_function')
        if not terminate_function_name:
            logger.error('tried to terminate WorkUnit(%r) but no function name', self.key)
            return None
        terminate_function = getattr(self.module, self.spec['terminate_function'])
        if not terminate_function:
            logger.error('tried to terminate WorkUnit(%r) but no function %s in module %r', self.key, terminate_function_name, self.module.__name__)
            return None
        logger.info('calling terminate function for work unit {}'
                    .format(self.key))
        ret_val = terminate_function(self)
        self.update(lease_time=-10)
        return ret_val

    def update(self, lease_time=300):
        '''Refresh this task's expiration time.

        This tries to set the task's expiration time to the current
        time, plus `lease_time` seconds.  It requires the job to not
        already be complete.  If `lease_time` is negative, makes the
        job immediately be available for other workers to run.

        :param int lease_time: time to extend job lease beyond now
        :raises rejester.exceptions.LostLease: if the lease has already
          expired

        '''
        if self.finished:
            raise ProgrammerError('cannot .update() after .finish()')
        if self.failed:
            raise ProgrammerError('cannot .update() after .fail()')
        with self.registry.lock(identifier=self.worker_id) as session:
            if self.finished:
                logger.debug('WorkUnit(%r) became finished while waiting for lock to update', self.key)
                return
            if self.failed:
                logger.debug('WorkUnit(%r) became failed while waiting for lock to update', self.key)
                return
            try:
                self.expires = time.time() + lease_time
                session.update(
                    WORK_UNITS_ + self.work_spec_name,
                    {self.key: self.data}, 
                    priorities={self.key: self.expires},
                    locks={self.key: self.worker_id})

            except EnvironmentError, exc:
                raise LostLease(exc)
                
    def finish(self):
        '''Move this work unit to a finished state.

        In the standard worker system, the worker calls this on the job's
        behalf when :meth:`run_function` returns successfully.  Does nothing
        if the work unit is already complete.

        '''
        if self.finished or self.failed:
            return
        with self.registry.lock(identifier=self.worker_id) as session:
            if self.finished or self.failed:
                return
            logger.debug('WorkUnit finish %r', self.key)
            session.move(
                WORK_UNITS_ + self.work_spec_name,
                WORK_UNITS_ + self.work_spec_name + _FINISHED,
                {self.key: self.data})
            blocks = session.get(WORK_UNITS_ + self.work_spec_name + _BLOCKS,
                                 self.key)
            if blocks is not None:
                for block in blocks:
                    spec = block[0]
                    unit = block[1]
                    hard = block[2]
                    depends = session.get(WORK_UNITS_ + spec + _DEPENDS, unit)
                    if depends is None: continue
                    depends.remove([self.work_spec_name, self.key])
                    if len(depends) == 0:
                        session.popmany(WORK_UNITS_ + spec + _DEPENDS, unit)
                        unitdef = session.get(WORK_UNITS_ + spec + _BLOCKED,
                                              unit)
                        session.move(WORK_UNITS_ + spec + _BLOCKED,
                                     WORK_UNITS_ + spec,
                                     { unit: unitdef })
                    else:
                        session.set(WORK_UNITS_ + spec + _DEPENDS, unit,
                                    depends)
        self.finished = True

    def fail(self, exc=None):
        '''Move this work unit to a failed state.

        In the standard worker system, the worker calls this on the job's
        behalf when :meth:`run_function` ends with any exception:

        .. code-block:: python

            try:
                work_unit.run()
                work_unit.finish()
            except Exception, e:
                work_unit.fail(e)

        A ``traceback`` property is recorded with a formatted version
        of `exc`, if any.  Does nothing if the work unit is already
        complete.

        :param exc: Exception that caused the failure, or :const:`None`

        '''
        if self.failed or self.finished:
            return
        self.data['traceback'] = exc and traceback.format_exc(exc) or exc
        #self.data['worker_state'] = self.worker_state
        with self.registry.lock(identifier=self.worker_id) as session:
            if self.finished or self.failed:
                return
            session.move(
                WORK_UNITS_ + self.work_spec_name,
                WORK_UNITS_ + self.work_spec_name + _FAILED,
                {self.key: self.data})
            blocks = session.get(WORK_UNITS_ + self.work_spec_name + _BLOCKS,
                                 self.key)
            if blocks is not None:
                for block in blocks:
                    spec = block[0]
                    unit = block[1]
                    hard = block[2]
                    if hard:
                        session.popmany(WORK_UNITS_ + spec + _DEPENDS, unit)
                        unitdef = session.get(WORK_UNITS_ + spec + _BLOCKED,
                                              unit)
                        if unitdef is not None:
                            session.move(WORK_UNITS_ + spec + _BLOCKED,
                                         WORK_UNITS_ + spec + _FAILED,
                                         { unit: unitdef })
                    else:
                        depends = session.get(WORK_UNITS_ + spec + _DEPENDS, unit)
                        if depends is None: continue
                        depends.remove([self.work_spec_name, self.key])
                        if len(depends) == 0:
                            session.popmany(WORK_UNITS_ + spec + _DEPENDS, unit)
                            unitdef = session.get(WORK_UNITS_ + spec + _BLOCKED,
                                                  unit)
                            if unitdef is not None:
                                session.move(WORK_UNITS_ + spec + _BLOCKED,
                                             WORK_UNITS_ + spec,
                                             { unit: unitdef })
                        else:
                            session.set(WORK_UNITS_ + spec + _DEPENDS, unit,
                                        depends)
        self.failed = True


class TaskMaster(object):
    '''Control object for the rejester task queue.

    The task queue consists of a series of *work specs*, which include
    configuration information, and each work spec has some number of
    *work units* attached to it.  Both the work specs and work units
    are defined as (non-empty) dictionaries.  The work spec must have
    keys ``name`` and ``min_gb``, but any other properties are permitted.
    Conventionally ``desc`` contains a description of the job and
    ``config`` contains the top-level global configuration.

    There are three ways to use :class:`TaskMaster`:

    1. Create work specs and work units with :meth:`update_bundle`.
       Directly call :meth:`get_work` to get work units back.  Based
       on the information stored in the work spec and work unit
       dictionaries, do the work manually, and call
       :meth:`WorkUnit.finish` or :meth:`WorkUnit.fail` as
       appropriate.

    2. Create work specs and work units with :meth:`update_bundle`.
       The work spec must contain three keys, ``module`` naming a Python
       module, and ``run_function`` and ``terminate_function`` each
       naming functions of a single parameter in that module.  Directly
       call :meth:`get_work` to get work units back, then call their
       :meth:`WorkUnit.run` function to execute them.  See the basic
       example in :meth:`WorkUnit.fail`.

    3. Create work specs and work units with :meth:`update_bundle`,
       including the Python information.  Use one of the standard worker
       implementations in :mod:`rejester.workers` to actually run the
       job.

    Most applications will use the third option, the standard worker
    system.  In all three cases populating and executing jobs can
    happen on different systems, or on multiple systems in parallel.
    To use the standard worker system, create a Python module::

        def rejester_run(work_unit):
            # Does the actual work for `work_unit`.
            # Must be a top-level function in the module.
            # The work unit will succeed if this returns normally,
            # and will fail if this raises an exception.
            pass

        def rejester_terminate(work_unit):
            # Called only if a signal terminates the worker.
            # This usually does nothing, but could kill a known subprocess.
            pass

    The work spec would look something like::

        work_spec = {
            'name': 'rejester_sample',
            'desc': 'A sample rejester job.',
            'min_gb': 1,
            'config': yakonfig.get_global_config(),
            'module': 'name.of.the.module.from.above',
            'run_function': 'rejester_run',
            'terminate_function': 'rejester_terminate',
        }

    The work units can be any non-empty dictionaries that are
    meaningful to the run function.  :class:`WorkUnit` objects are
    created with their :attr:`~WorkUnit.key` and :attr:`~WorkUnit.data`
    fields set to individual keys and values from the parameter to
    :meth:`update_bundle`.

    A work unit can be in one of five states.  It is *available* if it
    has been added to the queue but nobody is working on it.  It is
    *pending* if somebody is currently working on it.  When they
    finish, it will become either *finished* or *failed*.  If
    dependencies are added between tasks using `add_dependent_task()`,
    a task can also be *blocked*.

    The general flow for a rejester application is to :meth:`set_mode`
    to :attr:`IDLE`, then add work units using
    :meth:`update_bundle()`, and :meth:`set_mode` to :attr:`RUN`.
    :meth:`get_work` will return work units until all have been
    consumed.  :meth:`set_mode` to :attr:`TERMINATE` will instruct
    workers to shut down.

    This object keeps very little state locally and can safely be used
    concurrently, including from multiple systems.  Correspondingly,
    any settings here, including :meth:`set_mode`, are persistent even
    beyond the end of the current process.

    .. automethod:: __init__

    '''

    def __init__(self, config):
        '''Create a new task master.

        This is a lightweight object, and it is safe to have multiple
        objects concurrently accessing the same rejester system, even
        on multiple machines.

        :param dict config: Configuration for the task master, generally
          the contents of the ``rejester`` block in the global configuration
        
        '''
        config['app_name'] = 'rejester'
        #: Configuration for the task master
        self.config = config
        #: Mid-level Redis interface
        self.registry = Registry(config)
        #: Worker ID, if this is tied to a worker
        self.worker_id = None

    #: Mode constant instructing workers to do work
    RUN = 'RUN'
    #: Mode constant instructing workers to not start new work
    IDLE = 'IDLE'
    #: Mode constant instructing workers to shut down
    TERMINATE = 'TERMINATE'

    def set_mode(self, mode):
        '''Set the global mode of the rejester system.

        This must be one of the constants :attr:`TERMINATE`,
        :attr:`RUN`, or :attr:`IDLE`.  :attr:`TERMINATE` instructs any
        running workers to do an orderly shutdown, completing current
        jobs then exiting.  :attr:`IDLE` instructs workers to stay
        running but not start new jobs.  :attr:`RUN` tells workers to
        do actual work.

        :param str mode: new rejester mode
        :raise rejester.exceptions.ProgrammerError: on invalid `mode`

        '''
        if not mode in [self.TERMINATE, self.RUN, self.IDLE]:
            raise ProgrammerError('mode=%r is not recognized' % mode)
        with self.registry.lock(identifier=self.worker_id) as session:
            session.set('modes', 'mode', mode)
        logger.info('set mode to %s', mode)

    def get_mode(self):
        '''Get the global mode of the rejester system.

        :return: rejester mode, :attr:`IDLE` if unset

        '''
        return self.registry.get('modes', 'mode') or self.IDLE

    def idle_all_workers(self):
        '''Set the global mode to :attr:`IDLE` and wait for workers to stop.

        This can wait arbitrarily long before returning.  The worst
        case in "normal" usage involves waiting five minutes for a
        "lost" job to expire; a well-behaved but very-long-running job
        can extend its own lease further, and this function will not
        return until that job finishes (if ever).

        .. deprecated:: 0.4.5
            There isn't an obvious use case for this function, and its
            "maybe wait forever for something out of my control" nature
            makes it hard to use in real code.  Polling all of the work
            specs and their :meth:`num_pending` in application code if
            you really needed this operation would have the same
            semantics and database load.

        '''
        self.set_mode(self.IDLE)
        while 1:
            num_pending = dict()
            for work_spec_name in self.registry.pull(NICE_LEVELS).keys():
                num_pending[work_spec_name] = self.num_pending(work_spec_name)
            if sum(num_pending.values()) == 0:
                break
            logger.warn('waiting for pending work_units: %r', num_pending)
            time.sleep(1)

    def mode_counts(self):
        '''Get the number of workers in each mode.

        This returns a dictionary where the keys are mode constants
        and the values are a simple integer count of the number of
        workers in that mode.

        '''
        modes = {self.RUN: 0, self.IDLE: 0, self.TERMINATE: 0}
        for worker_id, mode in self.workers().items():
            modes[mode] += 1
        return modes

    def workers(self, alive=True):
        '''Get a listing of all workers.

        This returns a dictionary mapping worker ID to the mode
        constant for their last observed mode.

        :param bool alive: if true (default), only include workers
          that have called :meth:`Worker.heartbeat` sufficiently recently

        '''
        return self.registry.filter(
            WORKER_OBSERVED_MODE, 
            priority_min=alive and time.time() or '-inf')

    def get_heartbeat(self, worker_id):
        '''Get the last known state of some worker.

        If the worker never existed, or the worker's lifetime has
        passed without it heartbeating, this will return an empty
        dictionary.

        :param str worker_id: worker ID
        :return: dictionary of worker state, or empty dictionary
        :see: :meth:`Worker.heartbeat`

        '''
        return self.registry.pull(WORKER_STATE_ + worker_id)

    def dump(self):
        '''Print the entire contents of this to debug log messages.

        This is really only intended for debugging.  It could produce
        a lot of data.

        '''
        with self.registry.lock(identifier=self.worker_id) as session:
            for work_spec_name in self.registry.pull(NICE_LEVELS).iterkeys():
                def scan(sfx):
                    v = self.registry.pull(WORK_UNITS_ + work_spec_name + sfx)
                    if v is None: return []
                    return v.keys()
                for key in scan(''):
                    logger.debug('spec {} unit {} available or pending'
                                 .format(work_spec_name, key))
                for key in scan(_BLOCKED):
                    blocked_on = self.registry.get(
                        WORK_UNITS_ + work_spec_name + _DEPENDS, key)
                    logger.debug('spec {} unit {} blocked on {!r}'
                                 .format(work_spec_name, key, blocked_on))
                for key in scan(_FINISHED):
                    logger.debug('spec {} unit {} finished'
                                 .format(work_spec_name, key))
                for key in scan(_FAILED):
                    logger.debug('spec {} unit {} failed'
                                 .format(work_spec_name, key))


    @classmethod
    def validate_work_spec(cls, work_spec):
        '''Check that `work_spec` is valid.

        It must at the very minimum contain a ``name`` and ``min_gb``.

        :raise rejester.exceptions.ProgrammerError: if it isn't valid

        '''
        if 'name' not in work_spec:
            raise ProgrammerError('work_spec lacks "name"')
        if 'min_gb' not in work_spec or \
                not isinstance(work_spec['min_gb'], (float, int)):
            raise ProgrammerError('work_spec["min_gb"] must be a number')

    def num_available(self, work_spec_name):
        '''Get the number of available work units for some work spec.

        These are work units that could be returned by :meth:`get_work`:
        they are not complete, not currently executing, and not blocked
        on some other work unit.

        '''
        return self.registry.len(WORK_UNITS_ + work_spec_name, 
                                 priority_max=time.time())

    def num_pending(self, work_spec_name):
        '''Get the number of pending work units for some work spec.

        These are work units that some worker is currently working on
        (hopefully; it could include work units assigned to workers that
        died and that have not yet expired).

        '''
        return self.registry.len(WORK_UNITS_ + work_spec_name, 
                                 priority_min=time.time())

    def num_blocked(self, work_spec_name):
        '''Get the number of blocked work units for some work spec.

        These are work units that are the first parameter to
        :meth:`add_dependent_work_units` where the job they depend on
        has not yet completed.

        '''
        return self.registry.len(WORK_UNITS_ + work_spec_name + _BLOCKED)

    def num_finished(self, work_spec_name):
        '''Get the number of finished work units for some work spec.

        These have completed successfully with :meth:`WorkUnit.finish`.

        '''
        return self.registry.len(WORK_UNITS_ + work_spec_name + _FINISHED)

    def num_failed(self, work_spec_name):
        '''Get the number of failed work units for some work spec.

        These have completed unsuccessfully with :meth:`WorkUnit.fail`.

        '''
        return self.registry.len(WORK_UNITS_ + work_spec_name + _FAILED)

    def num_tasks(self, work_spec_name):
        '''Get the total number of work units for some work spec.'''
        return self.num_finished(work_spec_name) + \
               self.num_failed(work_spec_name) + \
               self.registry.len(WORK_UNITS_ + work_spec_name)

    def status(self, work_spec_name):
        '''Get a summary dictionary for some work spec.

        The keys are the strings :meth:`num_available`, :meth:`num_pending`,
        :meth:`num_blocked`, :meth:`num_finished`, :meth:`num_failed`,
        and :meth:`num_tasks`, and the values are the values returned
        from those functions.

        '''
        return dict(
            num_available=self.num_available(work_spec_name),
            num_pending=self.num_pending(work_spec_name),
            num_blocked=self.num_blocked(work_spec_name),
            num_finished=self.num_finished(work_spec_name),
            num_failed=self.num_failed(work_spec_name),
            num_tasks=self.num_tasks(work_spec_name),
            )

    def list_work_specs(self, limit=None, start=None):
        '''Get the list of [(work spec name, work spec), ...]

        The keys are the work spec names; the values are the actual
        work spec definitions.

        return [(spec name, spec), ...], next start value

        '''
        return self.registry.pull(WORK_SPECS).items(), None

    def iter_work_specs(self, limit=None, start=None):
        '''
        yield work spec dicts
        '''
        count = 0
        ws_list, start = self.list_work_specs(limit, start)
        while True:
            for name_spec in ws_list:
                yield name_spec[1]
                count += 1
                if (limit is not None) and (count >= limit):
                    break
            if not start:
                break
            if limit is not None:
                limit -= count
            ws_list, start = self.list_work_specs(limit, start)

    def get_work_spec(self, work_spec_name):
        '''Get the dictionary defining some work spec.'''
        with self.registry.lock(identifier=self.worker_id) as session:
            return session.get(WORK_SPECS, work_spec_name)

    def list_work_units(self, work_spec_name, start=0, limit=None):
        """Get a dictionary of work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Only work units that have not been completed ("available" or
        "pending" work units) are included.

        """
        return self.registry.filter(WORK_UNITS_ + work_spec_name,
                                    start=start, limit=limit)

    def list_available_work_units(self, work_spec_name, start=0, limit=None):
        """Get a dictionary of available work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Only work units that have not been started, or units that were
        started but did not complete in a timely fashion, are
        included.

        """
        return self.registry.filter(WORK_UNITS_ + work_spec_name,
                                    priority_max=time.time(),
                                    start=start, limit=limit)

    def list_pending_work_units(self, work_spec_name, start=0, limit=None):
        """Get a dictionary of in-progress work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Units listed here should be worked on by some worker.

        """
        return self.registry.filter(WORK_UNITS_ + work_spec_name,
                                    priority_min=time.time(),
                                    start=start, limit=limit)

    def list_blocked_work_units(self, work_spec_name, start=0, limit=None):
        """Get a dictionary of blocked work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Work units included in this list are blocked because they were
        listed as the first work unit in
        :func:`add_dependent_work_units`, and the work unit(s) they
        depend on have not completed yet.  This function does not tell
        why work units are blocked, it merely returns the fact that
        they are.

        """
        return self.registry.filter(WORK_UNITS_ + work_spec_name + _BLOCKED,
                                    start=start, limit=limit)

    def list_finished_work_units(self, work_spec_name, start=0, limit=None):
        """Get a dictionary of finished work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Only work units that have been successfully completed are
        included.

        """
        return self.registry.filter(WORK_UNITS_ + work_spec_name + _FINISHED,
                                    start=start, limit=limit)

    def list_failed_work_units(self, work_spec_name, start=0, limit=None):
        """Get a dictionary of failed work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Only work units that have completed unsuccessfully are included.

        """
        return self.registry.filter(WORK_UNITS_ + work_spec_name + _FAILED,
                                    start=start, limit=limit)

    def _remove_some_work_units(self, work_spec_name, work_unit_names,
                                suffix='', priority_min='-inf',
                                priority_max='+inf'):
        '''Remove some units from somewhere.'''
        now = time.time()
        if work_unit_names is None:
            count = 0
            while True:
                with self.registry.lock(identifier=self.worker_id) as session:
                    names = session.filter(
                        WORK_UNITS_ + work_spec_name + suffix,
                        priority_min=priority_min, priority_max=priority_max,
                        limit=1000)
                    if not names: break
                    count += session.popmany(
                        WORK_UNITS_ + work_spec_name + suffix, *names)
        else:
            # TODO: This needs to honor priority_min/priority_max,
            # otherwise it gets the wrong answer for "available"/
            # "pending" (it will get both states).
            with self.registry.lock(identifier=self.worker_id) as session:
                count = session.popmany(WORK_UNITS_ + work_spec_name + suffix,
                                        *work_unit_names)
        return count

    def remove_available_work_units(self, work_spec_name, work_unit_names):
        '''Remove some work units in the available queue.

        If `work_unit_names` is :const:`None` (which must be passed
        explicitly), all available work units in `work_spec_name` are
        removed; otherwise only the specific named work units will be.

        :param str work_spec_name: name of the work spec
        :param list work_unit_names: names of the work units, or
          :const:`None` for all in `work_spec_name`
        :return: number of work units removed

        '''
        return self._remove_some_work_units(
            work_spec_name, work_unit_names, priority_max=time.time())

    def remove_pending_work_units(self, work_spec_name, work_unit_names):
        '''Remove some work units in the pending list.

        If `work_unit_names` is :const:`None` (which must be passed
        explicitly), all pending work units in `work_spec_name` are
        removed; otherwise only the specific named work units will be.

        Note that this function has the potential to confuse workers
        if they are actually working on the work units in question.  If
        you have ensured that the workers are dead and you would be
        otherwise waiting for the leases to expire before calling
        :meth:`remove_available_work_units`, then this is a useful
        shortcut.

        :param str work_spec_name: name of the work spec
        :param list work_unit_names: names of the work units, or
          :const:`None` for all in `work_spec_name`
        :return: number of work units removed

        '''
        return self._remove_some_work_units(
            work_spec_name, work_unit_names, priority_min=time.time())

    def remove_blocked_work_units(self, work_spec_name, work_unit_names):
        '''Remove some work units in the blocked list.

        If `work_unit_names` is :const:`None` (which must be passed
        explicitly), all pending work units in `work_spec_name` are
        removed; otherwise only the specific named work units will be.

        Note that none of the "remove" functions will restart blocked
        work units, so if you have called
        e.g. :meth:`remove_available_work_units` for a predecessor
        job, you may need to also call this method for its successor.

        :param str work_spec_name: name of the work spec
        :param list work_unit_names: names of the work units, or
          :const:`None` for all in `work_spec_name`
        :return: number of work units removed

        '''
        return self._remove_some_work_units(
            work_spec_name, work_unit_names, suffix=_BLOCKED)

    def remove_failed_work_units(self, work_spec_name, work_unit_names):
        '''Remove some failed work units.

        If `work_unit_names` is :const:`None` (which must be passed
        explicitly), all failed work units in `work_spec_name` are
        removed; otherwise only the specific named work units will be.

        Also consider :meth:`retry` to move failed work units back into
        the available queue.

        :param str work_spec_name: name of the work spec
        :param list work_unit_names: names of the work units, or
          :const:`None` for all in `work_spec_name`
        :return: number of work units removed

        '''
        return self._remove_some_work_units(
            work_spec_name, work_unit_names, suffix=_FAILED)

    def remove_finished_work_units(self, work_spec_name, work_unit_names):
        '''Remove some finished work units.

        If `work_unit_names` is :const:`None` (which must be passed
        explicitly), all finished work units in `work_spec_name` are
        removed; otherwise only the specific named work units will be.

        :param str work_spec_name: name of the work spec
        :param list work_unit_names: names of the work units, or
          :const:`None` for all in `work_spec_name`
        :return: number of work units removed

        '''
        return self._remove_some_work_units(
            work_spec_name, work_unit_names, suffix=_FINISHED)

    def get_work_unit_status(self, work_spec_name, work_unit_key):
        '''Get a high-level status for some work unit.

        The return value is a dictionary.  The only required key is
        ``status``, which could be any of:

        ``missing``
          The work unit does not exist anywhere
        ``available``
          The work unit is available for new workers; additional
          keys include ``expiration`` (may be 0)
        ``pending``
          The work unit is being worked on; additional keys include
          ``expiration`` and ``worker_id`` (usually)
        ``blocked``
          The work unit is waiting for some other work units to finish;
          additional keys include ``depends_on``
        ``finished``
          The work unit has completed
        ``failed``
          The work unit failed; additional keys include ``traceback``

        :param str work_spec_name: name of the work spec
        :param str work_unit_name: name of the work unit
        :return: dictionary description of summary status
        '''
        with self.registry.lock(identifier=self.worker_id) as session:
            # In the available list?
            (unit,priority) = session.get(WORK_UNITS_ + work_spec_name,
                                          work_unit_key, include_priority=True)
            if unit:
                result = {}
                if priority < time.time():
                    result['status'] = 'available'
                else:
                    result['status'] = 'pending'
                result['expiration'] = priority
                # ...is anyone working on it?
                worker = session.get(WORK_UNITS_ + work_spec_name + "_locks",
                                     work_unit_key)
                if worker:
                    result['worker_id'] = worker
                return result

            # In the finished list?
            unit = session.get(WORK_UNITS_ + work_spec_name + _FINISHED,
                               work_unit_key)
            if unit:
                return { 'status': 'finished' }

            # In the failed list?
            unit = session.get(WORK_UNITS_ + work_spec_name + _FAILED,
                               work_unit_key)
            if unit:
                result = { 'status': 'failed' }
                if 'traceback' in unit:
                    result['traceback'] = unit['traceback']
                return result

            # In the blocked list?
            unit = session.get(WORK_UNITS_ + work_spec_name + _BLOCKED,
                               work_unit_key)
            if unit:
                # This should always have *something*, right?
                deps = session.get(WORK_UNITS_ + work_spec_name + _DEPENDS,
                                   work_unit_key, default=[])
                result = { 'status': 'blocked',
                           'depends_on': deps }
                return result

        return { 'status': 'missing' }

    def inspect_work_unit(self, work_spec_name, work_unit_key):
        '''Get the data for some work unit.

        Returns the data for that work unit, or `None` if it really
        can't be found.

        :param str work_spec_name: name of the work spec
        :param str work_unit_key: name of the work unit
        :return: definition of the work unit, or `None`
        '''
        with self.registry.lock(identifier=self.worker_id) as session:
            work_unit_data = session.get(
                WORK_UNITS_ + work_spec_name, work_unit_key)
            if not work_unit_data:
                work_unit_data = session.get(
                    WORK_UNITS_ + work_spec_name + _BLOCKED, work_unit_key)
            if not work_unit_data:
                work_unit_data = session.get(
                    WORK_UNITS_ + work_spec_name + _FINISHED, work_unit_key)
            if not work_unit_data:
                work_unit_data = session.get(
                    WORK_UNITS_ + work_spec_name + _FAILED, work_unit_key)
            return work_unit_data

    def _inspect_work_unit(self, session, work_spec_name, work_unit_key):
        return work_unit_data


    def reset_all(self, work_spec_name):
        '''Restart a work spec.

        This calls :meth:`idle_all_workers`, then moves all finished
        jobs back into the available queue.

        .. deprecated:: 0.4.5
            See :meth:`idle_all_workers` for problems with that method.
            This also ignores failed jobs and work unit dependencies.
            In practice, whatever generated a set of work units
            initially can recreate them easily enough.

        '''
        self.idle_all_workers()
        with self.registry.lock(identifier=self.worker_id) as session:
            session.move_all(WORK_UNITS_ + work_spec_name + _FINISHED,
                             WORK_UNITS_ + work_spec_name)
            session.reset_priorities(WORK_UNITS_ + work_spec_name, 0)

    def update_bundle(self, work_spec, work_units, nice=0):
        '''Load a work spec and some work units into the task list.
        
        update the work_spec and work_units.  Overwrites any existing
        work spec with the same ``work_spec['name']`` and similarly
        overwrites any WorkUnit with the same ``work_unit.key``

        :param dict work_spec: Work spec dictionary
        :param work_units: Keys are used as :attr:`WorkUnit.key`, values
          are used as :attr:`WorkUnit.data`
        :type work_units: dict of dict
        :param int nice: Niceness of `work_spec`, higher value is lower
          priority

        '''
        self.validate_work_spec(work_spec)
        
        work_spec_name = work_spec['name']
        with self.registry.lock(identifier=self.worker_id) as session:
            session.update(NICE_LEVELS, {work_spec_name: nice})
            session.update(WORK_SPECS,  {work_spec_name: work_spec})
            
            ## redis chokes on lua scripts with 
            work_units = work_units.items()
            window_size = 10**4
            for i in xrange(0, len(work_units), window_size):
                self.registry.re_acquire_lock()
                session.update(WORK_UNITS_ + work_spec_name, 
                               dict(work_units[i: i + window_size]))

    def set_work_spec(self, work_spec):
        '''
        work_spec is a dict()
        work_spec['name'] is used as work_spec_name in other API calls
        work_spec['nice'] is used for prioritization, if set.
        '''
        self.validate_work_spec(work_spec)

        nice = int(work_spec.get('nice', 0))
        work_spec_name = work_spec['name']
        with self.registry.lock(identifier=self.worker_id) as session:
            session.update(NICE_LEVELS, {work_spec_name: nice})
            session.update(WORK_SPECS,  {work_spec_name: work_spec})

    def add_work_units(self, work_spec_name, work_unit_key_vals):
        '''
        work_unit_key_vals list of (work_unit_key, work_unit_data)
        '''
        with self.registry.lock(identifier=self.worker_id) as session:
            window_size = 10**4
            for i in xrange(0, len(work_unit_key_vals), window_size):
                self.registry.re_acquire_lock()
                session.update(WORK_UNITS_ + work_spec_name,
                               dict(work_unit_key_vals[i: i + window_size]))

    def add_dependent_work_units(self, work_unit, depends_on, hard=True):
        """Add work units, where one prevents execution of the other.

        The two work units may be attached to different work specs,
        but both must be in this task master's namespace.  `work_unit`
        and `depends_on` are both tuples of (work spec name, work unit
        name, work unit dictionary).  The work specs must already
        exist; they may be created with :meth:`update_bundle` with
        an empty work unit dictionary.  If a work unit dictionary is
        provided with either work unit, then this defines that work
        unit, and any existing definition is replaced.  Either or both
        work unit dictionaries may be :const:`None`, in which case the
        work unit is not created if it does not already exist.  In
        this last case, the other work unit will be added if
        specified, but the dependency will not be added, and this
        function will return :const:`False`.  In all other cases, this
        dependency is added in addition to all existing dependencies
        on either or both work units, even if the work unit dictionary
        is replaced.

        `work_unit` will not be executed or reported as available via
        :meth:`get_work` until `depends_on` finishes execution.  If
        the `depends_on` task fails, then the `hard` parameter
        describes what happens: if `hard` is :const:`True` then
        `work_unit` will also fail, but if `hard` is :const:`False`
        then `work_unit` will be able to execute even if `depends_on`
        fails, it just must have completed some execution attempt.

        Calling this function with ``hard=True`` suggests an ordered
        sequence of tasks where the later task depends on the output
        of the earlier tasks.  Calling this function with
        ``hard=False`` suggests a cleanup task that must run after
        this task (and, likely, several others) are done, but doesn't
        specifically depend on its result being available.

        :param work_unit: "Later" work unit to execute
        :paramtype work_unit: tuple of (str,str,dict)
        :param depends_on: "Earlier" work unit to execute
        :paramtype depends_on: tuple of (str,str,dict)
        :param bool hard: if True, then `work_unit` automatically fails
          if `depends_on` fails
        :return: :const:`True`, unless one or both of the work units
          didn't exist and weren't specified, in which case, :const:`False`
        :raise rejester.exceptions.NoSuchWorkSpecError: if a work spec was
          named that doesn't exist

        """
        # There's no good, not-confusing terminology here.
        # I'll call work_unit "later" and depends_on "earlier"
        # consistently, because that at least makes the time flow
        # correct.
        later_spec, later_unit, later_unitdef = work_unit
        earlier_spec, earlier_unit, earlier_unitdef = depends_on
        with self.registry.lock(identifier=self.worker_id) as session:
            # Bail if either work spec doesn't already exist
            if session.get(WORK_SPECS, later_spec) is None:
                raise NoSuchWorkSpecError(later_spec)
            if session.get(WORK_SPECS, earlier_spec) is None:
                raise NoSuchWorkSpecError(earlier_spec)
            
            # Cause both work units to exist (if possible)
            # Note that if "earlier" is already finished, we may be
            # able to make "later" available immediately
            earlier_done = False
            earlier_successful = False
            if earlier_unitdef is not None:
                session.update(WORK_UNITS_ + earlier_spec,
                               { earlier_unit: earlier_unitdef })
            else:
                earlier_unitdef = session.get(
                    WORK_UNITS_ + earlier_spec, earlier_unit)
                if earlier_unitdef is None:
                    earlier_unitdef = session.get(
                        WORK_UNITS_ + earlier_spec + _BLOCKED, earlier_unit)
                if earlier_unitdef is None:
                    earlier_unitdef = session.get(
                        WORK_UNITS_ + earlier_spec + _FINISHED, earlier_unit)
                    if earlier_unitdef is not None:
                        earlier_done = True
                        earlier_successful = True
                if earlier_unitdef is None:
                    earlier_unitdef = session.get(
                        WORK_UNITS_ + earlier_spec + _FAILED, earlier_unit)
                    if earlier_unitdef is not None:
                        earlier_done = True

            later_failed = earlier_done and hard and not earlier_successful
            later_unblocked = ((earlier_done and not later_failed) or
                               (earlier_unitdef is None))
            if later_failed:
                later_destination = WORK_UNITS_ + later_spec + _FAILED
            elif later_unblocked:
                later_destination = WORK_UNITS_ + later_spec
            else:
                later_destination = WORK_UNITS_ + later_spec + _BLOCKED

            if later_unitdef is not None:
                for suffix in ['', _FINISHED, _FAILED, _BLOCKED]:
                    k = WORK_UNITS_ + later_spec + suffix
                    if k != later_destination:
                        session.popmany(k, later_unit)
                session.update(later_destination,
                               { later_unit: later_unitdef })
            elif earlier_unitdef is not None:
                later_unitdef = session.get(
                    WORK_UNITS_ + later_spec, later_unit)
                if later_unitdef is not None:
                    session.move(
                        WORK_UNITS_ + later_spec,
                        WORK_UNITS_ + later_spec + _BLOCKED,
                        { later_unit: later_unitdef })
                else:
                    later_unitdef = session.get(
                        WORK_UNITS_ + later_spec + _BLOCKED, later_unit)

            if later_unitdef is None or earlier_unitdef is None:
                return False

            # Now both units exist and are in the right place;
            # record the dependency
            blocks = session.get(WORK_UNITS_ + earlier_spec + _BLOCKS,
                                 earlier_unit)
            if blocks is None: blocks = []
            blocks.append([later_spec, later_unit, hard])
            session.set(WORK_UNITS_ + earlier_spec + _BLOCKS,
                        earlier_unit, blocks)

            depends = session.get(WORK_UNITS_ + later_spec + _DEPENDS,
                                  later_unit)
            if depends is None: depends = []
            depends.append([earlier_spec, earlier_unit])
            session.set(WORK_UNITS_ + later_spec + _DEPENDS,
                        later_unit, depends)

            return True

    def retry(self, work_spec_name, *work_unit_names):
        '''Move failed work unit(s) back into the "pending" queue.

        The work unit will be available to execute immediately.  If
        other tasks had depended on it, those dependencies will not
        be recreated.

        :param str work_spec_name: name of the (existing) work spec
        :param str work_unit_names: name(s) of the (failed) work unit(s)
        :raise rejester.NoSuchWorkSpecError: if `work_spec_name` is
          invalid
        :raise rejester.NoSuchWorkUnitError: if `work_spec_name` is
          valid but any of the `work_unit_names` are not a failed work unit
        :raise rejester.LockError: if the registry lock could not be
          obtained
        
        '''
        with self.registry.lock(identifier=self.worker_id) as session:
            # This sequence is safe even if this system dies
            units = {}
            for work_unit_name in work_unit_names:
                unit = session.get(WORK_UNITS_ + work_spec_name + _FAILED,
                                   work_unit_name)
                if unit is None:
                    spec = session.get(WORK_SPECS, work_spec_name)
                    if spec is None:
                        raise NoSuchWorkSpecError(work_spec_name)
                    else:
                        raise NoSuchWorkUnitError(work_unit_name)
                if 'traceback' in unit:
                    del unit['traceback']
                units[work_unit_name] = unit
            session.move(WORK_UNITS_ + work_spec_name + _FAILED,
                         WORK_UNITS_ + work_spec_name,
                         units,
                         priority=0)

    def nice(self, work_spec_name, nice):
        '''Change the priority of an existing work spec.'''
        with self.registry.lock(identifier=self.worker_id) as session:
            session.update(NICE_LEVELS, dict(work_spec_name=nice))

    def get_work(self, worker_id, available_gb=None, lease_time=300, work_spec_names=None):
        '''obtain a WorkUnit instance based on available memory for the
        worker process.  
        
        :param worker_id: unique identifier string for a worker to
          which a WorkUnit will be assigned, if available.
        :param available_gb: number of gigabytes of RAM available to
          this worker
        :param lease_time: how many seconds to lease a WorkUnit
        :param work_spec_names: limit to queue from one work_spec. NOT IMPLEMENTD. this implementation will return work from any work spec.

        '''

        if not isinstance(available_gb, (int, float)):
            raise ProgrammerError('must specify available_gb')

        work_unit = None

        try: 
            with self.registry.lock(identifier=self.worker_id) as session:
                ## use the simple niceness algorithm described in
                ## http://en.wikipedia.org/wiki/Nice_(Unix)
                ## where each job gets a (20-niceness) share
                nice_levels = session.pull(NICE_LEVELS)
                for work_spec_name, nice in nice_levels.iteritems():
                    nice = min(19, nice)
                    nice = max(-19, nice)
                    nice = 20 - nice
                    nice_levels[work_spec_name] = nice

                while nice_levels:
                    total_nice = sum(nice_levels.values())
                    score = random.randrange(total_nice)
                    work_spec_name = None
                    total_score = 0
                    for wsn, nice in nice_levels.iteritems():
                        total_score += nice
                        if total_score > score:
                            work_spec_name = wsn
                            break
                    assert work_spec_name is not None
                    nice_levels.pop(work_spec_name)
                    
                    ## verify sufficient memory
                    work_spec = session.get(WORK_SPECS, work_spec_name)
                    if available_gb < work_spec['min_gb']:
                        continue

                    ## try to get a task
                    wu_expires = time.time() + lease_time
                    _work_unit = session.getitem_reset(
                        WORK_UNITS_ + work_spec_name,
                        priority_max=time.time(),
                        new_priority=wu_expires,
                        lock=worker_id,
                    )

                    if _work_unit:
                        logger.info('work unit %r', _work_unit)
                        work_unit = WorkUnit(
                            self.registry, work_spec_name,
                            _work_unit[0], _work_unit[1],
                            worker_id=worker_id,
                            expires=wu_expires,
                        )
                        break

        except (LockError, EnvironmentError), exc:
            logger.error('took to long to get work', exc_info=True)

        logger.debug('get_work %r', work_unit)
        return work_unit

    def get_assigned_work_unit(
            self, worker_id, work_spec_name, work_unit_key,
        ):
        '''get a specific WorkUnit that has already been assigned to a
        particular worker_id
        '''
        with self.registry.lock(identifier=self.worker_id) as session:
            assigned_work_unit_key = session.get(
                WORK_UNITS_ + work_spec_name + '_locks', worker_id)
            if not assigned_work_unit_key == work_unit_key:
                ## raise LostLease instead of EnvironmentError, so
                ## users of TaskMaster can have a single type of
                ## expected exception, rather than two
                raise LostLease(
                    'assigned_work_unit_key=%r != %r'
                    % (assigned_work_unit_key, work_unit_key))
            ## could trap EnvironmentError and raise LostLease instead
            work_unit_data = session.get(
                        WORK_UNITS_ + work_spec_name,
                        work_unit_key)
            return WorkUnit(
                self.registry, work_spec_name,
                work_unit_key, work_unit_data,
                worker_id=worker_id,                            
            )

    def worker_register(self, worker_id, mode=None, lifetime=6000, environment=None):
        # actually the same as heartbeat, just, "hello, I'm here"
        self.worker_heartbeat(worker_id, mode, lifetime, environment)

    def worker_heartbeat(self, worker_id, mode=None, lifetime=6000, environment=None):
        if environment is None:
            environment = {}
        with self.registry.lock(identifier=worker_id) as session:
            session.set(WORKER_OBSERVED_MODE, worker_id, mode,
                        priority=time.time() + lifetime)
            session.update(WORKER_STATE_ + worker_id, environment,
                           expire=lifetime)

    def worker_unregister(self, worker_id):
        with self.registry.lock(
                identifier=worker_id) as session:
            session.delete(WORKER_STATE_ + worker_id)
            session.popmany(WORKER_OBSERVED_MODE, worker_id)
