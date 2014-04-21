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
import socket
import traceback
import pkg_resources
from operator import itemgetter

from rejester._registry import Registry
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

class Worker(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self.config = config
        self.task_master = TaskMaster(config)
        self.worker_id = None
        self.lifetime = 300 ## five minutes

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
        self.worker_id = uuid.uuid4().hex
        self.heartbeat()
        return self.worker_id

    def unregister(self):
        '''Remove this worker from the list of available workers.

        This requires the worker to already have been :meth:`register()`.

        ''' 
        with self.task_master.registry.lock() as session:
            session.delete(WORKER_STATE_ + self.worker_id)
            session.popmany(WORKER_OBSERVED_MODE, self.worker_id)
        self.worker_id = None

    def heartbeat(self):
        '''record the current worker state in the registry and also the
        observed mode and return it

        :returns mode:
        ''' 
        mode = self.task_master.get_mode()
        with self.task_master.registry.lock() as session:
            session.set(WORKER_OBSERVED_MODE, self.worker_id, mode,
                        priority=time.time() + self.lifetime)
            session.update(WORKER_STATE_ + self.worker_id, self.environment(), 
                           expire=self.lifetime)
        logger.debug('worker {} observed mode {}'.format(self.worker_id, mode))
        return mode

    @abc.abstractmethod
    def run(self):
        return


class WorkUnit(object):
    def __init__(self, registry, work_spec_name, key, data, worker_id=None, expires=None):
        if not worker_id:
            raise ProgrammerError('must specify a worker_id, not: %r' % worker_id)
        self.worker_id = worker_id
        self.registry = registry
        self.work_spec_name = work_spec_name
        self.key = key
        self.data = data
        self.finished = False
        self.failed = False
        self.expires = expires  # time.time() when lease expires
        self._spec_cache = None  # storage for lazy getter property
        self._module_cache = None  # storage for lazy getter property
        logger.debug('WorkUnit init %r', self.key)

    def __repr__(self):
        return 'WorkUnit(key=%s)' % self.key

    # lazy caching getter for work spec
    @property
    def spec(self):
        if self._spec_cache is None:
            self._spec_cache = self.registry.get(WORK_SPECS, self.work_spec_name)
        return self._spec_cache

    # lazy caching getter for module
    @property
    def module(self):
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
        '''execute this WorkUnit using the function specified in its
        work_spec.  Is called multiple times.
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
        '''shutdown this WorkUnit using the function specified in its
        work_spec.  Is called multiple times.
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
        '''reset the task lease by incrementing lease_time.  Default is to put
        it five minutes ahead of now.  Setting lease_time<0 will drop
        the lease.

        If the worker waited too long since the previous call to
        update and another worker has taken this WorkUnit, then this
        raises rejester.exceptions.LostLease
        '''
        if self.finished:
            raise ProgrammerError('cannot .update() after .finish()')
        if self.failed:
            raise ProgrammerError('cannot .update() after .fail()')
        with self.registry.lock() as session:
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
        '''move this WorkUnit to finished state
        '''
        if self.finished or self.failed:
            return
        with self.registry.lock() as session:
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
        '''move this WorkUnit to failed state and record info about the
        worker
        '''
        if self.failed or self.finished:
            return
        self.data['traceback'] = exc and traceback.format_exc(exc) or exc
        #self.data['worker_state'] = self.worker_state
        with self.registry.lock() as session:
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
    keys 'name' and 'min_gb'; in typical use, 'module' names a Python
    module, 'run_function' and 'terminate_function' name specific
    functions within that module, and 'config' provides a
    configuration dictionary as well.

    A work unit can be in one of five states.  It is *available* if it
    has been added to the queue but nobody is working on it.  It is
    *pending* if somebody is currently working on it.  When they
    finish, it will become either *finished* or *failed*.  If
    dependencies are added between tasks using `add_dependent_task()`,
    a task can also be *blocked*.

    The general flow for a rejester application is to
    `set_mode(IDLE)`, then add work units using `update_bundle()`, and
    `set_mode(RUN)`.  `get_work()` will return work units until all
    have been consumed.  `set_mode(TERMINATE)` will instruct workers
    to shut down.

    This object keeps very little state locally and can safely be used
    concurrently, including from multiple systems.  Correspondingly,
    any settings here, including `set_mode()`, are persistent even
    beyond the end of the current process.

    '''

    def __init__(self, config):
        config['app_name'] = 'rejester'
        self.config = config
        self.registry = Registry(config)

    RUN = 'RUN'
    IDLE = 'IDLE'
    TERMINATE = 'TERMINATE'

    def set_mode(self, mode):
        '''Set the global mode of the rejester system.

        This must be one of the constants `TERMINATE`, `RUN`, or `IDLE`.
        `TERMINATE` instructs any running workers to do an orderly
        shutdown.  `IDLE` instructs workers to stay running but not
        start new jobs.  `RUN` tells workers to do actual work.

        :param str mode: new rejester mode
        :raise ProgrammerError: on invalid `mode`

        '''
        if not mode in [self.TERMINATE, self.RUN, self.IDLE]:
            raise ProgrammerError('mode=%r is not recognized' % mode)
        with self.registry.lock() as session:
            session.set('modes', 'mode', mode)
        logger.info('set mode to %s', mode)

    def get_mode(self):
        '''Get the global mode of the rejester system.

        :return: rejester mode, `IDLE` if unset

        '''
        with self.registry.lock() as session:
            return session.get('modes', 'mode') or self.IDLE

    def idle_all_workers(self):
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
        '''counts the modes observed by workers who heartbeated within one
        lifetime of the present time.
        '''
        modes = {self.RUN: 0, self.IDLE: 0, self.TERMINATE: 0}
        for worker_id, mode in self.workers().items():
            modes[mode] += 1
        return modes

    def workers(self, alive=True):
        '''returns dictionary of all worker_ids and current mode.  If
        alive=True, then only include worker_ids that have heartbeated
        within one lifetime of now.

        '''
        with self.registry.lock() as session:
            return session.filter(
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
        with self.registry.lock() as session:
            return session.pull(WORKER_STATE_ + worker_id)

    def dump(self):
        '''Print the entire contents of this to debug log messages.

        This is really only intended for debugging.  It could produce
        a lot of data.

        '''
        with self.registry.lock() as session:
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
        'raise ProgrammerError if work_spec is invalid'
        if 'name' not in work_spec:
            raise ProgrammerError('work_spec lacks "name"')
        if 'min_gb' not in work_spec or \
                not isinstance(work_spec['min_gb'], (float, int)):
            raise ProgrammerError('work_spec["min_gb"] must be a number')

    def num_available(self, work_spec_name):
        return self.registry.len(WORK_UNITS_ + work_spec_name, 
                                 priority_max=time.time())

    def num_pending(self, work_spec_name):
        return self.registry.len(WORK_UNITS_ + work_spec_name, 
                                 priority_min=time.time())

    def num_blocked(self, work_spec_name):
        return self.registry.len(WORK_UNITS_ + work_spec_name + _BLOCKED)

    def num_finished(self, work_spec_name):
        return self.registry.len(WORK_UNITS_ + work_spec_name + _FINISHED)

    def num_failed(self, work_spec_name):
        return self.registry.len(WORK_UNITS_ + work_spec_name + _FAILED)

    def num_tasks(self, work_spec_name):
        return self.num_finished(work_spec_name) + \
               self.num_failed(work_spec_name) + \
               self.registry.len(WORK_UNITS_ + work_spec_name)

    def status(self, work_spec_name):
        return dict(
            num_available=self.num_available(work_spec_name),
            num_pending=self.num_pending(work_spec_name),
            num_blocked=self.num_blocked(work_spec_name),
            num_finished=self.num_finished(work_spec_name),
            num_failed=self.num_failed(work_spec_name),
            num_tasks=self.num_tasks(work_spec_name),
            )


    def get_work_spec(self, work_spec_name):
        '''Get the dictionary defining some work spec.'''
        with self.registry.lock(atime=1000) as session:
            return session.get(WORK_SPECS, work_spec_name)

    def list_work_units(self, work_spec_name):
        """Get a dictionary of work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Only work units that have not been completed ("available" or
        "pending" work units) are included.

        """
        with self.registry.lock(atime=1000) as session:
            return session.pull(WORK_UNITS_ + work_spec_name)

    def list_failed_work_units(self, work_spec_name):
        """Get a dictionary of failed work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Only work units that have completed unsuccessfully are included.

        """
        with self.registry.lock(atime=1000) as session:
            return session.pull(WORK_UNITS_ + work_spec_name + _FAILED)

    def inspect_work_unit(self, work_spec_name, work_unit_key):
        '''Get the data for some work unit.

        Returns the data for that work unit, or `None` if it really
        can't be found.

        :param str work_spec_name: name of the work spec
        :param str work_unit_key: name of the work unit
        :return: definition of the work unit, or `None`
        '''
        with self.registry.lock(atime=1000) as session:
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
        self.idle_all_workers()
        with self.registry.lock(ltime=10000) as session:
            session.move_all(WORK_UNITS_ + work_spec_name + _FINISHED,
                             WORK_UNITS_ + work_spec_name)
            session.reset_priorities(WORK_UNITS_ + work_spec_name, 0)

    def update_bundle(self, work_spec, work_units, nice=0):
        '''Load a work spec and some work units into the task list.
        
        update the work_spec and work_units.  Overwrites any existing
        work_spec with the same ``work_spec['name']`` and similarly
        overwrites any WorkUnit with the same ``work_unit.key``

        :param work_units: dict of dicts where the keys are used as
          WorkUnit.key and the values are used as WorkUnit.data

        '''
        self.validate_work_spec(work_spec)
        
        work_spec_name = work_spec['name']
        with self.registry.lock(atime=1000) as session:
            session.update(NICE_LEVELS, {work_spec_name: nice})
            session.update(WORK_SPECS,  {work_spec_name: work_spec})
            
            ## redis chokes on lua scripts with 
            work_units = work_units.items()
            window_size = 10**4
            for i in xrange(0, len(work_units), window_size):
                self.registry.re_acquire_lock(ltime=2000)
                session.update(WORK_UNITS_ + work_spec_name, 
                               dict(work_units[i: i + window_size]))

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
        :raise NoSuchWorkSpecError: if a work spec was named that doesn't
          exist

        """
        # There's no good, not-confusing terminology here.
        # I'll call work_unit "later" and depends_on "earlier"
        # consistently, because that at least makes the time flow
        # correct.
        later_spec, later_unit, later_unitdef = work_unit
        earlier_spec, earlier_unit, earlier_unitdef = depends_on
        with self.registry.lock(atime=1000) as session:
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

    def retry(self, work_spec_name, work_unit_name):
        '''Move a failed work unit back into the "pending" queue.

        The work unit will be available to execute immediately.  If
        other tasks had depended on it, those dependencies will not
        be recreated.

        :param str work_spec_name: name of the (existing) work spec
        :param str work_unit_name: name of the (failed) work unit
        :raise rejester.NoSuchWorkSpecError: if `work_spec_name` is
          invalid
        :raise rejester.NoSuchWorkUnitError: if `work_spec_name` is
          valid but `work_unit_name` is not a failed work unit
        :raise rejester.LockError: if the registry lock could not be
          obtained
        
        '''
        with self.registry.lock(atime=1000) as session:
            # This sequence is safe even if this system dies
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
            session.move(WORK_UNITS_ + work_spec_name + _FAILED,
                         WORK_UNITS_ + work_spec_name,
                         { work_unit_name: unit },
                         priority=0)

    def nice(self, work_spec_name, nice):        
        with self.registry.lock(atime=1000) as session:
            session.update(NICE_LEVELS, dict(work_spec_name=nice))

    def get_work(self, worker_id, available_gb=None, lease_time=300):
        '''obtain a WorkUnit instance based on available memory for the
        worker process.  
        
        :param worker_id: unique identifier string for a worker to
          which a WorkUnit will be assigned, if available.
        :param available_gb: number of gigabytes of RAM available to
          this worker
        :param lease_time: how many seconds to lease a WorkUnit

        '''
        start = time.time()

        if not isinstance(available_gb, (int, float)):
            raise ProgrammerError('must specify available_gb')

        work_unit = None

        try: 
            with self.registry.lock(atime=1000, ltime=10000) as session:
                ## figure out which work_specs have low nice levels
                nice_levels = session.pull(NICE_LEVELS)
                nice_levels = nice_levels.items()
                nice_levels.sort(key=itemgetter(1))

                work_specs = session.pull(WORK_SPECS)
                for work_spec_name, nice in nice_levels:
                    self.registry.re_acquire_lock(ltime=10000)

                    ## verify sufficient memory
                    if available_gb < work_specs[work_spec_name]['min_gb']:
                        continue

                    logger.debug('considering %s %s', work_spec_name, nice_levels)

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
            logger.critical('failed to get work', exc_info=True)

        logger.debug('get_work %s in %.3f', work_unit is not None, time.time() - start)
        return work_unit

    def get_assigned_work_unit(
            self, worker_id, work_spec_name, work_unit_key,
        ):
        '''get a specific WorkUnit that has already been assigned to a
        particular worker_id
        '''
        with self.registry.lock(atime=1000, ltime=10000) as session:
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
