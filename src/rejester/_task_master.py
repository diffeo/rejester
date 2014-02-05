'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

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
    LostLease, EnvironmentError

logger = logging.getLogger(__name__)

BUNDLES = 'bundles'
NICE_LEVELS = 'NICE_LEVELS'
WORK_SPECS = 'WORK_SPECS'
WORK_UNITS_ = 'WORK_UNITS_'
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
        '''
        raw data about worker to support forensics on failed tasks
        '''
        env = dict(
            worker_id = self.worker_id,
            host = socket.gethostbyaddr(socket.gethostname()),
            fqdn = socket.getfqdn(),
            version = pkg_resources.get_distribution("rejester").version, # pylint: disable=E1103
            working_set = [(dist.key, dist.version) for dist in pkg_resources.WorkingSet()], # pylint: disable=E1103
            #config_hash = self.config['config_hash'],
            #config_json = self.config['config_json'],
            memory = psutil.phymem_usage(),
        )
        return env

    def register(self):
        '''record the availability of this worker and get a unique identifer
        '''
        if self.worker_id:
            raise ProgrammerError('Worker.register cannot be called again without first calling unregister; it is not idempotent')
        self.worker_id = uuid.uuid4().hex
        self.heartbeat()
        return self.worker_id

    def unregister(self):
        '''remove this worker from the list of available workers
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
        logger.info('worker observed mode=%r', mode)
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
                    logger.critical('failed to load spec["module"] = %r', self.spec['module'])
                    raise
        return self._module_cache

    def run(self):
        '''execute this WorkUnit using the function specified in its
        work_spec.  Is called multiple times.
        '''
        run_function = getattr(self.module, self.spec['run_function'])
        ret_val = run_function(self)
        self.update()
        return ret_val

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
        ret_val = terminate_function(self)
        self.update(lease_time=-10)
        logger.critical('called workunit.terminate()')
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
        self.failed = True


class TaskMaster(object):
    '''
    work_spec = dict(
        name = 
        desc = 
        min_gb = 
        config = dict -- like we have in yaml files now
        work_unit = tuple -- like we emit with i_str now in streamcorpus.pipeline
    )

    operations:
      update bundle -- appends latest work_spec to list
    '''

    def __init__(self, config):
        config['app_name'] = 'rejester'
        self.config = config
        self.registry = Registry(config)

    RUN = 'RUN'
    IDLE = 'IDLE'
    TERMINATE = 'TERMINATE'

    def set_mode(self, mode):
        'set the mode to TERMINATE, RUN, or IDLE'
        if not mode in [self.TERMINATE, self.RUN, self.IDLE]:
            raise ProgrammerError('mode=%r is not recognized' % mode)
        with self.registry.lock() as session:
            session.set('modes', 'mode', mode)
        logger.info('set mode to %s', mode)

    def get_mode(self):
        'returns mode, defaults to IDLE'
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
                priority_min=alive and time.time() or None)

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
            num_finished=self.num_finished(work_spec_name),
            num_failed=self.num_failed(work_spec_name),
            num_tasks=self.num_tasks(work_spec_name),
            )


    def list_work_units(self, work_spec_name):
        """Get a dictionary of work units for some work spec.

        The dictionary is from work unit name to work unit definiton.
        Only work units that have not been completed ("available" or
        "pending" work units) are included.

        """
        with self.registry.lock(atime=1000) as session:
            return session.pull(WORK_UNITS_ + work_spec_name)

    def inspect_work_unit(self, work_spec_name, work_unit_key):
        '''
        returns work_unit.data
        '''
        with self.registry.lock(atime=1000) as session:
            work_unit_data = session.get(
                WORK_UNITS_ + work_spec_name, work_unit_key)
            if not work_unit_data:
                work_unit_data = session.get(
                    WORK_UNITS_ + work_spec_name + _FINISHED, work_unit_key)
            return work_unit_data


    def reset_all(self, work_spec_name):
        self.idle_all_workers()
        with self.registry.lock(ltime=10000) as session:
            session.move_all(WORK_UNITS_ + work_spec_name + _FINISHED,
                             WORK_UNITS_ + work_spec_name)
            session.reset_priorities(WORK_UNITS_ + work_spec_name, 0)

    def update_bundle(self, work_spec, work_units, nice=0):
        '''update the work_spec and work_units.  Overwrites any existing
        work_spec with the same work_spec['name'] and similarly
        overwrites any WorkUnit with the same work_unit.key

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
                logger.debug('%d:%d', i, i + window_size)

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

                    logger.info('considering %s %s', work_spec_name, nice_levels)

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
                    'assigned_work_unit_key=%r != %'
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

             
