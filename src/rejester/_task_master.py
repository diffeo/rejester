'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import uuid
import time
import logging
from operator import itemgetter

from rejester._logging import logger
from rejester._registry import Registry
from rejester.exceptions import ProgrammerError


BUNDLES = 'bundles'
NICE_LEVELS = 'NICE_LEVELS'
WORK_SPECS = 'WORK_SPECS'
WORK_UNITS_ = 'WORK_UNITS_'
_FINISHED = '_FINISHED'

class WorkUnit(object):
    def __init__(self, registry, work_spec_name, key, data):
        self.registry = registry
        self.work_spec_name = work_spec_name
        self.key = key
        self.data = data
        self._finished = False
        self.spec = self.registry.get(WORK_SPECS, self.work_spec_name)
        self.module = __import__(
            self.spec['module'], globals(), (), 
            [self.spec['exec_function'], self.spec['shutdown_function']], -1)


    def execute(self):
        '''execute this WorkUnit using the function specified in its
        work_spec.  Is called multiple times.
        '''
        exec_function = getattr(self.module, self.spec['exec_function'])
        ret_val = exec_function(self)
        self.update()
        return ret_val


    def shutdown(self):
        '''shutdown this WorkUnit using the function specified in its
        work_spec.  Is called multiple times.
        '''
        shutdown_function = getattr(self.module, self.spec['shutdown_function'])
        ret_val = shutdown_function(self)
        self.update(lease_time=-10)
        logger.critical('called shutdown')
        return ret_val


    def update(self, lease_time=300):
        '''reset the task lease by incrementing lease_time.  Default is to put
        it five minutes ahead of now.  Setting lease_time<0 will drop
        the lease.
        '''
        if self._finished:
            raise ProgrammerError('cannot update after finishing at task')
        with self.registry.lock() as session:
            session.update(
                WORK_UNITS_ + self.work_spec_name,
                {self.key: self.data}, 
                {self.key: time.time() + lease_time})

    def finish(self):
        if self._finished:
            return
        with self.registry.lock() as session:
            session.move(
                WORK_UNITS_ + self.work_spec_name,
                WORK_UNITS_ + self.work_spec_name + _FINISHED,
                {self.key: self.data})
        self._finished = True

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
        self.registry = Registry(config)

    def register_worker(self):
        '''record the availability of this worker and get a unique identifer
        '''
        self.worker_id = uuid.uuid4()
        with self.registry.lock() as session:
            session.update('workers', {self.worker_id: ('host_info',)})
        return self.worker_id

    def unregister_worker(self):
        '''remove this worker from the list of available workers
        ''' 
        with self.registry.lock() as session:
            session.popmany('workers', self.worker_id)

    RUN = 'RUN'
    IDLE = 'IDLE'
    TERMINATE = 'TERMINATE'

    def set_mode(self, mode):
        'set the mode to TERMINATE, RUN, or IDLE'
        if not mode in [self.TERMINATE, self.RUN, self.IDLE]:
            raise ProgrammerError('mode=%r is not recognized' % mode)
        with self.registry.lock() as session:
            session.set('modes', 'mode', mode)

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

    def num_tasks(self, work_spec_name):
        return self.num_finished(work_spec_name) + \
               self.registry.len(WORK_UNITS_ + work_spec_name)

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
        '''
        
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

    def get_work(self, available_gb=None, lease_time=300):
        '''obtain a WorkUnit instance based on available memory for the
        worker process.  

        :param available_gb: number of gigabytes of RAM available to
        this worker

        :param lease_time: how many seconds to lease a WorkUnit
        '''
        if not isinstance(available_gb, (int, float)):
            raise ProgrammerError('must always specify available_gb')

        with self.registry.lock(atime=1000) as session:

            ## should do something here, like a loop or greenlet with
            ## timeout to avoid going past atime without taking action
            
            ## figure out which work_specs have low nice levels
            nice_levels = session.pull(NICE_LEVELS)
            nice_levels = nice_levels.items()
            nice_levels.sort(key=itemgetter(1))

            work_specs = session.pull(WORK_SPECS)
            for work_spec_name, nice in nice_levels:

                ## verify sufficient memory
                if available_gb < work_specs[work_spec_name]['min_gb']:
                    continue

                logger.info('considering %s %s', work_spec_name, nice_levels)

                ## try to get a task
                _work_unit = session.getitem_reset(
                    WORK_UNITS_ + work_spec_name,
                    priority_max=time.time(),
                    new_priority=time.time() + lease_time,
                )

                if _work_unit:
                    return WorkUnit(
                        self.registry, work_spec_name,
                        _work_unit[0], _work_unit[1])

            ## did not find work!
