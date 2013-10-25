'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
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

    def update(self, lease_time=300):
        '''refresh the task lease by incrementing lease_time ahead of now
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

    @classmethod
    def validate_work_spec(cls, work_spec):
        'raise ProgrammerError if work_spec is invalid'
        if 'name' not in work_spec:
            raise ProgrammerError('work_spec lacks "name"')
        if 'min_gb' not in work_spec or \
                not isinstance(work_spec['min_gb'], (float, int)):
            raise ProgrammerError('work_spec["min_gb"] must be a number')

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
        if not available_gb:
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
