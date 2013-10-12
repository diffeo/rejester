'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

from rejester._registry import Registry

BUNDLES = 'bundles'


class TaskMaster(object):
    '''
    work_spec = dict(
        name = 
        desc = 
        mingb = 
        config = dict -- like we have in yaml files now
        work_unit = tuple -- like we emit with i_str now in streamcorpus.pipeline
    )

    operations:
      update bundle -- appends latest work_spec to list
    '''

    def __init__(self, config):
        config['app_name'] = 'rejester'
        self.registry = Registry(config)

    def update_bundle(self, work_spec, work_units, nice=0):
        '''

        '''
        work_spec_name = work_spec['name']
        with self.registry.lock(atime=1000) as session:
            session.update(
                'nice_levels',
                dict(work_spec_name=nice),
            )
            session.update(
                'work_specs',
                dict(work_spec_name=work_spec),
            )
            session.update(
                'work_units_' + work_spec_name,
                work_units,
            )

    def nice(self, work_spec_name, nice):        
        with self.registry.lock(atime=1000) as session:
            session.update(
                'nice_levels',
                dict(work_spec_name=nice),
            )

    def get_task(self, worker_class):
        pass

