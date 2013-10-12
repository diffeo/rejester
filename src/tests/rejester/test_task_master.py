'''

'''
from __future__ import absolute_import
import os
import sys
import yaml
import pytest
import rejester
from rejester import TaskMaster
from rejester._logging import logger

from tests.rejester.make_namespace_string import make_namespace_string

@pytest.fixture(scope='function')
def task_master(request):
    config_path = os.path.join(os.path.dirname(__file__), 'config_registry.yaml')
    if not os.path.exists(config_path):
        sys.exit('failed to find test config: %r' % config_path)

    try:
        config = yaml.load(open(config_path))
    except Exception, exc:
        sys.exit('failed to load %r: %s' % (config_path, exc))

    config['namespace'] = make_namespace_string()

    def fin():
        task_master = TaskMaster(config)
        task_master.registry.delete_namespace()
    request.addfinalizer(fin)

    task_master = TaskMaster(config)
    return task_master


def test_task_master(task_master):
    '''
    '''
    work_spec = dict(
        name = 'tbundle',
        desc = 'a test work bundle',
        mingb = 8,
        config = dict(many='', params=''),
        work_unit = tuple(),
    )

    task_master.update_bundle(work_spec, dict(foo={}, bar={}))

    
