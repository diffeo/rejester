'''

'''
from __future__ import absolute_import
from __future__ import division
import os
import sys
import time
import yaml
import pytest
import rejester
from rejester import TaskMaster
from rejester._logging import logger
from rejester._task_master import WORK_UNITS_, _FINISHED
from rejester.exceptions import PriorityRangeEmpty

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


def test_task_master_basic_interface(task_master):
    '''
    '''
    work_spec = dict(
        name = 'tbundle',
        desc = 'a test work bundle',
        min_gb = 8,
        config = dict(many='', params=''),
    )

    work_units = dict(foo={}, bar={})
    task_master.update_bundle(work_spec, work_units)

    assert task_master.registry.pull(WORK_UNITS_ + work_spec['name'])

    ## check that we ccannot get a task with memory below min_gb
    assert task_master.get_work(available_gb=3) == None

    work_unit = task_master.get_work(available_gb=13)
    assert work_unit.key in work_units

    work_unit.data['status'] = 10
    work_unit.update()

    assert task_master.registry.pull(WORK_UNITS_ + work_spec['name'])[work_unit.key]['status'] == 10

    work_unit.finish()
    work_unit.finish()

    assert task_master.registry.pull(WORK_UNITS_ + work_spec['name'] + _FINISHED)[work_unit.key]['status'] == 10   

    assert 'status' in task_master.inspect_work_unit(work_spec['name'], work_unit.key)

def test_task_master_reset_all(task_master):
    '''
    '''
    work_spec = dict(
        name = 'tbundle',
        desc = 'a test work bundle',
        min_gb = 8,
        config = dict(many='', params=''),
    )

    work_units = dict(foo={}, bar={})
    task_master.update_bundle(work_spec, work_units)
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_pending(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 2

    work_unit = task_master.get_work(available_gb=13)
    work_unit.data['status'] = 10
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 1
    assert task_master.num_pending(work_spec['name']) == 1

    work_unit.update()
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 1
    assert task_master.num_pending(work_spec['name']) == 1

    work_unit.finish()
    assert task_master.num_finished(work_spec['name']) == 1
    assert task_master.num_available(work_spec['name']) == 1
    assert task_master.num_pending(work_spec['name']) == 0

    task_master.reset_all(work_spec['name'])
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 2
    assert task_master.num_pending(work_spec['name']) == 0

    assert len(task_master.registry.pull(WORK_UNITS_ + work_spec['name'])) == 2
    with pytest.raises(PriorityRangeEmpty):
        with task_master.registry.lock() as session:
            session.popitem(WORK_UNITS_ + work_spec['name'], priority_max=-1)

    
@pytest.mark.performance
def test_task_master_throughput(task_master):
    '''
    '''
    ## 1KB sized work_spec config
    work_spec = dict(
        name = 'tbundle',
        desc = 'a test work bundle',
        min_gb = 8,
        config = dict(many=' ' * 2**10, params=''),
    )

    num_units = 10**6
    work_units = {str(x): {str(x): ' ' * 30} for x in xrange(num_units)}

    start = time.time()
    task_master.update_bundle(work_spec, work_units)
    elapsed = time.time() - start
    logger.info('%d work_units pushed in %.1f seconds --> %.1f units/sec',
                num_units, elapsed, num_units / elapsed)

    assert len(task_master.registry.pull(WORK_UNITS_ + work_spec['name'])) == num_units

    work_unit = task_master.get_work(available_gb=13)
    assert work_unit.key in work_units


def test_task_master_register(task_master):
    worker_id = task_master.register_worker()
    assert worker_id in task_master.registry.pull('workers')
    task_master.unregister_worker()
    assert not task_master.registry.pull('workers')
