'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division
import logging
import os
import sys
import time

import pytest

from rejester import TaskMaster
from rejester._task_master import WORK_UNITS_, _FINISHED
from rejester.exceptions import LostLease, NoSuchWorkUnitError

logger = logging.getLogger(__name__)
pytest_plugins = 'rejester.tests.fixtures'

work_spec = dict(
    name = 'tbundle',
    desc = 'a test work bundle',
    min_gb = 8,
    config = dict(many='', params=''),
    module = 'tests.rejester.test_workers',
    run_function = 'work_program',
    terminate_function = 'work_program',
)

def test_task_master_basic_interface(task_master):
    work_units = dict(foo={}, bar={})
    task_master.update_bundle(work_spec, work_units)

    assert task_master.registry.pull(WORK_UNITS_ + work_spec['name'])

    ## check that we ccannot get a task with memory below min_gb
    assert task_master.get_work('fake_worker_id', available_gb=3) == None

    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert work_unit.key in work_units

    work_unit.data['status'] = 10
    work_unit.update()

    assert task_master.registry.pull(WORK_UNITS_ + work_spec['name'])[work_unit.key]['status'] == 10

    work_unit.finish()
    work_unit.finish()

    assert task_master.registry.pull(WORK_UNITS_ + work_spec['name'] + _FINISHED)[work_unit.key]['status'] == 10   

    assert 'status' in task_master.inspect_work_unit(work_spec['name'], work_unit.key)

def test_list_work_units(task_master):
    work_units = dict(foo={ 'length': 3 }, foobar={ 'length': 6 })
    task_master.update_bundle(work_spec, work_units)

    # Initial check: both work units are there
    u = task_master.list_work_units(work_spec['name'])
    for k,v in u.iteritems():
        assert k in work_units
        assert 'length' in v
        assert len(k) == v['length']
    for k in work_units.iterkeys(): assert k in u

    # Start one unit; should still be there
    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert work_unit.key in work_units
    u = task_master.list_work_units(work_spec['name'])
    assert work_unit.key in u
    for k in u.iterkeys(): assert k in work_units
    for k in work_units.iterkeys(): assert k in u

    # Finish that unit; should be gone, the other should be there
    work_unit.finish()
    u = task_master.list_work_units(work_spec['name'])
    assert work_unit.key not in u
    for k in u.iterkeys(): assert k in work_units
    for k in work_units.iterkeys(): assert k == work_unit.key or k in u

def test_task_master_reset_all(task_master):
    work_units = dict(foo={}, bar={})
    task_master.update_bundle(work_spec, work_units)
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_pending(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 2

    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
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
    with task_master.registry.lock() as session:
        assert session.popitem(WORK_UNITS_ + work_spec['name'], priority_max=-1) is None

def test_task_master_retry(task_master):
    work_units = dict(foo={}, bar={})
    task_master.update_bundle(work_spec, work_units)
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_pending(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 2

    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    wuname = work_unit.key
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 1
    assert task_master.num_pending(work_spec['name']) == 1

    work_unit.fail(exc=Exception())
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_pending(work_spec['name']) == 0
    assert task_master.num_failed(work_spec['name']) == 1
    assert task_master.num_available(work_spec['name']) == 1

    task_master.retry(work_spec['name'], wuname)
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_pending(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 2

    with pytest.raises(NoSuchWorkUnitError):
        task_master.retry(work_spec['name'], wuname)

    assert (sorted(task_master.list_work_units(work_spec['name'])) ==
            ['bar', 'foo'])

    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert 'traceback' not in work_unit.data
    work_unit.finish()
    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert 'traceback' not in work_unit.data
    work_unit.finish()
    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert work_unit is None

def test_task_master_lost_lease(task_master):
    '''test that waiting too long to renew a lease allows another worker
    to get the lease and leads to LostLease exception in the worker
    that waited too long.
    '''
    work_units = dict(task_key_42=dict(data='hello'))
    task_master.update_bundle(work_spec, work_units)

    ## lease a WorkUnit with very short lease_time
    work_unit1 = task_master.get_work('fake_worker_id1', available_gb=13, lease_time=1)
    time.sleep(2)
    work_unit2 = task_master.get_work('fake_worker_id2', available_gb=13, lease_time=10)

    assert work_unit1.key == work_unit2.key
    assert work_unit1.worker_id == 'fake_worker_id1'
    assert work_unit2.worker_id == 'fake_worker_id2'

    with pytest.raises(LostLease):
        work_unit1.update()


@pytest.mark.performance
def test_task_master_throughput(task_master):
    '''exercises TaskMaster by pumping a million records through it
    '''
    num_units = 10**6
    work_units = {str(x): {str(x): ' ' * 30} for x in xrange(num_units)}

    start = time.time()
    task_master.update_bundle(work_spec, work_units)
    elapsed = time.time() - start
    logger.info('%d work_units pushed in %.1f seconds --> %.1f units/sec',
                num_units, elapsed, num_units / elapsed)

    assert len(task_master.registry.pull(WORK_UNITS_ + work_spec['name'])) == num_units

    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert work_unit.key in work_units
