'''
http://github.com/diffeo/rejester

.. This software is released under an MIT/X11 open source license.
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

def test_list_work_specs(task_master):
    # Initial state: nothing
    assert task_master.list_work_specs() == ([],None)
    
    work_units = dict(foo={ 'length': 3 }, foobar={ 'length': 6 })
    task_master.update_bundle(work_spec, work_units)

    specs, next = task_master.list_work_specs()
    specs = dict(specs)
    assert len(specs) == 1
    assert work_spec['name'] in specs
    assert specs[work_spec['name']]['desc'] == work_spec['desc']

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

def test_list_work_units_start_limit(task_master):
    work_units = dict(foo={ 'length': 3 }, bar={ 'length': 6 })
    task_master.update_bundle(work_spec, work_units)

    u = task_master.list_work_units(work_spec['name'], start=0, limit=1)
    assert u == { 'bar': { 'length': 6 } }

    u = task_master.list_work_units(work_spec['name'], start=1, limit=1)
    assert u == { 'foo': { 'length': 3 } }

    u = task_master.list_work_units(work_spec['name'], start=2, limit=1)
    assert u == { }

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


def test_worker_child(task_master):
    '''test the basic parent/child worker interface'''
    task_master.worker_register('child', mode=task_master.get_mode(),
                                parent='parent')
    try:
        assert task_master.get_child_work_units('parent') == {'child': None}
        assert task_master.get_child_work_units('child') == {}

        task_master.update_bundle(work_spec, {'k': {'kk': 'vv'}})
        wu = task_master.get_work('child', available_gb=13)
        assert wu is not None
        assert wu.key == 'k'

        cwus = task_master.get_child_work_units('parent')
        assert cwus.keys() == ['child']
        cwu = cwus['child']
        assert cwu.worker_id == 'child'
        assert cwu.work_spec_name == work_spec['name']
        assert cwu.key == wu.key
        assert cwu.expires == wu.expires

        assert task_master.get_child_work_units('child') == {}
    finally:
        task_master.worker_unregister('child', parent='parent')


def test_worker_child_expiry(task_master):
    '''test the parent/child interface when a job expires'''
    task_master.worker_register('child', mode=task_master.get_mode(),
                                parent='parent')
    try:
        assert task_master.get_child_work_units('parent') == {'child': None}
        assert task_master.get_child_work_units('child') == {}

        task_master.update_bundle(work_spec, {'k': {'kk': 'vv'}})
        wu = task_master.get_work('child', available_gb=13, lease_time=1)
        assert wu is not None
        assert wu.key == 'k'

        time.sleep(2)
        # Now the job is technically expired

        cwus = task_master.get_child_work_units('parent')
        assert cwus.keys() == ['child']
        cwu = cwus['child']
        assert cwu.worker_id == 'child'
        assert cwu.work_spec_name == work_spec['name']
        assert cwu.key == wu.key
        assert cwu.expires == wu.expires

        assert task_master.get_child_work_units('child') == {}
    finally:
        task_master.worker_unregister('child', parent='parent')


def test_worker_child_stolen(task_master):
    '''test the parent/child interface when a job expires'''
    task_master.worker_register('child', mode=task_master.get_mode(),
                                parent='parent')
    try:
        assert task_master.get_child_work_units('parent') == {'child': None}
        assert task_master.get_child_work_units('child') == {}

        task_master.update_bundle(work_spec, {'k': {'kk': 'vv'}})
        wu = task_master.get_work('child', available_gb=13, lease_time=1)
        assert wu is not None
        assert wu.key == 'k'

        time.sleep(2)
        wu = task_master.get_work('thief', available_gb=13)
        assert wu is not None
        assert wu.key == 'k'

        cwus = task_master.get_child_work_units('parent')
        assert cwus.keys() == ['child']
        cwu = cwus['child']
        assert cwu.worker_id == 'thief'
        assert cwu.work_spec_name == work_spec['name']
        assert cwu.key == wu.key
        assert cwu.expires == wu.expires

        assert task_master.get_child_work_units('child') == {}
    finally:
        task_master.worker_unregister('child', parent='parent')
