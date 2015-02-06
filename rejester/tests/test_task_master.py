'''Unit tests for the rejester task master.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division
import logging
import uuid

import pytest

from rejester._task_master import WORK_UNITS_, _FINISHED
from rejester.exceptions import LostLease, NoSuchWorkUnitError

logger = logging.getLogger(__name__)
pytest_plugins = 'rejester.tests.fixtures'

work_spec = {
    'name': 'tbundle',
    'desc': 'a test work bundle',
    'min_gb': 8,
    'config': {'many': '', 'params': ''},
    'module': 'tests.rejester.test_workers',
    'run_function': 'work_program',
    'terminate_function': 'work_program',
}


def test_task_master_basic_interface(task_master):
    work_units = dict(foo={}, bar={})
    task_master.update_bundle(work_spec, work_units)

    assert task_master.registry.pull(WORK_UNITS_ + work_spec['name'])

    # check that we ccannot get a task with memory below min_gb
    assert task_master.get_work('fake_worker_id', available_gb=3) is None

    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert work_unit.key in work_units

    work_unit.data['status'] = 10
    work_unit.update()

    assert task_master.registry.pull(
        WORK_UNITS_ + work_spec['name'])[work_unit.key]['status'] == 10

    work_unit.finish()
    work_unit.finish()

    assert task_master.registry.pull(
        WORK_UNITS_ + work_spec['name'] + _FINISHED)[work_unit.key][
            'status'] == 10

    assert 'status' in task_master.inspect_work_unit(
        work_spec['name'], work_unit.key)


def test_list_work_specs(task_master):
    # Initial state: nothing
    assert task_master.list_work_specs() == ([], None)

    work_units = dict(foo={'length': 3}, foobar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    specs, next = task_master.list_work_specs()
    specs = dict(specs)
    assert len(specs) == 1
    assert work_spec['name'] in specs
    assert specs[work_spec['name']]['desc'] == work_spec['desc']


def test_list_work_units(task_master):
    work_units = dict(foo={'length': 3}, foobar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    # Initial check: both work units are there
    u = task_master.list_work_units(work_spec['name'])
    for k, v in u.iteritems():
        assert k in work_units
        assert 'length' in v
        assert len(k) == v['length']
    assert sorted(work_units.keys()) == sorted(u.keys())

    # Start one unit; should still be there
    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert work_unit.key in work_units
    u = task_master.list_work_units(work_spec['name'])
    assert work_unit.key in u
    assert sorted(u.keys()) == sorted(work_units.keys())

    # Finish that unit; should be gone, the other should be there
    work_unit.finish()
    u = task_master.list_work_units(work_spec['name'])
    assert work_unit.key not in u
    assert all(k in work_units for k in u.iterkeys())
    assert all(k == work_unit.key or k in u for k in work_units.iterkeys())


def test_list_work_units_start_limit(task_master):
    work_units = dict(foo={'length': 3}, bar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    u = task_master.list_work_units(work_spec['name'], start=0, limit=1)
    assert u == {'bar': {'length': 6}}

    u = task_master.list_work_units(work_spec['name'], start=1, limit=1)
    assert u == {'foo': {'length': 3}}

    u = task_master.list_work_units(work_spec['name'], start=2, limit=1)
    assert u == {}


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
        assert session.popitem(WORK_UNITS_ + work_spec['name'],
                               priority_max=-1) is None


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


def test_task_master_lost_lease(task_master, monkeypatch):
    '''test that waiting too long to renew a lease allows another worker
    to get the lease and leads to LostLease exception in the worker
    that waited too long.
    '''
    monkeypatch.setattr('time.time', lambda: 100000000)
    work_units = dict(task_key_42=dict(data='hello'))
    task_master.update_bundle(work_spec, work_units)

    work_unit1 = task_master.get_work('fake_worker_id1', available_gb=13,
                                      lease_time=30)
    monkeypatch.setattr('time.time', lambda: 100000060)
    work_unit2 = task_master.get_work('fake_worker_id2', available_gb=13,
                                      lease_time=30)

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


def test_worker_child_expiry(task_master, monkeypatch):
    '''test the parent/child interface when a job expires'''
    monkeypatch.setattr('time.time', lambda: 100000000)
    task_master.worker_register('child', mode=task_master.get_mode(),
                                parent='parent')
    try:
        assert task_master.get_child_work_units('parent') == {'child': None}
        assert task_master.get_child_work_units('child') == {}

        task_master.update_bundle(work_spec, {'k': {'kk': 'vv'}})
        wu = task_master.get_work('child', available_gb=13, lease_time=30)
        assert wu is not None
        assert wu.key == 'k'

        monkeypatch.setattr('time.time', lambda: 100000060)
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


def test_worker_child_stolen(task_master, monkeypatch):
    '''test the parent/child interface when a job expires'''
    monkeypatch.setattr('time.time', lambda: 100000000)
    task_master.worker_register('child', mode=task_master.get_mode(),
                                parent='parent')
    try:
        assert task_master.get_child_work_units('parent') == {'child': None}
        assert task_master.get_child_work_units('child') == {}

        task_master.update_bundle(work_spec, {'k': {'kk': 'vv'}})
        wu = task_master.get_work('child', available_gb=13, lease_time=30)
        assert wu is not None
        assert wu.key == 'k'

        monkeypatch.setattr('time.time', lambda: 100000060)
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


def test_task_master_binary_work_unit(task_master):
    work_units = {
        b'\x00': {'k': b'\x00', 't': 'single null'},
        b'\x00\x01\x02\x03': {'k': b'\x00\x01\x02\x03', 't': 'control chars'},
        b'\x00a\x00b': {'k': b'\x00a\x00b', 't': 'UTF-16BE'},
        b'a\x00b\x00': {'k': b'a\x00b\x00', 't': 'UTF-16LE'},
        b'f\xc3\xbc': {'k': b'f\xc3\xbc', 't': 'UTF-8'},
        b'f\xfc': {'k': b'f\xfc', 't': 'ISO-8859-1'},
        b'\xf0\x0f': {'k': b'\xf0\x0f', 't': 'F00F'},
        b'\xff': {'k': b'\xff', 't': 'FF'},
        b'\xff\x80': {'k': b'\xff\x80', 't': 'FF80'},
    }
    task_master.update_bundle(work_spec, work_units)

    assert task_master.list_work_units(work_spec['name']) == work_units

    completed = []
    for _ in xrange(len(work_units)):
        wu = task_master.get_work('fake_worker_id', available_gb=13)
        assert wu.key in work_units
        assert wu.data == work_units[wu.key]
        completed.append(wu.key)
        wu.finish()

    wu = task_master.get_work('fake_worker_id', available_gb=13)
    assert wu is None

    assert sorted(completed) == sorted(work_units.keys())

    assert (task_master.list_finished_work_units(work_spec['name']) ==
            work_units)


def test_task_master_work_unit_value(task_master):
    work_units = {
        'k': {'list': [1, 2, 3],
              'tuple': (4, 5, 6),
              'mixed': [1, (2, [3, 4])],
              'uuid': uuid.UUID('01234567-89ab-cdef-0123-456789abcdef'),
              'str': b'foo',
              'unicode': u'foo',
              'unicode2': u'f\u00fc'}
    }
    task_master.update_bundle(work_spec, work_units)
    assert task_master.list_work_units(work_spec['name']) == work_units
