"""Various dependency tests for TaskMaster.

-----

This software is released under an MIT/X11 open source license.

Copyright 2014 Diffeo, Inc.

"""
from __future__ import absolute_import
import logging

import pytest

from rejester import TaskMaster
from rejester.exceptions import NoSuchWorkSpecError

logger = logging.getLogger(__name__)
pytest_plugins = 'rejester.tests.fixtures'

work_spec_a = { 'name': 'a', 'min_gb': 1 }
work_spec_b = { 'name': 'b', 'min_gb': 1 }
work_units_aa = { 'aa': { 'x': 'x' } }
work_units_bb = { 'bb': { 'x': 'x' } }
work_units_cc = { 'cc': { 'x': 'x' } }

def get_and_check_unit(task_master, spec, unit, fail=False):
    u = task_master.get_work('worker', available_gb=4)
    assert u.work_spec_name == spec
    assert u.key == unit
    if fail:
        u.fail()
    else:
        u.finish()
    return u

def get_and_check_done(task_master):
    unit = task_master.get_work('worker', available_gb=4)
    assert unit is None

def test_two_no_dependency(task_master):
    """Add two tasks, which can run in either order"""
    task_master.update_bundle(work_spec_a, work_units_aa, nice=0)
    task_master.update_bundle(work_spec_b, work_units_bb, nice=1)
    u1 = task_master.get_work('worker', available_gb=4)
    assert u1 is not None
    u1.finish()
    u2 = task_master.get_work('worker', available_gb=4)
    assert u2 is not None
    u2.finish()
    # We expect u1 to be 'a' slightly more often than 'b', but both
    # should be present
    assert u1.work_spec_name in ['a', 'b']
    if u1.work_spec_name == 'a':
        assert u1.key == 'aa'
        assert u2.work_spec_name == 'b'
        assert u2.key == 'bb'
    else:
        assert u1.key == 'bb'
        assert u2.work_spec_name == 'a'
        assert u2.key == 'aa'
    get_and_check_done(task_master)

def test_two_simple_dependency(task_master):
    """Set a dependency so the tasks run out of order"""
    task_master.update_bundle(work_spec_a, work_units_aa, nice=0)
    task_master.update_bundle(work_spec_b, work_units_bb, nice=1)
    res = task_master.add_dependent_work_units(
        ('a', 'aa', None), ('b', 'bb', None))
    assert res == True
    get_and_check_unit(task_master, 'b', 'bb')
    get_and_check_unit(task_master, 'a', 'aa')
    get_and_check_done(task_master)

def test_two_hard_dependency(task_master):
    """If the depends_on task fails, the work_unit should fail too"""
    task_master.update_bundle(work_spec_a, work_units_aa, nice=0)
    task_master.update_bundle(work_spec_b, work_units_bb, nice=1)
    res = task_master.add_dependent_work_units(
        ('a', 'aa', None), ('b', 'bb', None), hard=True)
    assert res == True
    get_and_check_unit(task_master, 'b', 'bb', fail=True)
    get_and_check_done(task_master)
    assert task_master.num_failed('a') == 1
    assert task_master.num_failed('b') == 1

def test_two_soft_dependency(task_master):
    """If the depends_on task fails, the work_unit still runs"""
    task_master.update_bundle(work_spec_a, work_units_aa, nice=0)
    task_master.update_bundle(work_spec_b, work_units_bb, nice=1)
    res = task_master.add_dependent_work_units(
        ('a', 'aa', None), ('b', 'bb', None), hard=False)
    get_and_check_unit(task_master, 'b', 'bb', fail=True)
    get_and_check_unit(task_master, 'a', 'aa')
    get_and_check_done(task_master)
    assert task_master.num_failed('a') == 0
    assert task_master.num_failed('b') == 1

def test_two_caller_defines(task_master):
    """Actually define the tasks via the add_dependent function"""
    task_master.update_bundle(work_spec_a, {}, nice=0)
    task_master.update_bundle(work_spec_b, {}, nice=1)
    res = task_master.add_dependent_work_units(
        ('a', 'aa', {'hello': 'there'}), ('b', 'bb', {'hello': 'world'}))
    assert res == True
    unit = get_and_check_unit(task_master, 'b', 'bb')
    assert unit.data['hello'] == 'world'
    unit = get_and_check_unit(task_master, 'a', 'aa')
    assert unit.data['hello'] == 'there'
    get_and_check_done(task_master)

def test_two_caller_redefines(task_master):
    """add_dependent changes the definitions"""
    task_master.update_bundle(work_spec_a, {'aa': {'hello': 'world'}}, nice=0)
    task_master.update_bundle(work_spec_b, {'bb': {'hello': 'there'}}, nice=1)
    res = task_master.add_dependent_work_units(
        ('a', 'aa', {'hello': 'kitty'}), ('b', 'bb', {'hello': 'goodbye'}))
    assert res == True
    unit = get_and_check_unit(task_master, 'b', 'bb')
    assert unit.data['hello'] == 'goodbye'
    unit = get_and_check_unit(task_master, 'a', 'aa')
    assert unit.data['hello'] == 'kitty'
    get_and_check_done(task_master)

def test_missing_units(task_master):
    """Dependencies on units that don't exist"""
    task_master.update_bundle(work_spec_a, work_units_aa)
    task_master.update_bundle(work_spec_b, work_units_bb)
    res = task_master.add_dependent_work_units(
        ('a', 'aa', None), ('a', 'bb', None))
    assert res is False
    res = task_master.add_dependent_work_units(
        ('b', 'aa', None), ('b', 'bb', None))
    assert res is False
    with pytest.raises(NoSuchWorkSpecError):
        task_master.add_dependent_work_units(
            ('a', 'aa', None), ('c', 'cc', None))
    with pytest.raises(NoSuchWorkSpecError):
        task_master.add_dependent_work_units(
            ('c', 'cc', None), ('b', 'bb', None))

def test_missing_units_definitions(task_master):
    """Dependencies on units that don't exist, with partial definitions"""
    task_master.update_bundle(work_spec_a, work_units_aa)
    task_master.update_bundle(work_spec_b, work_units_bb)
    res = task_master.add_dependent_work_units(
        ('a', 'bb', { 'x': 'x' }), ('a', 'cc', None))
    assert res is False
    # This defines a/bb, and so
    res = task_master.add_dependent_work_units(
        ('a', 'aa', None), ('a', 'bb', None))
    assert res is True
    # Same test, other direction
    res = task_master.add_dependent_work_units(
        ('b', 'aa', None), ('b', 'cc', { 'x': 'x' }))
    assert res is False
    res = task_master.add_dependent_work_units(
        ('b', 'bb', None), ('b', 'cc', None))
    assert res is True
    # Can't create otherwise valid units in missing work specs
    with pytest.raises(NoSuchWorkSpecError):
        task_master.add_dependent_work_units(
            ('a', 'aa', None), ('c', 'cc', { 'x': 'x' }))
    with pytest.raises(NoSuchWorkSpecError):
        task_master.add_dependent_work_units(
            ('c', 'cc', { 'x': 'x' }), ('b', 'bb', { 'x': 'x' }))
    with pytest.raises(NoSuchWorkSpecError):
        task_master.add_dependent_work_units(
            ('c', 'aa', { 'x': 'x' }), ('c', 'bb', { 'x': 'x' }))
 
def test_three(task_master):
    """A depends on both B and C"""
    task_master.update_bundle(work_spec_a, work_units_aa, nice=0)
    task_master.update_bundle(work_spec_b, work_units_bb, nice=1)
    task_master.update_bundle(work_spec_b, work_units_cc, nice=2)
    # so natural order is a/aa, b/bb, b/cc
    res = task_master.add_dependent_work_units(('a', 'aa', None),
                                               ('b', 'bb', None))
    assert res is True
    res = task_master.add_dependent_work_units(('a', 'aa', None),
                                               ('b', 'cc', None))
    assert res is True
    # now aa can't run until bb and cc do, bb should be first
    get_and_check_unit(task_master, 'b', 'bb')
    get_and_check_unit(task_master, 'b', 'cc')
    get_and_check_unit(task_master, 'a', 'aa')
    get_and_check_done(task_master)

def test_three_failure(task_master):
    """As test_three, but the first task fails"""
    task_master.update_bundle(work_spec_a, work_units_aa, nice=0)
    task_master.update_bundle(work_spec_b, work_units_bb, nice=1)
    task_master.update_bundle(work_spec_b, work_units_cc, nice=2)
    res = task_master.add_dependent_work_units(('a', 'aa', None),
                                               ('b', 'bb', None))
    assert res is True
    res = task_master.add_dependent_work_units(('a', 'aa', None),
                                               ('b', 'cc', None))
    assert res is True

    get_and_check_unit(task_master, 'b', 'bb', fail=True)
    # a/aa should be failed right now
    assert task_master.num_failed('a') == 1
    assert task_master.num_failed('b') == 1
    assert task_master.num_available('b') == 1
    get_and_check_unit(task_master, 'b', 'cc')
    get_and_check_done(task_master)

def test_three_soft_failure(task_master):
    """As test_three_failure, but aa can still run"""
    task_master.update_bundle(work_spec_a, work_units_aa, nice=0)
    task_master.update_bundle(work_spec_b, work_units_bb, nice=1)
    task_master.update_bundle(work_spec_b, work_units_cc, nice=2)
    res = task_master.add_dependent_work_units(('a', 'aa', None),
                                               ('b', 'bb', None), hard=False)
    assert res is True
    res = task_master.add_dependent_work_units(('a', 'aa', None),
                                               ('b', 'cc', None), hard=False)
    assert res is True

    get_and_check_unit(task_master, 'b', 'bb', fail=True)
    get_and_check_unit(task_master, 'b', 'cc', fail=True)
    get_and_check_unit(task_master, 'a', 'aa')
    get_and_check_done(task_master)
    assert task_master.num_failed('a') == 0
    assert task_master.num_finished('a') == 1
    assert task_master.num_failed('b') == 2
    assert task_master.num_finished('b') == 0
