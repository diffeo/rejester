"""Tests for rejester._queue

This software is released under an MIT/X11 open source license.

Copyright 2014 Diffeo, Inc.
"""

from __future__ import absolute_import

import logging
import os
import time

import pytest

from rejester.exceptions import ItemInUseError, LostLease

logger = logging.getLogger(__name__)
pytest_plugins = 'rejester.tests.fixtures'

def all_of_queue(queue):
    while True:
        item = queue.check_out_item(300)
        if item is None: return
        queue.return_item(item, None)
        yield item

def test_worker_id(rejester_queue):
    """simple worker_id test"""
    # since we start with a new namespace every time, the worker IDs
    # should start fresh too
    worker_id = rejester_queue.worker_id
    assert worker_id == 1

def test_one(rejester_queue):
    """Add 'a' with priority 1, and get it back"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    rejester_queue.return_item(item, None)
    item = rejester_queue.check_out_item(300)
    assert item is None

def test_one_helper(rejester_queue):
    """Add 'a' with priority 1, and get it back"""
    rejester_queue.add_item('a', 1)
    assert list(all_of_queue(rejester_queue)) == ['a']

def test_interesting_priorities(rejester_queue):
    """Zero, one, many"""
    rejester_queue.add_item('a', 1)
    rejester_queue.add_item('b', 1048576)
    rejester_queue.add_item('c', -65536)
    rejester_queue.add_item('d', 0)
    rejester_queue.add_item('e', 0.5)
    rejester_queue.add_item('f', -0.1)
    rejester_queue.add_item('g', 0.2)
    assert list(all_of_queue(rejester_queue)) == ['b', 'a', 'e', 'g', 'd', 'f', 'c']

def test_add_checked_out(rejester_queue):
    """Adding a checked-out item should fail"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    with pytest.raises(ItemInUseError):
        rejester_queue.add_item('a', 1)

def test_two(rejester_queue):
    """Add two items, out of order, and get them back"""
    rejester_queue.add_item('a', 2)
    rejester_queue.add_item('b', 1)
    assert list(all_of_queue(rejester_queue)) == ['a', 'b']
    rejester_queue.add_item('a', 1)
    rejester_queue.add_item('b', 2)
    assert list(all_of_queue(rejester_queue)) == ['b', 'a']

def test_reprioritize(rejester_queue):
    """Use add_item to change the priority of something"""
    rejester_queue.add_item('a', 1)
    rejester_queue.add_item('b', 2)
    rejester_queue.add_item('b', 0)
    assert list(all_of_queue(rejester_queue)) == ['a', 'b']

def test_requeue(rejester_queue):
    """Pull items out and put them back"""
    rejester_queue.add_item('a', 3)
    rejester_queue.add_item('b', 2)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    rejester_queue.return_item(item, 3)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    rejester_queue.return_item(item, 1)
    assert list(all_of_queue(rejester_queue)) == ['b', 'a']

def test_wrong_worker(rejester_queue, rejester_queue2):
    """If one queue instance checks out an item, another can't return it"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    # NOTE: this also implicitly tests that creating a new queue
    # object gets a different worker ID
    with pytest.raises(LostLease):
        rejester_queue2.return_item(item, None)

def test_check_out_two(rejester_queue):
    """Nothing prevents the same worker from checking out two things"""
    rejester_queue.add_item('a', 2)
    rejester_queue.add_item('b', 1)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    item = rejester_queue.check_out_item(300)
    assert item == 'b'
    rejester_queue.return_item('a', None)
    rejester_queue.return_item('b', 1)
    assert list(all_of_queue(rejester_queue)) == ['b']

def test_reserve(rejester_queue, rejester_queue2):
    """Basic reservation/priority test

    1. Insert a@3, b@2
    2. Get, should return 'a'
    3. 'a' reserves 'b'
    4. Get, should be empty
    5. Return a@1
    6. Get, should return 'b'

    """
    rejester_queue.add_item('a', 3)
    rejester_queue.add_item('b', 2)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    reserved = rejester_queue.reserve_items(item, 'b')
    assert reserved == ['b']
    assert list(all_of_queue(rejester_queue2)) == []
    rejester_queue.return_item(item, 1)
    assert list(all_of_queue(rejester_queue2)) == ['b', 'a']

def test_reserve_twice(rejester_queue):
    """Reserving an item that's already reserved is a no-op"""
    rejester_queue.add_item('a', 2)
    rejester_queue.add_item('b', 1)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    reserved = rejester_queue.reserve_items(item, 'b')
    assert reserved == ['b']
    reserved = rejester_queue.reserve_items(item, 'b')
    assert reserved == []
    

def test_reserve_nonexistent(rejester_queue):
    """Reserving an item that doesn't exist is a no-op"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    reserved = rejester_queue.reserve_items(item, 'b')
    assert reserved == []
    rejester_queue.return_item(item, None)

def test_reserve_self(rejester_queue):
    """Reserving yourself is a no-op"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    reserved = rejester_queue.reserve_items(item, 'a')
    assert reserved == []
    rejester_queue.return_item(item, None)
    
def test_basic_expire(rejester_queue):
    """Basic expiration test"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(1)
    assert item == 'a'
    time.sleep(3)
    with pytest.raises(LostLease):
        rejester_queue.return_item(item, None)

def test_expiration_releases_reservations(rejester_queue, rejester_queue2):
    """If a reserves b, and a expires, then b should be released"""
    rejester_queue.add_item('a', 2)
    rejester_queue.add_item('b', 1)
    item = rejester_queue.check_out_item(1)
    assert item == 'a'
    reserved = rejester_queue.reserve_items(item, 'b')
    assert reserved == ['b']

    assert list(all_of_queue(rejester_queue2)) == []
    time.sleep(3)
    assert list(all_of_queue(rejester_queue2)) == ['a', 'b']

def test_renew(rejester_queue):
    """Basic renew test"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(2)
    assert item == 'a'
    time.sleep(1)
    rejester_queue.renew_item(item, 3)
    time.sleep(2)
    # at this point we have slept(3), which is after the original
    # expiration, so we should *not* get LostLease because we renewed
    rejester_queue.return_item(item, None)

def test_renew_overdue(rejester_queue):
    """Can't renew something that's already expired"""
    rejester_queue.add_item('a', 1)
    item = rejester_queue.check_out_item(1)
    assert item == 'a'
    time.sleep(3)
    with pytest.raises(LostLease):
        rejester_queue.renew_item(item, 3)

def test_dump(rejester_queue):
    """dump_queue() shouldn't crash; doesn't validate output"""
    rejester_queue.add_item('a', 1)
    rejester_queue.add_item('b', 0)
    item = rejester_queue.check_out_item(300)
    assert item == 'a'
    reserved = rejester_queue.reserve_items(item, 'b')
    assert reserved == ['b']
    rejester_queue.dump_queue('worker', 'available', 'priorities', 'expiration',
                              'workers', 'reservations_a', 'reservations_b',
                              'foo')
