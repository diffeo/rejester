"""Tests for rejester._queue

This software is released under an MIT/X11 open source license.

Copyright 2014 Diffeo, Inc.
"""

from __future__ import absolute_import

import logging
import os
import time

import pytest
import yaml

from rejester.exceptions import ItemInUseError, LostLease
from rejester._queue import RejesterQueue
from tests.rejester.make_namespace_string import make_namespace_string

logger = logging.getLogger('rejester.Queue')

@pytest.fixture(scope='function')
def queue(request):
    """a RejesterQueue that will die at the end of execution"""
    config_path = os.path.join(os.path.dirname(__file__), 'config_registry.yaml')
    with open(config_path, 'r') as f:
        config = yaml.load(f)
    config['namespace'] = make_namespace_string()
    q = RejesterQueue(config, 'queue')

    def fin():
        q.delete_namespace()
    
    request.addfinalizer(fin)
    return q

@pytest.fixture
def queue2(queue):
    """a second RejesterQueue on the same queue with a different worker ID"""
    return RejesterQueue(queue.config, queue.name)

def all_of_queue(queue):
    while True:
        item = queue.check_out_item(300)
        if item is None: return
        queue.return_item(item, None)
        yield item

def test_worker_id(queue):
    """simple worker_id test"""
    # since we start with a new namespace every time, the worker IDs
    # should start fresh too
    worker_id = queue.worker_id
    assert worker_id == 1

def test_one(queue):
    """Add 'a' with priority 1, and get it back"""
    queue.add_item('a', 1)
    item = queue.check_out_item(300)
    assert item == 'a'
    queue.return_item(item, None)
    item = queue.check_out_item(300)
    assert item is None

def test_one_helper(queue):
    """Add 'a' with priority 1, and get it back"""
    queue.add_item('a', 1)
    assert list(all_of_queue(queue)) == ['a']

def test_interesting_priorities(queue):
    """Zero, one, many"""
    queue.add_item('a', 1)
    queue.add_item('b', 1048576)
    queue.add_item('c', -65536)
    queue.add_item('d', 0)
    queue.add_item('e', 0.5)
    queue.add_item('f', -0.1)
    queue.add_item('g', 0.2)
    assert list(all_of_queue(queue)) == ['b', 'a', 'e', 'g', 'd', 'f', 'c']

def test_add_checked_out(queue):
    """Adding a checked-out item should fail"""
    queue.add_item('a', 1)
    item = queue.check_out_item(300)
    assert item == 'a'
    with pytest.raises(ItemInUseError):
        queue.add_item('a', 1)

def test_two(queue):
    """Add two items, out of order, and get them back"""
    queue.add_item('a', 2)
    queue.add_item('b', 1)
    assert list(all_of_queue(queue)) == ['a', 'b']
    queue.add_item('a', 1)
    queue.add_item('b', 2)
    assert list(all_of_queue(queue)) == ['b', 'a']

def test_reprioritize(queue):
    """Use add_item to change the priority of something"""
    queue.add_item('a', 1)
    queue.add_item('b', 2)
    queue.add_item('b', 0)
    assert list(all_of_queue(queue)) == ['a', 'b']

def test_requeue(queue):
    """Pull items out and put them back"""
    queue.add_item('a', 3)
    queue.add_item('b', 2)
    item = queue.check_out_item(300)
    assert item == 'a'
    queue.return_item(item, 3)
    item = queue.check_out_item(300)
    assert item == 'a'
    queue.return_item(item, 1)
    assert list(all_of_queue(queue)) == ['b', 'a']

def test_wrong_worker(queue, queue2):
    """If one queue instance checks out an item, another can't return it"""
    queue.add_item('a', 1)
    item = queue.check_out_item(300)
    assert item == 'a'
    # NOTE: this also implicitly tests that creating a new queue
    # object gets a different worker ID
    with pytest.raises(LostLease):
        queue2.return_item(item, None)

def test_check_out_two(queue):
    """Nothing prevents the same worker from checking out two things"""
    queue.add_item('a', 2)
    queue.add_item('b', 1)
    item = queue.check_out_item(300)
    assert item == 'a'
    item = queue.check_out_item(300)
    assert item == 'b'
    queue.return_item('a', None)
    queue.return_item('b', 1)
    assert list(all_of_queue(queue)) == ['b']

def test_reserve(queue, queue2):
    """Basic reservation/priority test

    1. Insert a@3, b@2
    2. Get, should return 'a'
    3. 'a' reserves 'b'
    4. Get, should be empty
    5. Return a@1
    6. Get, should return 'b'

    """
    queue.add_item('a', 3)
    queue.add_item('b', 2)
    item = queue.check_out_item(300)
    assert item == 'a'
    reserved = queue.reserve_items(item, 'b')
    assert reserved == ['b']
    assert list(all_of_queue(queue2)) == []
    queue.return_item(item, 1)
    assert list(all_of_queue(queue2)) == ['b', 'a']

def test_reserve_twice(queue):
    """Reserving an item that's already reserved is a no-op"""
    queue.add_item('a', 2)
    queue.add_item('b', 1)
    item = queue.check_out_item(300)
    assert item == 'a'
    reserved = queue.reserve_items(item, 'b')
    assert reserved == ['b']
    reserved = queue.reserve_items(item, 'b')
    assert reserved == []
    

def test_reserve_nonexistent(queue):
    """Reserving an item that doesn't exist is a no-op"""
    queue.add_item('a', 1)
    item = queue.check_out_item(300)
    assert item == 'a'
    reserved = queue.reserve_items(item, 'b')
    assert reserved == []
    queue.return_item(item, None)

def test_reserve_self(queue):
    """Reserving yourself is a no-op"""
    queue.add_item('a', 1)
    item = queue.check_out_item(300)
    assert item == 'a'
    reserved = queue.reserve_items(item, 'a')
    assert reserved == []
    queue.return_item(item, None)
    
def test_basic_expire(queue):
    """Basic expiration test"""
    queue.add_item('a', 1)
    item = queue.check_out_item(1)
    assert item == 'a'
    time.sleep(3)
    with pytest.raises(LostLease):
        queue.return_item(item, None)

def test_expiration_releases_reservations(queue, queue2):
    """If a reserves b, and a expires, then b should be released"""
    queue.add_item('a', 2)
    queue.add_item('b', 1)
    item = queue.check_out_item(1)
    assert item == 'a'
    reserved = queue.reserve_items(item, 'b')
    assert reserved == ['b']

    assert list(all_of_queue(queue2)) == []
    time.sleep(3)
    assert list(all_of_queue(queue2)) == ['a', 'b']

def test_renew(queue):
    """Basic renew test"""
    queue.add_item('a', 1)
    item = queue.check_out_item(2)
    assert item == 'a'
    time.sleep(1)
    queue.renew_item(item, 3)
    time.sleep(2)
    # at this point we have slept(3), which is after the original
    # expiration, so we should *not* get LostLease because we renewed
    queue.return_item(item, None)

def test_renew_overdue(queue):
    """Can't renew something that's already expired"""
    queue.add_item('a', 1)
    item = queue.check_out_item(1)
    assert item == 'a'
    time.sleep(3)
    with pytest.raises(LostLease):
        queue.renew_item(item, 3)

def test_dump(queue):
    """dump_queue() shouldn't crash; doesn't validate output"""
    queue.add_item('a', 1)
    queue.add_item('b', 0)
    item = queue.check_out_item(300)
    assert item == 'a'
    reserved = queue.reserve_items(item, 'b')
    assert reserved == ['b']
    queue.dump_queue('worker', 'available', 'priorities', 'expiration',
                     'workers', 'reservations_a', 'reservations_b', 'foo')
