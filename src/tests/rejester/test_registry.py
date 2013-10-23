'''

'''
from __future__ import absolute_import
import os
import sys
import yaml
import time
import pytest
import rejester
from rejester import Registry
from rejester._logging import logger
from rejester.exceptions import PriorityRangeEmpty
from collections import defaultdict
from tests.rejester.make_namespace_string import make_namespace_string

@pytest.fixture(scope='function')
def registry(request):
    config_path = os.path.join(os.path.dirname(__file__), 'config_registry.yaml')
    if not os.path.exists(config_path):
        sys.exit('failed to find test config: %r' % config_path)

    try:
        config = yaml.load(open(config_path))
    except Exception, exc:
        sys.exit('failed to load %r: %s' % (config_path, exc))

    config['app_name'] = 'rejester_test'
    config['namespace'] = make_namespace_string()

    def fin():
        registry = Registry(config)
        registry.delete_namespace()
    request.addfinalizer(fin)

    registry = Registry(config)
    return registry


def test_registry_lock_block(registry):
    with registry.lock(ltime=100) as session1:
        ## check that we cannot get the lock
        with pytest.raises(rejester.exceptions.LockError):
            with registry.lock(atime=1) as session2:
                pass


def test_registry_lock_loss(registry):
    with pytest.raises(rejester.exceptions.EnvironmentError) as env_error:
        with registry.lock(ltime=1) as session1:
            ## check that we can get the lock
            with registry.lock(atime=100) as session2:
                assert session2
    assert 'Lost lock' in str(env_error)


def test_registry_update_pull(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict


def test_registry_get(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict
        assert session.get('test_dict', 'cars') == 10
        assert session.get('test_dict', 'not-there', 'hello') == 'hello'


def test_registry_set(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock(atime=5000) as session:
        session.set('test_dict', 'cars', 10)
        session.set('test_dict', 'houses', 5)
        assert session.pull('test_dict') == test_dict


def test_registry_popmany(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict
        session.popmany('test_dict', 'dogs', 'cars', 'houses')
        assert session.pull('test_dict') == dict()


def test_registry_popitem(registry):
    test_dict = dict(cars=10, houses=5)

    recovered = set()
    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict
        recovered.add(session.popitem('test_dict'))
        recovered.add(session.popitem('test_dict'))
        assert recovered == set(test_dict.items())

        with pytest.raises(PriorityRangeEmpty):
            session.popitem('test_dict')


def test_registry_popitem_priority(registry):
    test_dict = dict(cars=10, houses=5, dogs=99)

    recovered = set()
    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict, priorities=defaultdict(lambda: 10))
        assert session.pull('test_dict') == test_dict
        with pytest.raises(PriorityRangeEmpty):
            session.popitem('test_dict', priority_min=100)
        recovered.add(session.popitem('test_dict', priority_min=-100))
        recovered.add(session.popitem('test_dict', priority_min=10))
        recovered.add(session.popitem('test_dict'))
        assert recovered == set(test_dict.items())


def test_registry_popitem_move(registry):
    test_dict = dict(cars=10, houses=5)

    recovered = set()
    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict

        recovered.add(session.popitem_move('test_dict', 'second'))
        assert session.len('test_dict') == 1
        assert session.len('second') == 1
        recovered.add(session.popitem_move('test_dict', 'second'))
        assert session.len('test_dict') == 0
        assert session.len('second') == 2

        assert recovered == set(test_dict.items())
        assert recovered == set(session.pull('second').items())


def test_registry_popitem_move_priority(registry):
    test_dict = dict(cars=10, houses=5, dogs=99)

    recovered = set()
    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict, priorities=defaultdict(lambda: 10))
        assert session.pull('test_dict') == test_dict
        with pytest.raises(PriorityRangeEmpty):
            session.popitem_move('test_dict','second',  priority_min=100)
        recovered.add(session.popitem_move('test_dict', 'second', priority_min=-100))
        recovered.add(session.popitem_move('test_dict', 'second', priority_min=10))
        recovered.add(session.popitem_move('test_dict', 'second'))
        assert recovered == set(test_dict.items())
        assert recovered == set(session.pull('second').items())


def test_registry_popitem_move_empty(registry):
    test_dict = dict(cars=10, houses=5)

    recovered = set()
    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict

        session.popitem_move('test_dict', 'second')
        session.popitem_move('test_dict', 'second')
        assert session.len('test_dict') == 0

        with pytest.raises(PriorityRangeEmpty):
            session.popitem_move('test_dict', 'second')

def test_registry_move(registry):
    test_dict = dict(cars=10, houses=5)

    recovered = set()
    with registry.lock(atime=5000) as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict

        assert session.len('test_dict') == 2
        session.move('test_dict', 'second', dict(cars=3))
        assert session.len('test_dict') == 1
        assert session.len('second') == 1
        session.move('test_dict', 'second', dict(houses=2))
        assert session.len('test_dict') == 0
        assert session.len('second') == 2

        assert dict(cars=3, houses=2) == session.pull('second')

    
def test_registry_increment(registry):

    with registry.lock(atime=5000) as session:
        session.increment('test_dict', 'foo', 4)
        assert session.pull('test_dict') == dict(foo=4)
        session.increment('test_dict', 'foo', 0.5)
        assert session.pull('test_dict') == dict(foo=4.5)
