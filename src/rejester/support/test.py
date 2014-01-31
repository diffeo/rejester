"""py.test support for rejester

Purpose
=======

This module provides common py.test fixtures for various rejester
features.  They will clean up after themselves, taking care to not, for
instance, leak namespaces in the underlying redis database.


Implementation Notes
====================

If rejester is already installed in your Python environment, these
fixtures should be automatically available.  If not, you can add
configuration to your test module

>>> pytest_plugins = 'rejester.support.test'

Your tests will then have fixtures like ``task_master`` and
``rejester_queue`` available to them, with automatic namespace
management.  Your test directory should also have a file named
``config_registry.yaml`` that looks like

    registry_addresses: ['redis.example.com:6379']

You can usually get the namespace string from the configuration of
one of the high-level objects.  If you must manually work with the
namespace string outside of the managed objects, you can call
``make_namespace_string()`` or depend on the ``_rejester_namespace``
fixture.


This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.

"""

import getpass
import hashlib
import os
import socket

import pytest
import yaml

from rejester import RejesterQueue, TaskMaster

def make_namespace_string(test_name=''):
    '''
    generates a descriptive namespace for testing, which is unique to
    this user and also this process ID and host running the test.

    This also ensures that the namespace name is shorter than 48 chars
    and obeys the other constraints of the various backend DBs that we
    use.
    '''
    return '_'.join([
            test_name[-25:], 
            getpass.getuser().replace('-', '_')[:5],
            str(os.getpid()),
            hashlib.md5(socket.gethostname()).hexdigest()[:4],
            ])

@pytest.fixture(scope='function')
def _rejester_namespace(request):
    """A dynamically constructed rejester namespace.

    This is used by other fixtures to ensure that they use the same
    namespace.  If you use this directly, you are responsible for
    cleaning up the namespace when your test ends."""
    return make_namespace_string(request.node.name)

@pytest.fixture(scope='function')
def _rejester_config(request):
    """Basic Rejester configuration.

    If the test directory contains a file ``config_registry.yaml``,
    the configuration is loaded from there.  Otherwise this returns
    an empty dictionary.

    """
    config_path = request.fspath.new(basename='config_registry.yaml')
    try:
        with config_path.open('r') as f:
            return yaml.load(f)
    except OSError, exc:
        return {}

@pytest.fixture(scope='function')
def _rejester_namespace2(_rejester_namespace):
    """A second dynamically constructed rejester namespace.

    This is used by other fixtures to ensure that they use the same
    namespace.  If you use this directly, you are responsible for
    cleaning up the namespace when your test ends."""
    return _rejester_namespace + '_second'

@pytest.fixture(scope='function')
def task_master(request, _rejester_config, _rejester_namespace):
    """A rejester TaskMaster for loading or querying work.

    The TaskMaster will have a dynamically created namespace.
    When the test completes the namespace will be torn down.
    If you need the namespace name for other purposes, it will be
    in ``task_master.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = _rejester_namespace
    tm = TaskMaster(config)
    def fin():
        tm.registry.delete_namespace()
    request.addfinalizer(fin)
    return tm

@pytest.fixture(scope='function')
def task_master2(request, _rejester_config, _rejester_namespace2):
    """A rejester TaskMaster on a second namespace.

    The TaskMaster will have a dynamically created namespace, distinct
    from task_master's.  When the test completes the namespace will be
    torn down.  If you need the namespace name for other purposes, it
    will be in ``task_master2.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = _rejester_namespace2
    tm = TaskMaster(config)
    def fin():
        tm.registry.delete_namespace()
    request.addfinalizer(fin)
    return tm

@pytest.fixture(scope='function')
def rejester_queue(request, _rejester_config, _rejester_namespace):
    """A RejesterQueue that will die at the end of execution.

    The queue will will be named 'queue', and will have a dynamically
    created namespace.  When the test completes the namespace will be
    torn down.  If you need the namespace for other purposes, it will
    be in ``rejester_queue.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = _rejester_namespace
    q = RejesterQueue(config, 'queue')
    def fin():
        q.delete_namespace()
    request.addfinalizer(fin)
    return q

@pytest.fixture(scope='function')
def rejester_queue2(rejester_queue):
    """A copy of rejester_queue with a distinct worker ID"""
    return RejesterQueue(rejester_queue.config, rejester_queue.name)
