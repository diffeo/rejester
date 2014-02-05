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
import re
import socket

import pytest
import yaml

from rejester import RejesterQueue, TaskMaster

def make_namespace_string(test_name=''):
    '''
    generates a descriptive namespace for testing, which is unique to
    this user and also this process ID and host running the test.

    The returned string is never longer than 40 characters, and never
    contains non-alphanumeric-underscore characters.  (It matches the
    regular expression [a-zA-Z0-9_]{,40}.)
    '''
    return '_'.join([
            re.sub('\W', '', test_name)[-23:], 
            getpass.getuser().replace('-', '_')[:5],
            str(os.getpid())[-5:],
            hashlib.md5(socket.gethostname()).hexdigest()[:4],
            ])

@pytest.fixture(scope='function')
def _namespace_string(request):
    """A dynamically namespace string.

    This is used by other fixtures to ensure that they use the same
    namespace.  If you use this directly, you are responsible for
    cleaning up the namespace when your test ends.  This can be reused
    by other components that have the notion of a "namespace".

    """
    return make_namespace_string(request.node.name)

@pytest.fixture(scope='function')
def _rejester_namespace(_namespace_string):
    """A dynamically constructed rejester namespace.

    This is used by other fixtures to ensure that they use the same
    namespace.  If you use this directly, you are responsible for
    cleaning up the namespace when your test ends.

    *Deprecated:* use `_namespace_string` instead.  This string is
     identical.

    """
    return _namespace_string

@pytest.fixture(scope='function')
def _namespace_string2(request):
    """A second dynamically constructed namespace string.

    This is similar to `_namespace_string`, but is guaranteed to be
    different.  Again, this is not managed on its own but is intended
    to be used by higher-level fixtures, and callers are responsible
    for ensuring that the namespace is cleaned up afterwards.

    """
    return make_namespace_string(request.node.name + '2')

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
def task_master(request, _rejester_config, _namespace_string):
    """A rejester TaskMaster for loading or querying work.

    The TaskMaster will have a dynamically created namespace.
    When the test completes the namespace will be torn down.
    If you need the namespace name for other purposes, it will be
    in ``task_master.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = _namespace_string
    tm = TaskMaster(config)
    def fin():
        tm.registry.delete_namespace()
    request.addfinalizer(fin)
    return tm

@pytest.fixture(scope='function')
def task_master2(request, _rejester_config, _namespace_string2):
    """A rejester TaskMaster on a second namespace.

    The TaskMaster will have a dynamically created namespace, distinct
    from task_master's.  When the test completes the namespace will be
    torn down.  If you need the namespace name for other purposes, it
    will be in ``task_master2.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = _namespace_string2
    tm = TaskMaster(config)
    def fin():
        tm.registry.delete_namespace()
    request.addfinalizer(fin)
    return tm

@pytest.fixture(scope='function')
def rejester_queue(request, _rejester_config, _namespace_string):
    """A RejesterQueue that will die at the end of execution.

    The queue will will be named 'queue', and will have a dynamically
    created namespace.  When the test completes the namespace will be
    torn down.  If you need the namespace for other purposes, it will
    be in ``rejester_queue.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = _namespace_string
    q = RejesterQueue(config, 'queue')
    def fin():
        q.delete_namespace()
    request.addfinalizer(fin)
    return q

@pytest.fixture(scope='function')
def rejester_queue2(rejester_queue):
    """A copy of rejester_queue with a distinct worker ID"""
    return RejesterQueue(rejester_queue.config, rejester_queue.name)
