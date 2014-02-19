"""py.test support for rejester

Purpose
=======

This module provides common py.test fixtures for various rejester
features.  They will clean up after themselves, taking care to not, for
instance, leak namespaces in the underlying redis database.


Implementation Notes
====================

You will need to tell pytest to load this module.  You can add to
either your test module or a shared ``conftest.py`` file

>>> pytest_plugins = 'rejester.tests.fixtures'

Your tests will then have fixtures like ``task_master`` and
``rejester_queue`` available to them, with automatic namespace
management.  Your test directory should also have a file named
``config_registry.yaml`` that looks like

    registry_addresses: ['redis.example.com:6379']

You can usually get the namespace string from the configuration of
one of the high-level objects.  If you must manually work with the
namespace string outside of the managed objects, you can call
``make_namespace_string()`` or depend on the ``namespace_string``
fixture.

-----

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.

"""

import getpass
import hashlib
import logging
import os
import re
import socket

import pytest
import yaml

from rejester import RejesterQueue, TaskMaster

def _then_delete_namespace(request, obj, registry=None):
    """Helper to delete a namespace on cleanup.

    :param request: py.test request fixture
    :param obj: object to return
    :param registry: object to call ``delete_namespace()``, use
      `obj` if `None`
    :return: obj

    """
    if registry is None: registry = obj
    def fin():
        # Avoid a spurious "no handlers could be found for logger"
        # complaint.  delete_namespace() has a (useful) debug log
        # statement; but when we get here, under the py.test
        # capturelog plugin, the root logger has already been
        # removed.
        handler = logging.NullHandler()
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        registry.delete_namespace()
        root_logger.removeHandler(handler)
    request.addfinalizer(fin)
    return obj

@pytest.fixture(scope='function')
def namespace_string2(request):
    """A second dynamically constructed namespace string.

    This is similar to `namespace_string`, but is guaranteed to be
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
def task_master(request, _rejester_config, namespace_string):
    """A rejester TaskMaster for loading or querying work.

    The TaskMaster will have a dynamically created namespace.
    When the test completes the namespace will be torn down.
    If you need the namespace name for other purposes, it will be
    in ``task_master.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = namespace_string
    tm = TaskMaster(config)
    return _then_delete_namespace(request, tm, tm.registry)

@pytest.fixture(scope='function')
def task_master2(request, _rejester_config, namespace_string2):
    """A rejester TaskMaster on a second namespace.

    The TaskMaster will have a dynamically created namespace, distinct
    from task_master's.  When the test completes the namespace will be
    torn down.  If you need the namespace name for other purposes, it
    will be in ``task_master2.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = namespace_string2
    tm = TaskMaster(config)
    return _then_delete_namespace(request, tm, tm.registry)

@pytest.fixture(scope='function')
def rejester_queue(request, _rejester_config, namespace_string):
    """A RejesterQueue that will die at the end of execution.

    The queue will will be named 'queue', and will have a dynamically
    created namespace.  When the test completes the namespace will be
    torn down.  If you need the namespace for other purposes, it will
    be in ``rejester_queue.config['namespace']``.

    """
    config = dict(_rejester_config)
    config['namespace'] = namespace_string
    q = RejesterQueue(config, 'queue')
    return _then_delete_namespace(request, q)

@pytest.fixture(scope='function')
def rejester_queue2(rejester_queue):
    """A copy of rejester_queue with a distinct worker ID"""
    return RejesterQueue(rejester_queue.config, rejester_queue.name)
