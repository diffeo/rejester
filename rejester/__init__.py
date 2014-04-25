'''Redis-based distributed work manager.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

Rejester is a distributed work manager, using the `Redis`_ in-memory
database as shared state.  In typical use, some number of systems run
rejester *worker* processes, which query the central database to find
new work; anything that can reach the database can submit jobs.  The
standard rejester work system can also directly deliver work units to
programs without using the dedicated worker process.  For specialized
applications there is also a globally atomic string-based priority
queue.

.. _Redis: http://redis.io/

:command:`rejester` tool
========================

.. automodule:: rejester.run
.. currentmodule:: rejester

Task system
===========

Core API
--------

.. autoclass:: TaskMaster
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: WorkUnit
   :members:
   :undoc-members:
   :show-inheritance:

Workers
-------

.. autoclass:: Worker
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: rejester.workers
.. currentmodule:: rejester

Implementation Details
----------------------

.. autoclass:: Registry
   :members:
   :undoc-members:
   :show-inheritance:

Priority queue system
=====================

.. autoclass:: RejesterQueue
   :members:
   :undoc-members:
   :show-inheritance:

Exceptions
==========

.. automodule:: rejester.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

'''
from __future__ import absolute_import
from rejester.config import config_name, default_config, add_arguments, \
    runtime_keys, check_config
from rejester._task_master import TaskMaster, WorkUnit, Worker
from rejester._queue import RejesterQueue
from rejester._registry import Registry
