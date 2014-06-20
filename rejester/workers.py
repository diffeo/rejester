'''Rejester workers.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

The standard worker infrastructure in the classes below calls
:meth:`rejester.WorkUnit.run` on individual work units as they become
available.  In normal use, a caller will use
:meth:`rejester.TaskMaster.update_bundle` to submit jobs, then expect
an external caller to run ``rejester run_worker``, which will create a
:class:`MultiWorker` object that runs those jobs.

Other implementation strategies are definitely possible.  The
:class:`SingleWorker` class here will run exactly one job when
invoked.  It is also possible for a program that intends to do some
work, possibly even in parallel, but wants to depend on rejester for
queueing, to call :meth:`rejester.TaskMaster.get_work` itself and do
work based on whatever information is in the work spec; that would not
use this worker infrastructure at all.

.. autoclass:: SingleWorker
    :members:
    :show-inheritance:

.. autoclass:: MultiWorker
    :members:
    :show-inheritance:

.. autoclass:: HeadlessWorker
    :members:
    :show-inheritance:

.. autofunction:: run_worker

'''
from __future__ import absolute_import
import os
import sys
import time
import uuid
import logging
import gevent
import psutil
import random
import Queue
import multiprocessing
import threading
from signal import signal, SIGHUP, SIGTERM, SIGABRT
from operator import itemgetter
from collections import deque

from rejester import TaskMaster, Worker
from rejester._task_master import WORKER_OBSERVED_MODE, WORKER_STATE_
from rejester._registry import nice_identifier

logger = logging.getLogger(__name__)

def test_work_program(work_unit):
    ## just to show that this works, we get the config from the data
    ## and *reconnect* to the registry with a second instances instead
    ## of using work_unit.registry
    config = work_unit.data['config']
    sleeptime = float(work_unit.data.get('sleep', 9.0))
    task_master = TaskMaster(config)
    logger.info('executing work_unit %r ... %s', work_unit.key, sleeptime)
    time.sleep(sleeptime)  # pretend to work
    logger.info('finished %r' % work_unit)

def run_worker(worker_class, *args, **kwargs):
    '''Bridge function to run a worker under :mod:`multiprocessing`.

    The :mod:`multiprocessing` module cannot
    :meth:`~multiprocessing.Pool.apply_async` to a class constructor,
    even if the ``__init__`` calls ``.run()``, so this simple wrapper
    calls ``worker_class(*args, **kwargs)`` and logs any exceptions
    before re-raising them.

    This is usually only used to create a :class:`HeadlessWorker`, but
    it does run through the complete
    :meth:`~rejester.Worker.register`, :meth:`~rejester.Worker.run`,
    :meth:`~rejester.Worker.unregister` sequence with some logging
    on worker-level failures.

    '''
    try:
        worker = worker_class(*args, **kwargs)
    except Exception, exc:
        logger.critical('failed to create worker {!r}'.format(worker_class),
                        exc_info=True)
        raise
    try:
        worker.register()
        worker.run()
    except Exception, exc:
        logger.error('worker {!r} died'.format(worker_class), exc_info=True)
        raise
    finally:
        logger.debug('preparing to worker.unregister() for %r', worker)
        worker.unregister()


class HeadlessWorker(Worker):
    '''Child worker to do work under :mod:`multiprocessing`.

    The :meth:`run` method expects to run a single
    :class:`~rejester.WorkUnit`, which it will receive from its
    parent :class:`MultiWorker`.  This class expects to be the only
    thing run in a :mod:`multiprocessing` child process.

    '''

    def __init__(self, config, worker_id, work_spec_name, work_unit_key):
        # Do a complete reset of logging right now before we do anything else.
        #
        # multiprocessing.Pool is super super asynchronous: when you
        # apply_async() your job description goes into a queue, which
        # a thread moves to another queue, a second thread tries every
        # 0.1s to make sure the subprocesses exist, and the subprocess
        # actually pulls the job off the queue.  If your main thread is
        # doing something when that 0.1s timer fires, it's possible that
        # the os.fork()ed child is actually forked holding some lock from
        # the parent.
        #
        # See:  http://bugs.python.org/issue6721
        #
        # logging seems to be the most prominent thing that causes
        # trouble here.  There is both a global logging._lock,
        # plus every logging.Handler instance has a lock.  If we just
        # clean up these locks we'll be good.
        logging._lock = threading.RLock()
        for handler in logging._handlers.itervalues():
            handler.createLock()

        # Now go on as normal
        super(HeadlessWorker, self).__init__(config)
        for sig_num in [SIGTERM, SIGHUP, SIGABRT]:
            signal(sig_num, self.terminate)
        self.work_unit = self.task_master.get_assigned_work_unit(
            worker_id, work_spec_name, work_unit_key)
        ## carry this to overwrite self.worker_id after .register()
        self._pre_assigned_worker_id = worker_id

    def register(self):
        super(HeadlessWorker, self).register()
        self.worker_id = self._pre_assigned_worker_id

    def run(self):
        self.work_unit.run()

    def terminate(self, sig_num, frame):
        logger.info('received %d, ending work unit', sig_num)
        self.work_unit.terminate()
        sys.exit()

class MultiWorker(Worker):
    '''Parent worker that runs multiple jobs continuously.
    
    This uses :mod:`multiprocessing` to run one child
    :class:`HeadlessWorker` per core on the system, and averages
    system memory to report `available_gb`.  This class manages the
    :class:`~rejester.TaskMaster` interactions and sends
    :class:`~rejester.WorkUnit` instances to its managed child
    processes.

    This class is normally invoked from the command line by
    running ``rejester run_worker``, which runs this class as a
    daemon process.

    Instances of this class, running across many machines in a
    cluster, are controlled by :meth:`rejester.TaskMaster.get_mode`.
    The :meth:`run` method will exit if the current mode is
    :attr:`~rejester.TaskMaster.TERMINATE`.  If the mode is
    :attr:`~rejester.TaskMaster.IDLE` then the worker will stay
    running but will not start new jobs.  New jobs will be started
    only when the mode becomes :attr:`~rejester.TaskMaster.RUN`.  The
    system defaults to :attr:`~rejester.TaskMaster.IDLE` state, but if
    workers exit immediately, it may be because the mode has been left
    at :attr:`~rejester.TaskMaster.TERMINATE` from a previous
    execution.

    If `tasks_per_cpu` is set in the configuration block for rejester,
    then that many child process will be launched for each CPU on the
    machine.

    '''
    def __init__(self, config):
        super(MultiWorker, self).__init__(config)
        self._event_queue = multiprocessing.Queue()
        self._mode = None
        self.pool = None
        logger.debug('MultiWorker initialized')

    _available_gb = None

    @classmethod
    def available_gb(cls):
        if cls._available_gb is None:
            mem = psutil.phymem_usage()
            cls._available_gb = float(mem.free) / multiprocessing.cpu_count()
        return cls._available_gb

    def _finish_callback(self, *args):
        # We don't actually get anything useful from the work call, so
        # just post an event that causes us to wake up and poll all
        # the slots.
        self._event_queue.put(True)

    def _poll_async_result(self, async_result, work_unit, do_update=True):
        if async_result is None:
            return
        assert work_unit is not None
        if not async_result.ready():
            if do_update:
                logger.debug('not ready %r, update', work_unit.key)
                work_unit.update()
            return
        try:
            async_result.get(0)
        except multiprocessing.TimeoutError:
            if do_update:
                logger.debug('get timeout update %r', work_unit.key)
                work_unit.update()
            return
        except Exception, exc:
            logger.error('trapped child exception', exc_info=True)
            work_unit.fail(exc)
        else:
            ## if it gets here, slot should always be finished
            assert async_result.ready()
            work_unit.finish()
        ## either failed or finished
        assert work_unit.failed or work_unit.finished

    def _get_and_start_work(self):
        "return (async_result, work_unit) or (None, None)"
        worker_id = nice_identifier()
        work_unit = self.task_master.get_work(worker_id, available_gb=self.available_gb())
        if work_unit is None:
            return None, None
        async_result = self.pool.apply_async(
            run_worker,
            (HeadlessWorker, self.task_master.registry.config,
             worker_id,
             work_unit.work_spec_name,
             work_unit.key),
            callback=self._finish_callback)
        return async_result, work_unit

    def _poll_slots(self, slots, mode=None, do_update=False):
        hasWork = True
        for i in xrange(len(slots)):
            async_result, work_unit = slots[i]
            if (async_result is not None) and (work_unit is not None):
                self._poll_async_result(async_result, work_unit, do_update=do_update)
                if work_unit.failed or work_unit.finished:
                    slots[i] = (None, None)
            if (slots[i][0] is None) and (mode == self.task_master.RUN):
                if hasWork:
                    slots[i] = self._get_and_start_work()
                    if slots[i][0] is None:
                        # If we fail to get work, don't hammer the
                        # taskmaster with requests for work. Wait
                        # until after a sleep and the next _poll_slots
                        # cycle.
                        hasWork = False

    def run(self):
        '''Fetch and dispatch jobs as long as the system is running.

        This periodically checks the :class:`rejester.TaskMaster` mode
        and asks it for more work.  It will normally run forever in a
        loop until the mode becomes
        :attr:`~rejester.TaskMaster.TERMINATE`, at which point it
        waits for all outstanding jobs to finish and exits.

        This will :func:`~rejester.Worker.heartbeat` and check for new
        work whenever a job finishes, or otherwise on a random
        interval between 1 and 5 seconds.

        '''

        tm = self.task_master
        num_workers = multiprocessing.cpu_count()
        if 'tasks_per_cpu' in self.config:
            num_workers *= self.config.get('tasks_per_cpu') or 1
        if self.pool is None:
            self.pool = multiprocessing.Pool(num_workers, maxtasksperchild=1)
        ## slots is a fixed-length list of [AsyncRsults, WorkUnit]
        slots = [[None, None]] * num_workers
        logger.info('MultiWorker starting with %s workers', num_workers)
        min_loop_time = 2.0
        lastFullPoll = time.time()
        while True:
            mode = self.heartbeat()
            if mode != self._mode:
                logger.info('worker {} changed to mode {}'
                            .format(self.worker_id, mode))
                self._mode = mode
            now = time.time()
            should_update = (now - lastFullPoll) > min_loop_time
            self._poll_slots(slots, mode=mode, do_update=should_update)
            if should_update:
                lastFullPoll = now

            if mode == tm.TERMINATE:
                num_waiting = sum(map(int, map(bool, map(itemgetter(0), slots))))
                if num_waiting == 0:
                    logger.info('MultiWorker all children finished')
                    break
                else:
                    logger.info('MultiWorker waiting for %d children to finish', num_waiting)

            sleepsecs = random.uniform(1,5)
            sleepstart = time.time()
            try:
                self._event_queue.get(block=True, timeout=sleepsecs)
                logger.debug('woken by event looptime=%s sleeptime=%s', sleepstart - now, time.time() - sleepstart)
            except Queue.Empty, empty:
                logger.debug('queue timed out. be exhausting, looptime=%s sleeptime=%s', sleepstart - now, time.time() - sleepstart)
                # it's cool, timed out, do the loop of checks and stuff.

        logger.info('MultiWorker exiting')

class SingleWorker(Worker):
    '''Worker that runs exactly one job when called.'''
    def run(self):
        '''Get exactly one job, run it, and return.

        Does nothing (but returns :const:`False`) if there is no work
        to do.  Ignores the global mode; this will do work even
        if :func:`rejester.TaskMaster.get_mode` returns
        :attr:`~rejester.TaskMaster.TERMINATE`.

        :return: :const:`True` if there was a job (even if it failed)

        '''
        available_gb = MultiWorker.available_gb()
        unit = self.task_master.get_work(self.worker_id, available_gb)
        if unit is None: return False
        try:
            unit.run()
            unit.finish()
        except Exception, e:
            unit.fail(e)
        return True
