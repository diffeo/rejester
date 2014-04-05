'''Rejester workers.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2013 Diffeo, Inc.
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
    :func:`~multiprocessing.apply_async` to a class constructor, even
    if the ``__init__`` calls ``.run()``, so this simple wrapper calls
    ``worker_class(*args, **kwargs)`` and logs any exceptions before
    re-raising them.

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
        worker.unregister()


class HeadlessWorker(Worker):
    '''This expects to receive a WorkUnit from its parent process, which
    is running MultiWorker.
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
    '''launches one child process per core on the machine, and reports
    available_gb based on what measurements.  This class manages the
    TaskMaster interactions and sends WorkUnit instances to child
    processes.
    '''
    def __init__(self, config):
        super(MultiWorker, self).__init__(config)
        self._event_queue = multiprocessing.Queue()
        self._mode = None
        self.pool = None

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
        worker_id = uuid.uuid4().hex
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
        tm = self.task_master
        num_workers = multiprocessing.cpu_count()
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
