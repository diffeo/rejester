'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import time
import uuid
import gevent
import psutil
import random
import logging
import multiprocessing
from signal import signal, SIGHUP, SIGTERM, SIGABRT
from operator import itemgetter
from collections import deque
from rejester._task_master import TaskMaster, Worker, \
    WORKER_OBSERVED_MODE, WORKER_STATE_

logger = logging.getLogger('rejester.workers')

def run_worker(worker_class, *args, **kwargs):
    '''multiprocessing cannot apply_async to a class constructor, even if
    the __init__ calls .run(), so this simple wrapper calls
    worker_class(*args, **kwargs) and logs any exceptions before
    re-raising them.
    '''
    try:
        worker = worker_class(*args, **kwargs)
        worker.register()
        worker.run()
        worker.unregister()
    except Exception, exc:
        logger.critical('worker died!', exc_info=True)
        raise


class HeadlessWorker(Worker):
    '''This expects to receive a WorkUnit from its parent process, which
    is running MultiWorker.
    '''

    def __init__(self, config, worker_id, work_spec_name, work_unit_key):
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
        logger.critical('HeadlessWorker.run')
        self.work_unit.run()

    def terminate(self, sig_num, frame):
        logger.critical('received %d', sig_num)
        self.work_unit.terminate()
        logger.critical('WorkUnit.terminate() complete')
        sys.exit()

class MultiWorker(Worker):
    '''launches one child process per core on the machine, and reports
    available_gb based on what measurements.  This class manages the
    TaskMaster interactions and sends WorkUnit instances to child
    processes.
    '''
    def run(self):
        tm = self.task_master
        num_workers = multiprocessing.cpu_count()
        mem = psutil.phymem_usage()
        available_gb = float(mem.free) / num_workers
        pool = multiprocessing.Pool(num_workers, maxtasksperchild=1)
        ## slots is a fixed-length list of [AsyncRsults, WorkUnit]
        slots = [[None, None]] * num_workers
        logger.critical('MultiWorker starting')
        while 1:
            mode = self.heartbeat()
            logger.info('MultiWorker observed mode=%r', mode)
            for i in xrange(num_workers):
                if slots[i][0]:
                    try:
                        ## raises exceptions from children processes
                        slots[i][0].get(0)
                    except multiprocessing.TimeoutError:
                        ## still in progress
                        slots[i][1].update()
                        continue
                    except Exception, exc:
                        logger.critical('trapped child exception', exc_info=True)
                        slots[i][1].fail(exc)
                    else:
                        ## if it gets here, slot should always be finished
                        assert slots[i][0].ready()
                        slots[i][1].finish()
                    ## either failed or finished
                    assert slots[i][1].failed or slots[i][1].finished
                    slots[i][0] = None
                    slots[i][1] = None
                    
                if slots[i][0] is None and mode == tm.RUN:
                    worker_id = uuid.uuid4().hex
                    work_unit = tm.get_work(worker_id, available_gb=available_gb)
                    logger.info('tm.get_work provided: %r', work_unit)
                    if work_unit is not None:
                        async_result = pool.apply_async(
                            run_worker, 
                            (HeadlessWorker, tm.registry.config, 
                             worker_id, 
                             work_unit.work_spec_name,
                             work_unit.key))
                        slots[i] = [async_result, work_unit]

            if mode == tm.TERMINATE:
                num_waiting = sum(map(int, map(bool, map(itemgetter(0), slots))))
                if num_waiting == 0:
                    logger.info('MultiWorker all children finished')
                    break
                else:
                    logger.info('MultiWorker waiting for %d children to finish', num_waiting)

            time.sleep(random.uniform(1,5))

        logger.info('MultiWorker exiting')
