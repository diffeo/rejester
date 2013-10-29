'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import time
import gevent
import random
from signal import signal, SIGHUP, SIGTERM, SIGKILL, SIGABRT
from collections import deque
from rejester._logging import logger
from rejester._task_master import TaskMaster

def run_worker(worker_class, *args, **kwargs):
    '''multiprocessing cannot apply_async to a class constructor, even if
    the __init__ calls .run(), so this simple wrapper calls
    worker_class(*args, **kwargs) and logs any exceptions before
    re-raising them.
    '''
    try:
        worker = worker_class(*args, **kwargs)
        worker.run()
    except Exception, exc:
        logger.critical('worker died!', exc_info=True)
        raise

class BlockingWorker(object):
    '''waits for the rejester to transition to RUN, obtains a WorkUnit and
    executes it until the rejester transitions to IDLE or TERMINATE.
    Blocks on calls to WorkUnit.{execute,shutdown}
    '''

    def __init__(self, config, available_gb):
        self.config = config
        self.available_gb = available_gb
        self.task_master = TaskMaster(config)
        self.work_unit = None

    def run(self):
        logger.critical('worker starting')
        while 1:
            mode = self.task_master.get_mode()
            logger.info('worker observed mode=%r', mode)

            if mode in [self.task_master.IDLE, self.task_master.TERMINATE]:
                if self.work_unit:
                    self.work_unit.shutdown()

            if mode == self.task_master.TERMINATE:
                break

            if mode == self.task_master.RUN:
                if not self.work_unit:
                    self.work_unit = self.task_master.get_work(
                        available_gb=self.available_gb)
                if self.work_unit:
                    self.work_unit.execute()

            time.sleep(1)


class GreenletWorker(object):
    '''Similar to BlockingWorker but uses Greenlet co-routines to allow
    the execute function to periodically yield to the update function
    to maintain the lease.
    '''
    def __init__(self, config, available_gb):
        self.config = config
        self.available_gb = available_gb
        self.task_master = TaskMaster(config)
        self.work_unit = None
        self.greenlet = None

    def run(self):
        logger.critical('worker starting')
        while 1:
            mode = self.task_master.get_mode()
            logger.info('worker observed mode=%r', mode)

            if mode in [self.task_master.IDLE, self.task_master.TERMINATE]:
                if  self.work_unit:
                    self.work_unit.shutdown()

            if mode == self.task_master.TERMINATE:
                if  self.greenlet:
                    self.greenlet.kill(block=False)
                    self.greenlet.join()
                break

            if mode == self.task_master.RUN:
                if not self.work_unit:
                    self.work_unit = self.task_master.get_work(
                        available_gb=self.available_gb)
                if self.work_unit and not self.greenlet:
                    self.greenlet = gevent.spawn(self.work_unit.execute)

            gevent.sleep(random.uniform(1,5))


class HeadlessWorker(object):
    '''Unlike BlockingWorker and GreenletWorker, this expects to receive a
    WorkUnit from its parent process, which is running MultiWorker
    '''
    def __init__(self, config, work_unit):
        self.config = config
        self.task_master = TaskMaster(config)
        self.work_unit = work_unit
        for sig_num in [SIGTERM, SIGHUP, SIGKILL, sigABRT]:
            signal(self.shutdown, sig_num)

    def shutdown(self, sig_num, frame):
        logger.critical('received %d --> WorkUnit.shutdown()', sig_num)
        self.work_unit.shutdown()

    def run(self):
        logger.critical('HeadlessWorker.run')
        self.work_unit.execute()


class MultiWorker(object):
    '''launches one child process per core on the machine, and reports
    available_gb based on what measurements.  This class manages the
    TaskMaster interactions and sends WorkUnit instances to child
    processes.
    '''
    def __init__(self, config):
        self.config = config

    def run(self):
        tm = TaskMaster(config)
        num_workers = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(num_workers, maxtasksperchild=1)
        ## slots is a fixed-length list of [AsyncRsults, WorkUnit]
        slots = [[None, None]] * num_workers
        logger.critical('MultiWorker starting')
        while 1:
            mode = tm.get_mode()
            logger.info('MultiWorker observed mode=%r', mode)
            for i in xrange(num_workers):
                if slots[i][0]:
                    try:
                        ## raises exceptions from children processes
                        slots[i][0].get(0)
                    except multiprocessing.TimeoutError:
                        ## still in progress
                        slots[i][0].update()
                        continue
                    except Exception, exc:
                        logger.critical('trapped exception', exc_info=True)
                        slots[i][1].fail()
                    else:
                        ## clear the slot if finished
                        if  slots[i][0].ready():
                            slots[i][1].finish()
                    ## either failed or finished
                    slots[i][0] = None
                    slots[i][1] = None
                    
                if slots[i][0] is None and mode == tm.RUN:
                    work_unit = tm.get_work(available_gb=available_gb)
                    async_result = workers.apply_async(
                        run_worker, 
                        (HeadlessWorker, tm.registry.config, work_unit))

            if mode == tm.TERMINATE and not any(self.slots):
                break

            
            time.sleep(random.uniform(1,5))
