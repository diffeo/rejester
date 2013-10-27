'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import time
import gevent
import random
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
