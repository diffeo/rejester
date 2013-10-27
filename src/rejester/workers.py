'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import time
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

class Worker(object):
    def __init__(self, config, available_gb):
        self.config = config
        self.available_gb = available_gb
        self.task_master = TaskMaster(config)
        self.work_unit = None

    def run(self):
        logger.critical('worker starting')
        while 1:
            mode = self.task_master.get_mode()
            logger.critical('worker observed mode=%r', mode)
            
            self.task_master.registry.increment(
                'observed_modes_' + mode, str(os.getpid()))

            if mode in [self.task_master.IDLE, self.task_master.TERMINATE]:
                if self.work_unit:
                    self.work_unit.update(lease_time=-10)
                    self.work_unit = None

            if mode == self.task_master.TERMINATE:
                break

            if mode == self.task_master.RUN:
                if not self.work_unit:
                    self.work_unit = self.task_master.get_work(
                        available_gb=self.available_gb)
                self.work_unit.update()

            time.sleep(1)

