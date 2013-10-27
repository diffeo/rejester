'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import time
from rejester._logging import logger
from rejester._task_master import TaskMaster

def worker(config, work_spec_name):
    try:
        logger.critical('worker starting')
        task_master = TaskMaster(config)
        work_unit = None
        while 1:
            mode = task_master.get_mode(work_spec_name)
            logger.critical('worker observed mode=%r', mode)
            
            task_master.registry.increment('observed_modes_' + mode, str(os.getpid()))

            if mode in [task_master.IDLE, task_master.TERMINATE]:
                if work_unit:
                    work_unit.update(lease_time=-10)
                    work_unit = None

            if mode == task_master.TERMINATE:
                break

            if mode == task_master.RUN:
                if not work_unit:
                    work_unit = task_master.get_work(work_spec_name)
                work_unit.update()

            time.sleep(1)

    except Exception, exc:
        logger.critical('worker failed!', exc_info=True)
        sys.exit(-1)

