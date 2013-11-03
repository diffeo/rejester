'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import abc
import time
import uuid
import psutil
import gevent
import random
import socket
import pkg_resources
import multiprocessing
from signal import signal, SIGHUP, SIGTERM, SIGABRT
from operator import itemgetter
from collections import deque
from rejester._logging import logger
from rejester._task_master import TaskMaster, WORKER_OBSERVED_MODE, WORKER_STATE_


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


class Worker(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self.config = config
        self.task_master = TaskMaster(config)
        self.worker_id = None
        self.lifetime = 300 ## five minutes

    def environment(self):
        '''
        raw data about worker to support forensics on failed tasks
        '''
        env = dict(
            worker_id = self.worker_id,
            host = socket.gethostbyaddr(socket.gethostname()),
            fqdn = socket.getfqdn(),
            version = pkg_resources.get_distribution("rejester").version, # pylint: disable=E1103
            working_set = [(dist.key, dist.version) for dist in pkg_resources.WorkingSet()], # pylint: disable=E1103
            #config_hash = self.config['config_hash'],
            #config_json = self.config['config_json'],
            memory = psutil.phymem_usage(),
        )
        return env

    def register(self):
        '''record the availability of this worker and get a unique identifer
        '''
        if self.worker_id:
            raise ProgrammerError('Worker.register cannot be called again without first calling unregister; it is not idempotent')
        self.worker_id = uuid.uuid4().hex
        self.heartbeat()
        return self.worker_id

    def unregister(self):
        '''remove this worker from the list of available workers
        ''' 
        with self.task_master.registry.lock() as session:
            session.delete(WORKER_STATE_ + self.worker_id)
            session.popmany(WORKER_OBSERVED_MODE, self.worker_id)
        self.worker_id = None

    def heartbeat(self):
        '''record the current worker state in the registry and also the
        observed mode and return it

        :returns mode:
        ''' 
        mode = self.task_master.get_mode()
        with self.task_master.registry.lock() as session:
            session.set(WORKER_OBSERVED_MODE, self.worker_id, mode,
                        priority=time.time() + self.lifetime)
            session.update(WORKER_STATE_ + self.worker_id, self.environment(), 
                           expire=self.lifetime)
        logger.info('worker observed mode=%r', mode)
        return mode

    @abc.abstractmethod
    def run(self):
        return



class BlockingWorker(Worker):
    '''waits for the rejester to transition to RUN, obtains a WorkUnit and
    executes it until the rejester transitions to IDLE or TERMINATE.
    Blocks on calls to WorkUnit.{execute,shutdown}
    '''

    def __init__(self, config, available_gb):
        super(BlockingWorker, self).__init__(config)
        self.available_gb = available_gb
        self.work_unit = None

    def run(self):
        logger.critical('worker starting')
        while 1:
            mode = self.heartbeat()

            if mode in [self.task_master.IDLE, self.task_master.TERMINATE] and self.work_unit:
                self.work_unit.terminate()

            if mode == self.task_master.TERMINATE:
                break

            if mode == self.task_master.RUN:
                if not self.work_unit:
                    self.work_unit = self.task_master.get_work(available_gb=self.available_gb)
                if  self.work_unit:
                    ## this call will block the worker, so that it
                    ## fails to call heartbeat as often as it should
                    self.work_unit.run()
                else:
                    time.sleep(1)


class GreenletWorker(Worker):
    '''Similar to BlockingWorker but uses Greenlet co-routines to allow
    the execute function to periodically yield to the update function
    to maintain the lease.
    '''
    def __init__(self, config, available_gb):
        super(BlockingWorker, self).__init__(config)
        self.available_gb = available_gb
        self.work_unit = None
        self.greenlet = None

    def run(self):
        logger.critical('worker starting')
        while 1:
            mode = self.task_master.get_mode()
            logger.info('worker observed mode=%r', mode)

            if mode == self.task_master.IDLE:
                if  self.work_unit:
                    self.work_unit.idle()

            if mode == self.task_master.TERMINATE:
                if  self.work_unit:
                    self.work_unit.terminate()
                if  self.greenlet:
                    self.greenlet.kill(block=False)
                    self.greenlet.join()
                break

            if mode == self.task_master.RUN:
                if not self.work_unit:
                    self.work_unit = self.task_master.get_work(
                        available_gb=self.available_gb)
                if self.work_unit and not self.greenlet:
                    self.greenlet = gevent.spawn(self.work_unit.run)

            gevent.sleep(random.uniform(1,5))


class HeadlessWorker(Worker):
    '''Unlike BlockingWorker and GreenletWorker, this expects to receive a
    WorkUnit from its parent process, which is running MultiWorker
    '''

    def __init__(self, config, work_spec_name, work_unit_key):
        super(HeadlessWorker, self).__init__(config)
        for sig_num in [SIGTERM, SIGHUP, SIGABRT]:
            signal(sig_num, self.terminate)
        self.work_unit = self.task_master.get_work_unit(work_spec_name, work_unit_key)

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
        tm = TaskMaster(self.config)
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
                    work_unit = tm.get_work(available_gb=available_gb)
                    logger.info('tm.get_work provided: %r' % work_unit)
                    async_result = pool.apply_async(
                        run_worker, 
                        (HeadlessWorker, tm.registry.config, 
                         work_unit.work_spec_name, work_unit.key))
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
