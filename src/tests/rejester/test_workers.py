'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import copy
import time
import rejester
import multiprocessing
from rejester.workers import run_worker, BlockingWorker, GreenletWorker, MultiWorker
from rejester._logging import logger

from tests.rejester.test_task_master import task_master  ## a fixture that cleans up


def test_task_register(task_master):
    worker = BlockingWorker(task_master.registry.config, 10)
    worker_id = worker.register()
    assert worker_id in task_master.workers()
    worker.unregister()
    assert worker_id not in task_master.workers()


def work_program(work_unit):
    logger.critical('executing work_unit')

    ## just to show that this works, we get the config from the data
    ## and *reconnect* to the registry with a second instances instead
    ## of using work_unit.registry
    config = work_unit.data['config']
    task_master = rejester.TaskMaster(config)
    time.sleep(3)

def work_program_broken(work_unit):
    logger.critical('executing "broken" work_unit')

    ## just to show that this works, we get the config from the data
    ## and *reconnect* to the registry with a second instances instead
    ## of using work_unit.registry
    config = work_unit.data['config']
    task_master = rejester.TaskMaster(config)
    raise Exception('simulate broken work_unit')

## 1KB sized work_spec config
work_spec = dict(
    name = 'tbundle',
    desc = 'a test work bundle',
    min_gb = 8,
    config = dict(many=' ' * 2**10, params=''),
    module = 'tests.rejester.test_workers',
    run_function = 'work_program',
    terminate_function = 'work_program',
)

def test_task_master_manage_workers(task_master):
    num_units = 10
    num_workers = 10
    work_units = {str(x): dict(config=task_master.registry.config) 
                  for x in xrange(num_units)}

    task_master.update_bundle(work_spec, work_units)

    task_master.set_mode(task_master.RUN)

    workers = multiprocessing.Pool(num_workers, maxtasksperchild=1)
    results = []
    for x in range(num_workers):
        results.append(
            ## could use GreenletWorker here, just slower.  Both risk
            ## being blocked by a non-cooperative work_program...
            workers.apply_async(
                run_worker, (BlockingWorker, task_master.registry.config, 9)))
        ## "9" is the available_gb hard coded for this test
    workers.close()
    
    start = time.time()
    max_test_time = 60
    already_set_idle = False
    already_set_terminate = False
    while results and ((time.time() - start) < max_test_time):

        for res in results:
            try:
                ## raises exceptions from children processes
                res.get(0)
            except multiprocessing.TimeoutError:
                results.remove(res)

        modes = task_master.mode_counts()
        logger.critical(modes)

        if modes[task_master.RUN] == num_workers:
            task_master.idle_all_workers()
            already_set_idle = True

        if modes[task_master.IDLE] == num_workers:
            assert already_set_idle
            logger.critical('setting mode to TERMINATE')
            task_master.set_mode(task_master.TERMINATE)
            already_set_terminate = True

        time.sleep(1)

    if not (already_set_terminate and already_set_idle):
        raise Exception('timed out after %d seconds' % (time.time() - start))

    workers.join()
    logger.info('finished running %d worker processes' % num_workers)


def test_task_master_multi_worker(task_master):
    num_units = 10
    num_units_cursor = 0
    work_units = {'key' + str(x): dict(config=task_master.registry.config) 
                  for x in xrange(num_units_cursor, num_units_cursor + num_units)}
    num_units_cursor += num_units
    task_master.update_bundle(work_spec, work_units)

    task_master.set_mode(task_master.RUN)

    p = multiprocessing.Process(target=run_worker, 
                                args=(MultiWorker, task_master.registry.config))
    num_workers = multiprocessing.cpu_count()
    logger.critical('expecting num_workers=%d', num_workers)

    start = time.time()
    max_test_time = 60
    finished_cleanly = False
    already_set_idle = False
    already_set_terminate = False
    p.start()

    while time.time() - start < max_test_time:

        modes = task_master.mode_counts()
        logger.critical(modes)

        if modes[task_master.RUN] >= num_workers:
            task_master.idle_all_workers()
            already_set_idle = True

        if modes[task_master.IDLE] >= 0 and already_set_idle:
            logger.critical('setting mode to TERMINATE')
            task_master.set_mode(task_master.TERMINATE)
            already_set_terminate = True
        
        if p.exitcode is not None:
            assert already_set_idle
            assert already_set_terminate
            if p.exitcode == 0:
                finished_cleanly = True
                break

        time.sleep(2)

    if not finished_cleanly:
        raise Exception('timed out after %d seconds' % (time.time() - start))

    p.join()
    logger.info('finished running %d worker processes', num_workers)

    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) >= num_workers


def test_task_master_multi_worker_failed_task(task_master):
    work_spec_broken = copy.deepcopy(work_spec)
    work_spec_broken['run_function'] = 'work_program_broken'

    num_units = 10
    num_units_cursor = 0
    work_units = {'key' + str(x): dict(config=task_master.registry.config) 
                  for x in xrange(num_units_cursor, num_units_cursor + num_units)}
    num_units_cursor += num_units
    task_master.update_bundle(work_spec_broken, work_units)

    task_master.set_mode(task_master.RUN)

    p = multiprocessing.Process(target=run_worker, 
                                args=(MultiWorker, task_master.registry.config))
    num_workers = multiprocessing.cpu_count()
    logger.critical('expecting num_workers=%d', num_workers)

    start = time.time()
    max_test_time = 60
    finished_cleanly = False
    already_set_idle = False
    already_set_terminate = False
    p.start()

    while time.time() - start < max_test_time:

        modes = task_master.mode_counts()
        logger.critical(modes)

        if modes[task_master.RUN] >= num_workers:
            task_master.idle_all_workers()
            already_set_idle = True

        if modes[task_master.IDLE] >= 0 and already_set_idle:
            logger.critical('setting mode to TERMINATE')
            task_master.set_mode(task_master.TERMINATE)
            already_set_terminate = True
        
        if p.exitcode is not None:
            assert already_set_idle
            assert already_set_terminate
            if p.exitcode == 0:
                finished_cleanly = True
                break

        time.sleep(2)

    if not finished_cleanly:
        raise Exception('timed out after %d seconds' % (time.time() - start))

    p.join()
    logger.info('finished running %d worker processes', num_workers)

    assert task_master.num_failed(work_spec['name']) >= num_workers
    assert task_master.num_finished(work_spec['name']) == 0
