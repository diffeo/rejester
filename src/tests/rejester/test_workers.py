'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import copy
import time
import rejester
import multiprocessing
from rejester.workers import run_worker, BlockingWorker, GreenletWorker
from rejester._logging import logger

from tests.rejester.test_task_master import task_master  ## a fixture that cleans up
from tests.rejester.make_namespace_string import make_namespace_string

def num_seen(target_state, states):
    '''helper function that counts how many workers have seen
    target_state at least once.  "states" is constructed below.
    '''
    return sum([c > 0 for c in states[target_state].values()])


def work_program(work_unit):
    logger.critical('executing work_unit')

    ## just to show that this works, we get the config from the data
    ## and *reconnect* to the registry with a second instances instead
    ## of using work_unit.registry
    config = work_unit.data['config']
    task_master = rejester.TaskMaster(config)
    mode = task_master.get_mode()

    #config2 = copy.deepcopy(config)
    #config2['namespace'] = config['second_namespace']
    #registry = rejester.Registry(config2)
    
    task_master.registry.increment('observed_modes_' + mode, str(os.getpid()))
    

def test_task_master_manage_workers(task_master):
    ## 1KB sized work_spec config
    work_spec = dict(
        name = 'tbundle',
        desc = 'a test work bundle',
        min_gb = 8,
        config = dict(many=' ' * 2**10, params=''),
        module = 'tests.rejester.test_workers',
        exec_function = 'work_program',
        shutdown_function = 'work_program',
    )

    num_units = 10
    num_workers = 10
    work_units = {str(x): dict(config=task_master.registry.config) for x in xrange(num_units)}

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
    modes = dict()
    finished_cleanly = False
    while time.time() - start < max_test_time:

        for res in results:
            try:
                ## raises exceptions from children processes
                res.get(0)
            except multiprocessing.TimeoutError:
                pass

        for mode in [task_master.TERMINATE, task_master.IDLE, task_master.RUN]:
            modes[mode] = task_master.registry.pull('observed_modes_' + mode)
            assert all([isinstance(c, (int, float)) for c in modes[mode].values()])

        logger.critical({s: num_seen(s, modes) for s in modes})

        if num_seen(task_master.RUN, modes) == num_workers:
            task_master.idle_all_workers()

        if num_seen(task_master.IDLE, modes) == num_workers:
            assert num_seen(task_master.RUN, modes) == num_workers
            logger.critical('setting mode to TERMINATE')
            task_master.set_mode(task_master.TERMINATE)
        
        if num_seen(task_master.TERMINATE, modes) == num_workers:
            assert num_seen(task_master.IDLE, modes) == num_workers
            finished_cleanly = True
            break

    if not finished_cleanly:
        raise Exception('timed out after %d seconds' % (time.time() - start))

    workers.join()
    logger.info('finished running %d worker processes' % num_workers)
