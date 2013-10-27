'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import time
import multiprocessing
from rejester import worker
from rejester._logging import logger
from test_task_master import task_master  ## a fixture that cleans up

def num_seen(target_state, states):
    '''helper function that counts how many workers have seen
    target_state at least once.  "states" is constructed below.
    '''
    return sum([c > 0 for c in states[target_state].values()])

#def work_program(config):
    

def test_task_master_manage_workers(task_master):
    ## 1KB sized work_spec config
    work_spec = dict(
        name = 'tbundle',
        desc = 'a test work bundle',
        min_gb = 8,
        config = dict(many=' ' * 2**10, params=''),
    )

    num_units = 10
    num_workers = 10
    work_units = {str(x): {str(x): ' ' * 30} for x in xrange(num_units)}

    task_master.update_bundle(work_spec, work_units)

    task_master.set_mode(task_master.RUN)

    workers = multiprocessing.Pool(num_workers, maxtasksperchild=1)
    for x in range(num_workers):
        workers.apply_async(
            worker, 
            (task_master.registry.config, work_spec['name']))
    workers.close()
    
    start = time.time()
    max_test_time = 60
    modes = dict()
    finished_cleanly = False
    while time.time() - start < max_test_time:

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
