'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import time
import Queue
import multiprocessing
from rejester import TaskMaster
from collections import defaultdict, Counter
from rejester._logging import logger
from test_task_master import task_master  ## a fixture that cleans up

def worker(config, work_spec_name, observed_states_queue):
    try:
        logger.critical('worker starting')
        task_master = TaskMaster(config)
        work_unit = None
        while 1:
            mode = task_master.get_mode(work_spec_name)
            logger.critical('worker observed mode=%r', mode)
            observed_states_queue.put((os.getpid(), mode))

            if mode == task_master.IDLE:
                work_unit.update(lease_time=-10)

            if mode == task_master.TERMINATE:
                work_unit.update(lease_time=-10)
                break

            if mode == task_master.RUN_FOREVER:
                if not work_unit:
                    work_unit = task_master.get_work(work_spec_name)
                work_unit.update()

            time.sleep(1)
    except Exception, exc:
        logger.critical('worker failed!', exc_info=True)
        sys.exit(-1)


def num_seen(target_state, states):
    '''helper function that counts how many workers have seen
    target_state at least once.  "states" is constructed below.
    '''
    return sum([c > 0 for c in states[target_state].values()])


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

    task_master.set_mode(work_spec['name'], task_master.RUN_FOREVER)

    manager = multiprocessing.Manager()
    observed_states_queue = manager.Queue() # pylint: disable=E1101
    workers = multiprocessing.Pool(num_workers, maxtasksperchild=1)
    for x in range(num_workers):
        workers.apply_async(
            worker, 
            (task_master.registry.config, work_spec['name'], observed_states_queue))
    workers.close()
    
    start = time.time()
    max_test_time = 60
    states = defaultdict(Counter)
    state = None
    while 1:
        try:
            pid, state = observed_states_queue.get(timeout=1)
        except Queue.Empty:
            logger.critical('no messages after %d seconds', time.time() - start)
        if time.time() - start > max_test_time:
            raise Exception('no messages after %d seconds' % (time.time() - start))

        logger.critical({s: num_seen(s, states) for s in states})

        if not state:
            continue

        states[state][pid] += 1

        if num_seen(task_master.RUN_FOREVER, states) == num_workers:
            task_master.idle_all_workers(work_spec['name'])

        if num_seen(task_master.IDLE, states) == num_workers:
            assert num_seen(task_master.RUN_FOREVER, states) == num_workers
            logger.critical('setting mode to TERMINATE')
            task_master.set_mode(work_spec['name'], task_master.TERMINATE)
        
        if num_seen(task_master.TERMINATE, states) == num_workers:
            assert num_seen(task_master.IDLE, states) == num_workers
            break

    workers.join()
    logger.info('finished running %d worker processes' % num_workers)
