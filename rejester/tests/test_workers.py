'''
.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import contextlib
import os
import copy
import time
import logging
import multiprocessing
import signal
import subprocess
import sys

import pytest
import yaml

import rejester
from rejester.workers import run_worker, MultiWorker, SingleWorker

logger = logging.getLogger(__name__)
pytest_plugins = 'rejester.tests.fixtures'

@pytest.yield_fixture
def in_tmpdir(tmpdir):
    oldpwd = os.getcwd()
    try:
        os.chdir(str(tmpdir))
        yield
    finally:
        os.chdir(oldpwd)

def test_task_register(task_master):
    worker = MultiWorker(task_master.registry.config)
    worker_id = worker.register()
    assert worker_id in task_master.workers()
    worker.unregister()
    assert worker_id not in task_master.workers()


def work_program(work_unit):
    ## just to show that this works, we get the config from the data
    ## and *reconnect* to the registry with a second instances instead
    ## of using work_unit.registry
    config = work_unit.data['config']
    sleeptime = float(work_unit.data.get('sleep', 9.0))
    task_master = rejester.TaskMaster(config)
    logger.info('executing work_unit %r ... %s', work_unit.key, sleeptime)
    time.sleep(sleeptime)  # pretend to work

def work_program_broken(work_unit):
    logger.info('executing "broken" work_unit %r', work_unit.key)

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
    min_gb = 0.01,
    config = dict(many=' ' * 2**10, params=''),
    module = 'rejester.tests.test_workers',
    run_function = 'work_program',
    terminate_function = 'work_program',
)

def test_single_worker(task_master):
    work_units = {'key{}'.format(x): { 'config': task_master.registry.config,
                                       'sleep': 1 }
                  for x in xrange(2)}
    task_master.update_bundle(work_spec, work_units)
    assert task_master.num_finished(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == 2

    worker = SingleWorker(task_master.config)
    worker.register()
    rc = worker.run()
    assert rc is True
    worker.unregister()
    assert task_master.num_pending(work_spec['name']) == 0
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) == 1
    assert task_master.num_available(work_spec['name']) == 1

    worker = SingleWorker(task_master.config)
    worker.register()
    rc = worker.run()
    assert rc is True
    worker.unregister()
    assert task_master.num_finished(work_spec['name']) == 2
    assert task_master.num_available(work_spec['name']) == 0

    worker = SingleWorker(task_master.config)
    worker.register()
    rc = worker.run()
    assert rc is False
    worker.unregister()
    assert task_master.num_finished(work_spec['name']) == 2
    assert task_master.num_available(work_spec['name']) == 0

@contextlib.contextmanager
def run_multi_worker(task_master, duration):
    """Spawn a MultiWorker and clean up from it sanely.

    Use this as:

    >>> with run_multi_worker(task_master, 60) as deadline:
    ...   while time.time() < deadline:
    ...     # do some work
    ...     time.sleep(1)

    On exiting the context block, this will do a very clean shutdown
    of the workers; if anything goes wrong this will do an unclean shutdown
    of the workers.

    """
    p = multiprocessing.Process(target=run_worker, 
                                args=(MultiWorker, task_master.registry.config))
    num_workers = multiprocessing.cpu_count()
    logger.debug('expecting num_workers=%d', num_workers)

    start = time.time()
    end = start + duration
    finished_cleanly = False
    already_set_idle = False
    already_set_terminate = False
    p.start()

    try:
        yield end

        while time.time() < end:
            modes = task_master.mode_counts()
            logger.debug(modes)

            if modes[task_master.RUN] >= num_workers:
                logger.info('setting mode to IDLE')
                task_master.idle_all_workers()
                already_set_idle = True

            if modes[task_master.IDLE] >= 0 and already_set_idle:
                logger.info('setting mode to TERMINATE')
                task_master.set_mode(task_master.TERMINATE)
                already_set_terminate = True
        
            if p.exitcode is not None:
                assert already_set_idle
                assert already_set_terminate
                if p.exitcode == 0:
                    finished_cleanly = True
                break

            time.sleep(2)

        assert finished_cleanly, ('timed out after {0} seconds'
                                  .format(time.time() - start))

        p.join()
        logger.info('finished running %d worker processes', num_workers)
    finally:
        if p.is_alive():
            logger.debug("killing worker processes pid={0}".format(p.pid))
            p.terminate()
            p.join(1.0)
        if p.is_alive():
            logger.warn("worker processes pid={0} failed to die, hard killing"
                        .format(p.pid))
            os.kill(p.pid, signal.SIGKILL)
            p.join(1.0)
        if p.is_alive():
            logger.critical("worker processes pid={0} resisted SIGKILL"
                            .format(p.pid))

def test_task_master_multi_worker(task_master):
    num_units = 10
    num_units_cursor = 0
    work_units = {'key' + str(x): dict(config=task_master.registry.config) 
                  for x in xrange(num_units_cursor, num_units_cursor + num_units)}
    num_units_cursor += num_units
    task_master.update_bundle(work_spec, work_units)

    task_master.set_mode(task_master.RUN)

    with run_multi_worker(task_master, 60):
        pass

    num_workers = multiprocessing.cpu_count()
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

    with run_multi_worker(task_master, 60):
        pass

    num_workers = multiprocessing.cpu_count()
    assert task_master.num_failed(work_spec['name']) >= num_workers
    assert task_master.num_finished(work_spec['name']) == 0


@pytest.mark.slow
def test_task_master_multi_worker_multi_update(task_master):
    num_units = 10
    num_units_cursor = 0
    workduration = 2.0
    work_units = {'key' + str(x): dict(config=task_master.registry.config, sleep=workduration)
                  for x in xrange(num_units_cursor, num_units_cursor + num_units)}
    num_units_cursor += num_units
    task_master.update_bundle(work_spec, work_units)

    task_master.set_mode(task_master.RUN)

    with run_multi_worker(task_master, 120) as end:

        logger.info('waiting for first set to finish')
        while time.time() < end:
            assert task_master.num_failed(work_spec['name']) == 0
            if task_master.num_finished(work_spec['name']) >= len(work_units):
                break

            time.sleep(2)

        # add more work
        work_units2 = {'key' + str(x): dict(config=task_master.registry.config)
                       for x in xrange(num_units_cursor, num_units_cursor + num_units)}
        task_master.update_bundle(work_spec, work_units2)

        logger.info('waiting for second set to finish')
        while time.time() < end:
            assert task_master.num_failed(work_spec['name']) == 0
            if task_master.num_finished(work_spec['name']) >= (num_units * 2):
                break

            time.sleep(2)

        logger.info('all jobs finished')

    num_workers = multiprocessing.cpu_count()
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) >= num_workers


def test_task_master_multi_worker_multi_update_miniwait(task_master):
    num_units = 10
    num_units_cursor = 0
    workduration = 2.0
    work_units = {'key' + str(x): dict(config=task_master.registry.config, sleep=workduration)
                  for x in xrange(num_units_cursor, num_units_cursor + num_units)}
    num_units_cursor += num_units
    task_master.update_bundle(work_spec, work_units)

    task_master.set_mode(task_master.RUN)

    with run_multi_worker(task_master, 120) as end:
        # just wait a little bit, presumably some set of things are running
        time.sleep(1)

        # add more work
        work_units2 = {'key' + str(x): dict(config=task_master.registry.config, sleep=workduration)
                       for x in xrange(num_units_cursor, num_units_cursor + num_units)}
        task_master.update_bundle(work_spec, work_units2)

        while time.time() < end:
            assert task_master.num_failed(work_spec['name']) == 0
            num_finished = task_master.num_finished(work_spec['name'])
            if num_finished >= (num_units * 2):
                break
            logger.debug('work spec %r finished=%s', work_spec['name'], num_finished)
            time.sleep(2)

    num_workers = multiprocessing.cpu_count()
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) >= num_workers

@pytest.mark.slow
def test_fork_worker_expiry(task_master, tmpdir, in_tmpdir):
    '''Test that job expiration and default_lifetime work as expected.'''
    # If this accidentally runs in the top-level rejester directory,
    # WorkUnit.module's __import__ call doesn't find it...strange.
    # The in_tmpdir fixture runs this test with the current working
    # directory set to tmpdir to avoid this.

    # Create a job that sleeps for 10 seconds
    work_units = {'key0': {'config': task_master.registry.config,
                           'sleep': 10.0}}
    task_master.update_bundle(work_spec, work_units)
    task_master.set_mode(task_master.RUN)

    assert task_master.status(work_spec['name']) == {
        'num_available': 1, 'num_pending': 0,
        'num_finished': 0, 'num_failed': 0,
        'num_blocked': 0, 'num_tasks': 1,
    }

    pid1 = None
    pid2 = None

    try:
        # Start a ForkWorker, but with a 1-second job expiration
        c1 = {'rejester': dict(task_master.registry.config)}
        c1['rejester']['default_lifetime'] = 1
        c1['rejester']['worker'] = 'fork_worker'
        c1['rejester'].setdefault('fork_worker', {})
        c1['rejester']['fork_worker']['num_workers'] = 1
        fn1 = tmpdir.join('c1.yaml')
        fn1.write(yaml.dump(c1))
        pidfn1 = tmpdir.join('c1.pid')
        subprocess.check_call([sys.executable,
                               '-m', 'rejester.run_multi_worker',
                               '-c', str(fn1), '--pidfile', str(pidfn1)])
        # Wait for the worker to start up (up to 5s)
        for i in xrange(50):
            if pidfn1.exists():
                pid1 = int(pidfn1.read())
                break
            time.sleep(0.1)
        assert pid1 is not None, "worker didn't start up"

        time.sleep(0.1)

        # Now the job should be going
        assert task_master.status(work_spec['name']) == {
            'num_available': 0, 'num_pending': 1,
            'num_finished': 0, 'num_failed': 0,
            'num_blocked': 0, 'num_tasks': 1,
        }

        time.sleep(2.0)

        # Now the worker is still doing the job, but it's also in the
        # available queue again
        assert task_master.status(work_spec['name']) == {
            'num_available': 1, 'num_pending': 0,
            'num_finished': 0, 'num_failed': 0,
            'num_blocked': 0, 'num_tasks': 1,
        }

        # So start a second worker, with the default timeout
        c2 = {'rejester': dict(task_master.registry.config)}
        c2['rejester']['worker'] = 'fork_worker'
        c2['rejester'].setdefault('fork_worker', {})
        c2['rejester']['fork_worker']['num_workers'] = 1
        fn2 = tmpdir.join('c2.yaml')
        fn2.write(yaml.dump(c2))
        pidfn2 = tmpdir.join('c2.pid')
        subprocess.check_call([sys.executable,
                               '-m', 'rejester.run_multi_worker',
                               '-c', str(fn2), '--pidfile', str(pidfn2)])
        for i in xrange(50):
            if pidfn2.exists():
                pid2 = int(pidfn2.read())
                break
            time.sleep(0.1)
        assert pid2 is not None, "worker didn't start up"

        time.sleep(0.1)

        # Now the second worker should have picked up the job
        assert task_master.status(work_spec['name']) == {
            'num_available': 0, 'num_pending': 1,
            'num_finished': 0, 'num_failed': 0,
            'num_blocked': 0, 'num_tasks': 1,
        }

        # Wait for it to finish
        time.sleep(10.0)

        # Now: both workers should have finished the job; the first
        # worker catches LostLease and reports nothing; the second
        # worker reports successful completion
        assert task_master.status(work_spec['name']) == {
            'num_available': 0, 'num_pending': 0,
            'num_finished': 1, 'num_failed': 0,
            'num_blocked': 0, 'num_tasks': 1,
        }

    finally:
        if pid1 is not None:
            os.kill(pid1, signal.SIGTERM)
        if pid2 is not None:
            os.kill(pid2, signal.SIGTERM)
