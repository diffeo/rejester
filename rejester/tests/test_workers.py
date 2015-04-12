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
import signal
import subprocess
import sys

import pytest
import yaml

import rejester
from rejester.workers import SingleWorker, ForkWorker
from rejester.tests.fixtures import task_master, _rejester_config  # noqa

logger = logging.getLogger(__name__)

# A note on this batch of tests: we need to talk to Redis (which we can
# do fairly synchronously), a child process, and its child processes,
# and the synchronization between all of these is hard.  Basically all
# of these are "slow" tests that depend on work units sleeping, and
# the tests being able to step in at specific times.
#
# If none of these tests run, add --runslow to the py.test command line.


def work_program(work_unit):
    # just to show that this works, we get the config from the data
    # and *reconnect* to the registry with a second instances instead
    # of using work_unit.registry
    config = work_unit.data['config']
    sleeptime = float(work_unit.data.get('sleep', 9.0))
    rejester.TaskMaster(config)  # this constructor does the reconnect
    logger.info('executing work_unit %r ... %s', work_unit.key, sleeptime)
    time.sleep(sleeptime)  # pretend to work


def work_program_broken(work_unit):
    logger.info('executing "broken" work_unit %r', work_unit.key)
    config = work_unit.data['config']
    rejester.TaskMaster(config)
    raise Exception('simulate broken work_unit')


# 1KB sized work_spec config
work_spec = dict(
    name='tbundle',
    desc='a test work bundle',
    min_gb=0.01,
    config=dict(many=' ' * 2**10, params=''),
    module='rejester.tests.test_workers',
    run_function='work_program',
    terminate_function='work_program',
)


@pytest.mark.slow  # noqa
def test_single_worker(task_master):
    work_units = dict([('key{0}'.format(x),
                        {'config': task_master.registry.config,
                         'sleep': 1})
                       for x in xrange(2)])
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


def wait_for(timeout, condition):
    start = time.time()
    end = start + timeout
    while time.time() < end:
        if condition():
            return True
        time.sleep(0.1)
    return False


def assert_started(tm, work_spec, timeout=10):
    assert wait_for(
        timeout,
        lambda: (tm.num_available(work_spec['name']) == 0 or
                 tm.num_pending(work_spec['name']) > 0)), \
        ("worker didn't start doing work " +
         repr(tm.status(work_spec['name'])))


def assert_stopped(tm, work_spec, timeout=10):
    assert wait_for(timeout,
                    lambda: tm.num_pending(work_spec['name']) == 0), \
        ("worker didn't stop doing work " +
         repr(tm.status(work_spec['name'])))


def assert_completion(tm, work_spec, timeout=60):
    assert wait_for(
        timeout,
        lambda: (tm.num_available(work_spec['name']) == 0 and
                 tm.num_pending(work_spec['name']) == 0)), \
        ("worker didn't finish all of the work " +
         repr(tm.status(work_spec['name'])))


@contextlib.contextmanager
def run_worker(config, tmpdir, name='worker'):
    """Spawn a worker subprocess and clean up from it sanely.

    Use this as:

    >>> with run_fork_worker(task_master.registry.config, tmpdir):
    ...   while time.time() < deadline:
    ...     # do some work
    ...     time.sleep(1)

    On exiting the context block, this will do a very clean shutdown
    of the workers; if anything goes wrong this will do an unclean shutdown
    of the workers.

    """
    # Write out the config
    config_fn = tmpdir.join(name + '.yaml')
    config_fn.write(yaml.dump({'rejester': config}))

    # Start the child, with a target pid file
    #
    # (Oddly, if this test runs in the base rejester directory, the
    # re-import of this module to run the work unit fails; but explicitly
    # running the subprocess in tmpdir addresses this.)
    pid_fn = tmpdir.join(name + '.pid')
    subprocess.check_call([sys.executable,
                           '-m', 'rejester.run_multi_worker',
                           '-c', str(config_fn),
                           '--pidfile', str(pid_fn)],
                          cwd=str(tmpdir))

    # Wait for the worker to start up (up to 60s)
    assert wait_for(60, pid_fn.exists), "worker didn't start up"
    pid = int(pid_fn.read())

    # Now we're ready to go
    yield pid

    # Back?  Kill the worker
    if not ForkWorker.pid_is_alive(pid):
        return

    logger.info('stopping worker process pid={0}'.format(pid))
    os.kill(pid, signal.SIGTERM)
    if wait_for(10, lambda: not ForkWorker.pid_is_alive(pid)):
        return

    logger.info('killing worker process pid={0}'.format(pid))
    os.kill(pid, signal.SIGKILL)
    if wait_for(10, lambda: not ForkWorker.pid_is_alive(pid)):
        return

    logger.warn('worker failed to die, look for zombie pid={0}'.format(pid))


@pytest.mark.slow  # noqa
def test_worker_mode(task_master, tmpdir):
    '''Test that the worker shuts down when the mode becomes TERMINATE'''
    task_master.set_mode(task_master.RUN)
    c = dict(task_master.registry.config)
    c.setdefault('fork_worker', {})
    c['fork_worker']['heartbeat_interval'] = 1
    with run_worker(c, tmpdir) as pid:
        task_master.set_mode(task_master.TERMINATE)
        assert wait_for(5, lambda: not ForkWorker.pid_is_alive(pid)), \
            "worker failed to stop after TERMINATE"


@pytest.mark.slow  # noqa
def test_worker_idle(task_master, tmpdir):
    '''Test that the worker correctly honors most modes'''
    task_master.set_mode(task_master.IDLE)
    c = dict(task_master.registry.config)
    c.setdefault('fork_worker', {})
    c['fork_worker']['num_workers'] = 1
    c['fork_worker']['heartbeat_interval'] = 1
    c['fork_worker']['child_lifetime'] = 1
    task_master.update_bundle(work_spec, {'k1': {'config': c, 'sleep': 2},
                                          'k2': {'config': c, 'sleep': 2}})
    with run_worker(c, tmpdir):
        time.sleep(1)

        # Neither job should be going
        assert task_master.num_available(work_spec['name']) == 2

        # Let it go
        task_master.set_mode(task_master.RUN)
        assert_started(task_master, work_spec)

        assert task_master.num_available(work_spec['name']) == 1
        assert task_master.num_pending(work_spec['name']) == 1

        # Switch back to idle
        task_master.set_mode(task_master.IDLE)
        assert_stopped(task_master, work_spec)

        assert task_master.num_available(work_spec['name']) == 1
        assert task_master.num_finished(work_spec['name']) == 1

        # test_worker_mode tests the TERMINATE state


@pytest.mark.slow  # noqa
def test_do_work(task_master, tmpdir):
    num_units = 10
    num_units_cursor = 0
    c = dict(task_master.registry.config)
    work_units = dict([('key' + str(x), {'config': c, 'sleep': 2})
                       for x in xrange(num_units_cursor,
                                       num_units_cursor + num_units)])
    num_units_cursor += num_units
    task_master.update_bundle(work_spec, work_units)
    task_master.set_mode(task_master.RUN)

    with run_worker(c, tmpdir):
        assert_completion(task_master, work_spec)

    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) == num_units


@pytest.mark.slow  # noqa
def test_failed_work(task_master, tmpdir):
    work_spec_broken = copy.deepcopy(work_spec)
    work_spec_broken['run_function'] = 'work_program_broken'

    num_units = 10
    num_units_cursor = 0
    c = dict(task_master.registry.config)
    work_units = dict([('key' + str(x), {'config': c, 'sleep': 2})
                       for x in xrange(num_units_cursor,
                                       num_units_cursor + num_units)])
    num_units_cursor += num_units
    task_master.update_bundle(work_spec_broken, work_units)
    task_master.set_mode(task_master.RUN)

    with run_worker(c, tmpdir):
        assert_completion(task_master, work_spec_broken)

    assert task_master.num_failed(work_spec_broken['name']) == num_units
    assert task_master.num_finished(work_spec_broken['name']) == 0


@pytest.mark.slow  # noqa
def test_add_more_work(task_master, tmpdir):
    num_units = 10
    num_units_cursor = 0
    c = dict(task_master.registry.config)
    work_units = dict([('key' + str(x), {'config': c, 'sleep': 2})
                       for x in xrange(num_units_cursor,
                                       num_units_cursor + num_units)])
    num_units_cursor += num_units
    task_master.update_bundle(work_spec, work_units)
    task_master.set_mode(task_master.RUN)

    with run_worker(c, tmpdir):
        assert_completion(task_master, work_spec)

        work_units = dict([('key' + str(x), {'config': c, 'sleep': 2})
                           for x in xrange(num_units_cursor,
                                           num_units_cursor + num_units)])
        num_units_cursor += num_units
        task_master.update_bundle(work_spec, work_units)

        assert_completion(task_master, work_spec)

    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) == 2 * num_units


@pytest.mark.slow  # noqa
def test_add_work_midway(task_master, tmpdir):
    num_units = 10
    num_units_cursor = 0
    c = dict(task_master.registry.config)
    work_units = dict([('key' + str(x), {'config': c, 'sleep': 2})
                       for x in xrange(num_units_cursor,
                                       num_units_cursor + num_units)])
    num_units_cursor += num_units
    task_master.update_bundle(work_spec, work_units)
    task_master.set_mode(task_master.RUN)

    with run_worker(c, tmpdir):
        assert_started(task_master, work_spec)

        work_units = dict([('key' + str(x), {'config': c, 'sleep': 2})
                           for x in xrange(num_units_cursor,
                                           num_units_cursor + num_units)])
        num_units_cursor += num_units
        task_master.update_bundle(work_spec, work_units)

        assert_completion(task_master, work_spec)

    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) == 2 * num_units


@pytest.mark.slow  # noqa
def test_fork_worker_expiry_kill(task_master, tmpdir):
    '''Test that job expiry stops a job.'''
    c = dict(task_master.registry.config)
    c['default_lifetime'] = 3  # so the job will expire quickly
    c.setdefault('fork_worker', {})
    c['fork_worker']['num_workers'] = 1
    c['fork_worker']['heartbeat_interval'] = 1
    c['fork_worker']['stop_jobs_early'] = 1

    # Create a job that sleeps for 10 seconds.  What should actually
    # happen is that, after 2-3 seconds, the "stop jobs early" monitor
    # kills it off.
    work_units = {'key0': {'config': c, 'sleep': 10}}
    task_master.update_bundle(work_spec, work_units)
    task_master.set_mode(task_master.RUN)

    with run_worker(c, tmpdir):
        assert_started(task_master, work_spec)
        time.sleep(5.0)
        # Now (some synchronization issues aside) the job *should*
        # be dead
        assert task_master.status(work_spec['name']) == {
            'num_available': 0, 'num_pending': 0,
            'num_finished': 0, 'num_failed': 1,
            'num_blocked': 0, 'num_tasks': 1,
        }


@pytest.mark.slow  # noqa
def test_fork_worker_expiry_dup(task_master, tmpdir):
    '''Test that job expiration and default_lifetime work as expected.'''
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

    # Start a ForkWorker, but with a 1-second job expiration
    c1 = dict(task_master.registry.config)
    c1['default_lifetime'] = 1
    c1['worker'] = 'fork_worker'
    c1.setdefault('fork_worker', {})
    c1['fork_worker']['num_workers'] = 1
    with run_worker(c1, tmpdir, 'c1'):
        assert_started(task_master, work_spec)
        time.sleep(2.0)

        # Now the worker is still doing the job, but it's also in the
        # available queue again
        assert task_master.status(work_spec['name']) == {
            'num_available': 1, 'num_pending': 0,
            'num_finished': 0, 'num_failed': 0,
            'num_blocked': 0, 'num_tasks': 1,
        }

        # So start a second worker, with the default timeout
        c2 = dict(task_master.registry.config)
        c2['worker'] = 'fork_worker'
        c2.setdefault('fork_worker', {})
        c2['fork_worker']['num_workers'] = 1
        with run_worker(c2, tmpdir, 'c2'):
            assert_started(task_master, work_spec)

            # Now the second worker should have picked up the job
            assert task_master.status(work_spec['name']) == {
                'num_available': 0, 'num_pending': 1,
                'num_finished': 0, 'num_failed': 0,
                'num_blocked': 0, 'num_tasks': 1,
            }

            # Wait for it to finish
            assert_completion(task_master, work_spec)

            # Now: both workers should have finished the job; the first
            # worker catches LostLease and reports nothing; the second
            # worker reports successful completion
            assert task_master.status(work_spec['name']) == {
                'num_available': 0, 'num_pending': 0,
                'num_finished': 1, 'num_failed': 0,
                'num_blocked': 0, 'num_tasks': 1,
            }
