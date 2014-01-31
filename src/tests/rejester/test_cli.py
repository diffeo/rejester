'''Tests for the 'rejester' command-line tool.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import contextlib
import errno
import logging
import json
import os
import re
import signal
from subprocess import Popen, PIPE
import sys
import time

import pexpect
import pytest
import yaml

import rejester

logger = logging.getLogger(__name__)

## 1KB sized work_spec config
work_spec = dict(
    name = 'tbundle',
    desc = 'a test work bundle',
    min_gb = 8,
    config = dict(many=' ', params=''),
    module = 'rejester.workers',
    run_function = 'test_work_program',
    terminate_function = 'test_work_program',
)

@contextlib.contextmanager
def rejester_cmd(args, **kwargs):
    """Helper to run rejester in a subshell, cleaning up afterwards"""
    logger.info('rejester {0}'.format(' '.join(args)))
    child = pexpect.spawn('rejester', args=args, **kwargs)
    try:
        yield child
    finally:
        child.close()

def test_cli(task_master, tmpdir):
    tmpf = str(tmpdir.join("work_spec.yaml"))
    with open(tmpf, 'w') as f:
        f.write(yaml.dump(work_spec))
    assert yaml.load(open(tmpf, 'r')) == work_spec

    num_units = 11
    tmpf2 = str(tmpdir.join("work_units.yaml"))
    with open(tmpf2, 'w') as f:
        for x in xrange(num_units):
            work_unit = json.dumps({'key-%d' % x: dict(sleep=0.2, config=task_master.registry.config)})
            f.write(work_unit + '\n')

    namespace = task_master.registry.config['namespace']
    with rejester_cmd(['load', namespace, '--app-name', 'rejester_test',
                       '-u', tmpf2, '-w', tmpf]) as child:
        child.logfile = sys.stdout
        time.sleep(1)
        child.expect('loading', timeout=5)
        child.expect('pushing', timeout=5)
        child.expect('finish', timeout=5)

    logger.debug(json.dumps(task_master.status(work_spec['name']), indent=4))

    args = ['status', namespace,
            '--app-name', 'rejester_test',
            '-w', tmpf]
    p = Popen(['rejester'] + args, stdout=PIPE, stderr=PIPE)
    try:
        (out,err) = p.communicate()
        assert re.search('num_available.*{0}'.format(num_units), out)
    finally:
        p.wait()

    tmp_pid = str(tmpdir.join('pid'))
    tmp_log = str(tmpdir.join('log'))
    with rejester_cmd(['run_worker', namespace, '--app-name', 'rejester_test',
                       '--logpath', tmp_log, '--pidfile', tmp_pid]) as child:
        child.logfile = sys.stdout
        child.expect('entering', timeout=5)    

    max_time = 10
    elapsed = 0
    start_time = time.time()
    while elapsed < max_time:
        if os.path.exists(tmp_pid):
            break
        if os.path.exists(tmp_log):
            logger.debug('found logfile: %s' % open(tmp_log).read())
        logger.debug( '%.1f elapsed seconds, %r not there yet, wait' % (elapsed, tmp_pid))
        time.sleep(0.2)  # wait a moment for daemon child to wake up and write pidfile
        elapsed = time.time() - start_time
    pid = int(open(tmp_pid).read())
    try:
        os.kill(pid, 0) # will raise OSError if pid is dead

        with rejester_cmd(['set_RUN', namespace,
                           '--app-name', 'rejester_test']) as child:
            child.logfile = sys.stdout
            child.expect('set', timeout=5)

        elapsed = 0
        start_time = time.time()
        while elapsed < max_time:
            if task_master.num_finished(work_spec['name']) == num_units:
                break
            if os.path.exists(tmp_log):
                with open(tmp_log, 'r') as f:
                    print f.read()
            logger.info( '%.1f elapsed seconds, %d finished', elapsed,
                         task_master.num_finished(work_spec['name']))
            logger.info(json.dumps(task_master.status(work_spec['name']), indent=4))
            time.sleep(1)
            elapsed = time.time() - start_time

        assert task_master.num_finished(work_spec['name']) == num_units
        logger.info('tasks completed')
    finally:
        os.kill(pid, signal.SIGTERM)
        elapsed = 0
        start_time = time.time()
        while elapsed < max_time:
            try:
                os.kill(pid, 0)
            except OSError, exc:
                # If we get a "no such process" error then the process is dead
                # (which is what we want)
                assert exc.errno == errno.ESRCH
                break
            logger.debug( '%.1f elapsed seconds, %d still alive' % (elapsed, pid))
            time.sleep(0.2)  # wait a moment for daemon child to exit
            elapsed = time.time() - start_time
        # Double-check the process is dead
        try:
            os.kill(pid, 0)
            # if we did not get an exception then the process is alive
            logger.warn('worker pid {0} did not respond to SIGTERM, killing')
            os.kill(pid, signal.SIGKILL)
            assert False, "workers did not shut down"
        except OSError, exc:
            assert exc.errno == errno.ESRCH
            pass
        logger.info('worker exited')
