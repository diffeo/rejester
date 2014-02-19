'''Tests for the 'rejester' command-line tool.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import errno
import logging
import json
import os
import re
import signal
import sys
import time

import pexpect
import pytest
import yaml

import rejester

logger = logging.getLogger(__name__)
pytest_plugins = 'rejester.tests.fixtures'

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
    child = pexpect.spawn('rejester load {} --app-name rejester_test '
                          '-u {} -w {}'.format(namespace, tmpf2, tmpf),
                          logfile=sys.stdout)
    try:
        child.expect('loading', timeout=5)
        child.expect('pushing', timeout=5)
        child.expect('finish', timeout=5)
    finally:
        child.close()

    logger.debug(json.dumps(task_master.status(work_spec['name']), indent=4))

    out = pexpect.run('rejester status {} --app-name rejester_test -w {}'
                      .format(namespace, tmpf),
                      timeout=5, logfile=sys.stdout)
    assert re.search('num_available.*{0}'.format(num_units), out)

    out = pexpect.run('rejester work_units {} --app-name rejester_test -w {}'
                      .format(namespace, tmpf),
                      timeout=5, logfile=sys.stdout)
    assert out == ''.join(sorted(['key-{}\r\n'.format(n)
                                  for n in xrange(num_units)]))

    tmp_pid = str(tmpdir.join('pid'))
    tmp_log = str(tmpdir.join('log'))
    out = pexpect.run('rejester run_worker {} --app-name rejester_test '
                      '--logpath {} --pidfile {}'
                      .format(namespace, tmp_log, tmp_pid),
                      timeout=5, logfile=sys.stdout)
    assert os.path.exists(tmp_pid)
    pid = int(open(tmp_pid).read())
    try:
        os.kill(pid, 0) # will raise OSError if pid is dead

        out = pexpect.run('rejester set_RUN {} --app-name rejester_test'
                          .format(namespace),
                          timeout=5, logfile=sys.stdout) 
        assert out.find('set') != -1

        start_time = time.time()
        end_time = start_time + 60
        while time.time() < end_time:
            if task_master.num_finished(work_spec['name']) == num_units:
                break
            if os.path.exists(tmp_log):
                with open(tmp_log, 'r') as f:
                    print f.read()
            logger.info('{:.1f} elapsed seconds, {} finished'
                        .format(time.time() - start_time,
                                task_master.num_finished(work_spec['name'])))
            logger.debug(json.dumps(task_master.status(work_spec['name'])))
            time.sleep(1)

        assert task_master.num_finished(work_spec['name']) == num_units
        logger.info('tasks completed')
    finally:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError, exc:
            pass # assume errno == -ESRCH; we'll do more below
        start_time = time.time()
        end_time = start_time + 20
        while time.time() < end_time:
            try:
                os.kill(pid, 0)
            except OSError, exc:
                # If we get a "no such process" error then the process is dead
                # (which is what we want)
                assert exc.errno == errno.ESRCH
                break
            logger.debug('{:.1f} elapsed seconds, {} still alive'
                         .format(time.time() - start_time, pid))
            time.sleep(0.2)  # wait a moment for daemon child to exit
        # Double-check the process is dead
        try:
            os.kill(pid, 0)
            # if we did not get an exception then the process is alive
            logger.warn('worker pid {0} did not respond to SIGTERM, killing'
                        .format(pid))
            os.kill(pid, signal.SIGKILL)
            assert False, "workers did not shut down"
        except OSError, exc:
            assert exc.errno == errno.ESRCH
            pass
        logger.info('worker exited')
