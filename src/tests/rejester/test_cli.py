'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import sys
import copy
import time
import yaml
import json
import psutil
import pytest
import signal
import pexpect
import logging
import tempfile
import rejester
from subprocess import Popen, PIPE
from rejester._logging import logger

from tests.rejester.test_task_master import task_master  ## a fixture that cleans up
from tests.rejester.test_run import tmp_dir  ## a fixture that cleans up

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

def test_cli(task_master, tmp_dir):
    tmpf = tempfile.NamedTemporaryFile(delete=True)
    tmpf.write(yaml.dump(work_spec))
    tmpf.flush()
    assert yaml.load(open(tmpf.name)) == work_spec

    #open('foo.yaml', 'wb').write(yaml.dump(work_spec))

    num_units = 11
    tmpf2 = tempfile.NamedTemporaryFile(delete=True)
    for x in xrange(num_units):
        work_unit = json.dumps({'key-%d' % x: dict(sleep=0.2, config=task_master.registry.config)})
        tmpf2.write(work_unit + '\n')
    tmpf2.flush()

    namespace = task_master.registry.config['namespace']
    cmd = 'rejester load ' + namespace + ' --app-name rejester_test -u '\
          + tmpf2.name + ' -w ' + tmpf.name
    logger.info(cmd)
    child = pexpect.spawn(cmd)
    child.logfile = sys.stdout
    time.sleep(1)
    child.expect('loading', timeout=5)
    child.expect('pushing', timeout=5)
    child.expect('finish', timeout=5)

    logger.critical(json.dumps(task_master.status(work_spec['name']), indent=4))

    cmd = 'rejester status ' + namespace + ' --app-name rejester_test -w ' + tmpf.name
    logger.info(cmd)
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    logger.critical(p.stderr.read())
    logger.critical(p.stdout.read())
    child = pexpect.spawn(cmd)
    child.expect('num_available.*%d' % num_units)

    tmp_pid = os.path.join(tmp_dir, 'pid')
    tmp_log = os.path.join(tmp_dir, 'log')
    cmd = 'rejester run_worker ' + namespace + ' --app-name rejester_test --logpath '\
          + tmp_log + ' --pidfile ' + tmp_pid
    logger.info(cmd)
    child = pexpect.spawn(cmd)
    child.logfile = sys.stdout
    child.expect('entering', timeout=5)    
    max_time = 10
    elapsed = 0
    start_time = time.time()
    while elapsed < max_time:
        if os.path.exists(tmp_pid):
            break
        if os.path.exists(tmp_log):
            logger.critical('found logfile: %s' % open(tmp_log).read())
        logger.critical( '%.1f elapsed seconds, %r not there yet, wait' % (elapsed, tmp_pid))
        time.sleep(0.2)  # wait a moment for daemon child to wake up and write pidfile
        elapsed = time.time() - start_time
    pid = int(open(tmp_pid).read())
    assert pid in psutil.get_pid_list()

    cmd = 'rejester set_RUN ' + namespace + ' --app-name rejester_test'
    logger.info(cmd)
    child = pexpect.spawn(cmd)
    child.logfile = sys.stdout
    child.expect('set', timeout=5)

    elapsed = 0
    start_time = time.time()
    while elapsed < max_time:
        if task_master.num_finished(work_spec['name']) == num_units:
            break
        if os.path.exists(tmp_log):
            logger.critical('found logfile: %s' % open(tmp_log).read())
        logger.critical( '%.1f elapsed seconds, %d finished', elapsed,
                         task_master.num_finished(work_spec['name']))
        logger.critical(json.dumps(task_master.status(work_spec['name']), indent=4))
        time.sleep(1)
        elapsed = time.time() - start_time
    assert task_master.num_finished(work_spec['name']) == num_units
    logger.info('tasks completed')

    os.kill(pid, signal.SIGTERM)
    elapsed = 0
    start_time = time.time()
    while elapsed < max_time:
        if pid not in psutil.get_pid_list():
            break
        logger.critical( '%.1f elapsed seconds, %d still alive' % (elapsed, pid))
        time.sleep(0.2)  # wait a moment for daemon child to exit
        elapsed = time.time() - start_time
    assert pid not in psutil.get_pid_list()
    logger.info('worker exited')
