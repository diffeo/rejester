
import logging
import os
import time
import uuid
import pytest
import signal
import psutil
import subprocess
from rejester._logging import logger
from tests.rejester.make_namespace_string import make_namespace_string

@pytest.fixture
def rejester_cli_namespace(request):
    namespace = make_namespace_string()
    def fin():
        subprocess.call(['rejester', 'delete', namespace])
        pass
    request.addfinalizer(fin)
    return namespace

def test_run(tmpdir, rejester_cli_namespace):
    namespace = rejester_cli_namespace
    tmp_pid = str(tmpdir.join('pid'))
    tmp_log = str(tmpdir.join('log'))
    logger.info('pidfile=%r', tmp_pid)
    p = subprocess.Popen(
        ['rejester', 'run_worker', namespace, '--pidfile', tmp_pid,
         '--logpath', tmp_log],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    max_time = 20
    start_time = time.time()
    elapsed = 0
    while elapsed < max_time:
        logger.debug('{0} exists? {1}'.format(tmp_pid, os.path.exists(tmp_pid)))
        elapsed = time.time() - start_time
        out, err = p.communicate()
        print out
        print err
        ret = p.poll()
        if ret is not None:
            break
    assert ret == 0
    while elapsed < max_time:
        if os.path.exists(tmp_pid):
            break
        logger.debug('{0} not there yet, wait'.format(tmp_pid))
        time.sleep(0.2)  # wait a moment for daemon child to wake up and write pidfile
        elapsed = time.time() - start_time
    pid = int(open(tmp_pid).read())
    assert pid in psutil.get_pid_list()
    os.kill(pid, signal.SIGTERM)
    elapsed = 0
    start_time = time.time()
    while elapsed < max_time:
        if pid not in psutil.get_pid_list():
            break
        logger.debug( '%.1f elapsed seconds, %d still alive' % (elapsed, pid))
        time.sleep(0.2)  # wait a moment for daemon child to exit
        elapsed = time.time() - start_time
    assert pid not in psutil.get_pid_list()
    logger.debug('killed {0}'.format(pid))
