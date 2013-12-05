
import logging
import os
import time
import uuid
import pytest
import signal
import shutil
import psutil
import subprocess
from rejester._logging import logger

@pytest.fixture(scope='function')
def tmp_dir(request):
    tmp_dir = os.path.join('/tmp', str(uuid.uuid4()))
    os.makedirs(tmp_dir)
    def fin():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    request.addfinalizer(fin)
    return tmp_dir

def test_run(tmp_dir):
    tmp_pid = os.path.join(tmp_dir, 'pid')
    logging.info('pidfile=%r', tmp_pid)
    p = subprocess.Popen(
        ['rejester', 'run_worker', 'test_namespace', '--pidfile', tmp_pid],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    max_time = 20
    start_time = time.time()
    elapsed = 0
    print 'starting...'
    print '%r exists = %r' % (tmp_pid, os.path.exists(tmp_pid))
    while elapsed < max_time:
        print '%r exists = %r' % (tmp_pid, os.path.exists(tmp_pid))
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
        print '%r not there yet, wait' % (tmp_pid,)
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
        logger.critical( '%.1f elapsed seconds, %d still alive' % (elapsed, pid))
        time.sleep(0.2)  # wait a moment for daemon child to exit
        elapsed = time.time() - start_time
    assert pid not in psutil.get_pid_list()
    print 'killed', pid
