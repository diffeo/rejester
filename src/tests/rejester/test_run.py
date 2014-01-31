
import os
import time
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
    """Lifecycle test for 'rejester run_worker'.

    Running the top-level program starts a worker as a daemon process,
    so we need to verify that (1) the 'rejester' subprocess exits,
    (2) the daemon starts up, and (3) the daemon dies when signaled.

    """
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
    try:
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
    finally:
        if p.returncode is None:
            p.poll()
        if p.returncode is None:
            p.terminate()
            time.sleep(0.1)
            p.poll()
        if p.returncode is None:
            p.kill()
            time.sleep(0.1)
            p.poll()

    while elapsed < max_time:
        if os.path.exists(tmp_pid):
            break
        logger.debug('{0} not there yet, wait'.format(tmp_pid))
        time.sleep(0.2)  # wait a moment for daemon child to wake up and write pidfile
        elapsed = time.time() - start_time
    pid = int(open(tmp_pid).read())
    # Unixese for "make sure this process exists"
    os.kill(pid, 0)
    try:
        logger.info('Sending SIGTERM to pid {0}'.format(pid))
        os.kill(pid, signal.SIGTERM)
        elapsed = 0
        start_time = time.time()
        while elapsed < max_time:
            try:
                os.kill(pid, 0)
            except OSError, exc:
                assert exc.errno == 3
                break
            logger.debug( '%.1f elapsed seconds, %d still alive' % (elapsed, pid))
            time.sleep(0.2)  # wait a moment for daemon child to exit
            elapsed = time.time() - start_time
        assert elapsed < max_time, "Daemon process did not die when asked"
    finally:
        # kill -9 pid, if it still exists
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError, exc:
            pass
