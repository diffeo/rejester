"""Standalone test for 'rejester run_worker'.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.

"""

from __future__ import absolute_import
import errno
import logging
import os
import signal
import sys
import time

import pexpect
import pytest

logger = logging.getLogger(__name__)
pytest_plugins = 'rejester.support.test'

@pytest.fixture
def rejester_cli_namespace(request, namespace_string):
    """a rejester namespace that deletes itself using 'rejester delete'"""
    def fin():
        pexpect.run('rejester delete {0} --yes'.format(namespace_string),
                    logfile=sys.stdout, timeout=5)
    request.addfinalizer(fin)
    return namespace_string

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
    pexpect.run('rejester run_worker {0} --pidfile {1} --logpath {2}'
                .format(namespace, tmp_pid, tmp_log),
                logfile=sys.stdout,
                timeout=5)
    # This spawns a daemon and exits, so run() should return promptly,
    # and the pid file should exist
    assert os.path.exists(tmp_pid)
    pid = int(open(tmp_pid, 'r').read())
    # Unixese for "make sure this process exists"
    os.kill(pid, 0)
    try:
        logger.info('Sending SIGTERM to pid {0}'.format(pid))
        os.kill(pid, signal.SIGTERM)
        start_time = 0
        end_time = time.time() + 20
        while time.time() < end_time:
            try:
                os.kill(pid, 0)
            except OSError, exc:
                assert exc.errno == errno.ESRCH
                break
            logger.debug('{:1f} elapsed seconds, pid {} still alive'
                         .format(time.time() - start_time, pid))
            time.sleep(0.2)  # wait a moment for daemon child to exit
        assert time.time() < end_time, "Daemon process did not die when asked"
    finally:
        # kill -9 pid, if it still exists
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError, exc:
            pass
