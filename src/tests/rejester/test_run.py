
import logging
import os
import time
import uuid
import pytest
import signal
import subprocess

@pytest.fixture(scope='function')
def path(request):
    path = os.path.join('/tmp', str(uuid.uuid4()))
    def fin():
        try: os.unlink(path)
        except: pass
    request.addfinalizer(fin)
    return path

def test_run(path):
    logging.info('pidfile=%r', path)
    p = subprocess.Popen(
        ['rejester', 'run', 'test_namespace', '--pidfile', path],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    max_time = 20
    start_time = time.time()
    elapsed = 0
    print 'starting...'
    print '%r exists = %r' % (path, os.path.exists(path))
    while elapsed < max_time:
        print '%r exists = %r' % (path, os.path.exists(path))
        elapsed = time.time() - start_time
        out, err = p.communicate()
        print out
        print err
        ret = p.poll()
        if ret is not None:
            break
    assert ret == 0
    while elapsed < max_time:
        if os.path.exists(path):
            break
        print '%r not there yet, wait' % (path,)
        time.sleep(0.2)  # wait a moment for daemon child to wake up and write pidfile
        elapsed = time.time() - start_time
    pid = int(open(path).read())
    os.kill(pid, signal.SIGTERM)
    print 'killed', pid
