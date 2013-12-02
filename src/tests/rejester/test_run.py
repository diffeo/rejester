
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

@pytest.mark.xfail
def test_run(path):
    p = subprocess.Popen(
        ['rejester', 'run', 'test_namespace', '--pidfile', path],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    max_time = 20
    start_time = time.time()
    elapsed = 0
    print 'starting...'
    while elapsed < max_time:
        elapsed = time.time() - start_time
        out, err = p.communicate()
        print out
        print err
        ret = p.poll()
        if ret is not None:
            break
    assert ret == 0
    pid = open(path).read()
    os.kill(pid, signal.SIGTERM)
    print 'killed', pid
