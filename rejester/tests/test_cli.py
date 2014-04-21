'''Tests for the 'rejester' command-line tool.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
from cStringIO import StringIO
import errno
import logging
import json
import os
import signal
import subprocess
import sys
import time

import pytest
import yaml

import rejester
from rejester.run import Manager
from rejester._task_master import Worker
from rejester.tests.fixtures import task_master, _rejester_config
import yakonfig

logger = logging.getLogger(__name__)

@pytest.yield_fixture
def global_config(_rejester_config):
    with yakonfig.defaulted_config([rejester],
                                   config={'rejester': _rejester_config}):
        yield yakonfig.get_global_config()

@pytest.fixture
def manager(global_config, task_master):
    mgr = Manager()
    mgr._task_master = task_master
    mgr.stdout = StringIO()
    return mgr

class TrivialWorker(Worker):
    def run(self):
        self.work_unit.run()

@pytest.yield_fixture
def worker(_rejester_config):
    w = TrivialWorker(_rejester_config)
    w.register()
    yield w
    w.unregister()

@pytest.fixture
def work_spec():
    return dict(
        name = 'tbundle',
        desc = 'a test work bundle',
        min_gb = 8,
        config = dict(many=' ', params=''),
        module = 'rejester.workers',
        run_function = 'test_work_program',
        terminate_function = 'test_work_program',
    )

@pytest.fixture
def work_units(global_config):
    num_units = 11
    return { 'key-{}'.format(x): { 'sleep': 0.2, 'config': global_config }
             for x in xrange(num_units) }

@pytest.fixture
def work_spec_path(work_spec, tmpdir):
    tmpf = str(tmpdir.join("work_spec.yaml"))
    with open(tmpf, 'w') as f:
        f.write(yaml.dump(work_spec))
    return tmpf

@pytest.fixture
def work_units_path(work_units, tmpdir):
    tmpf = str(tmpdir.join('work_units.yaml'))
    with open(tmpf, 'w') as f:
        for k, v in work_units.iteritems():
            f.write(json.dumps({k: v}) + '\n')
    return tmpf

@pytest.fixture
def loaded(task_master, work_spec, work_units):
    task_master.update_bundle(work_spec, work_units)

def test_load_args(manager, work_spec_path, work_units_path):
    '''various tests for invalid arguments to "load"'''
    with pytest.raises(SystemExit):
        manager.runcmd('load', [])
    with pytest.raises(SystemExit):
        manager.runcmd('load', ['-w', work_spec_path])
    with pytest.raises(SystemExit):
        manager.runcmd('load', ['-u', work_units_path])
    with pytest.raises(SystemExit):
        manager.runcmd('load', ['-w', os.path.basename(work_spec_path),
                                '-u', work_units_path])
    with pytest.raises(SystemExit):
        manager.runcmd('load', ['-w', work_spec_path,
                                '-u', os.path.basename(work_units_path)])

def test_load(manager, work_spec_path, work_units_path, work_spec, work_units):
    manager.runcmd('load', ['-w', work_spec_path, '-u', work_units_path])

    tm = manager.task_master
    spec = tm.get_work_spec(work_spec['name'])
    assert spec['desc'] == work_spec['desc']
    units = tm.list_work_units(work_spec['name'])
    assert len(units) == len(work_units)
    assert sorted(units.keys()) == sorted(work_units.keys())

def test_delete(manager, loaded, work_spec, namespace_string):
    spec = manager.task_master.get_work_spec(work_spec['name'])
    assert spec['desc'] == work_spec['desc']

    manager.runcmd('delete', ['-y'])
    assert (manager.stdout.getvalue() ==
            'deleting namespace \'' + namespace_string + '\'\n')

    spec = manager.task_master.get_work_spec(work_spec['name'])
    assert spec is None

def test_work_spec_bad(manager, loaded, tmpdir):
    with pytest.raises(SystemExit):
        manager.runcmd('work_spec', [])
    with pytest.raises(SystemExit):
        manager.runcmd('work_spec', ['-w', str(tmpdir.join('missing'))])

def test_work_spec_by_name(manager, loaded, work_spec):
    manager.runcmd('work_spec', ['-W', work_spec['name']])
    spec = json.loads(manager.stdout.getvalue())
    assert spec == work_spec

def test_work_spec_by_file(manager, loaded, work_spec, work_spec_path):
    manager.runcmd('work_spec', ['-w', work_spec_path])
    spec = json.loads(manager.stdout.getvalue())
    assert spec == work_spec

def test_status_bad(manager, loaded, tmpdir):
    with pytest.raises(SystemExit):
        manager.runcmd('status', [])
    with pytest.raises(SystemExit):
        manager.runcmd('status', ['-w', str(tmpdir.join('missing'))])

def test_status_by_name(manager, loaded, work_spec, work_units):
    manager.runcmd('status', ['-W', work_spec['name']])
    status = json.loads(manager.stdout.getvalue())
    ref = {
        'num_available': len(work_units),
        'num_blocked': 0,
        'num_failed': 0,
        'num_finished': 0,
        'num_pending': 0,
        'num_tasks': len(work_units),
    }
    assert status == ref

def test_status_by_file(manager, loaded, work_spec_path, work_units):
    manager.runcmd('status', ['-w', work_spec_path])
    status = json.loads(manager.stdout.getvalue())
    ref = {
        'num_available': len(work_units),
        'num_blocked': 0,
        'num_failed': 0,
        'num_finished': 0,
        'num_pending': 0,
        'num_tasks': len(work_units),
    }
    assert status == ref

def test_status_not_loaded(manager, work_spec):
    manager.runcmd('status', ['-W', work_spec['name']])
    status = json.loads(manager.stdout.getvalue())
    ref = {
        'num_available': 0,
        'num_blocked': 0,
        'num_failed': 0,
        'num_finished': 0,
        'num_pending': 0,
        'num_tasks': 0,
    }
    assert status == ref

def test_status_with_work(manager, loaded, work_spec, work_units, worker):
    for n in xrange(3):
        unit = manager.task_master.get_work(worker.worker_id, available_gb=16)
        unit.finish()
    for n in xrange(2):
        unit = manager.task_master.get_work(worker.worker_id, available_gb=16)
        unit.fail()
    unit = manager.task_master.get_work(worker.worker_id, available_gb=16)
    
    manager.runcmd('status', ['-W', work_spec['name']])
    status = json.loads(manager.stdout.getvalue())
    ref = {
        'num_available': len(work_units)-6,
        'num_blocked': 0,
        'num_failed': 2,
        'num_finished': 3,
        'num_pending': 1,
        'num_tasks': len(work_units),
    }
    assert status == ref

def test_work_units_names(manager, loaded, work_spec, work_units):
    manager.runcmd('work_units', ['-W', work_spec['name']])
    response = manager.stdout.getvalue()
    names = response.strip().split('\n')
    assert sorted(names) == sorted(work_units.keys())

def test_work_units_details(manager, loaded, work_spec, work_units):
    manager.runcmd('work_units', ['-W', work_spec['name'], '--details'])
    response = manager.stdout.getvalue()
    found = set()
    for line in response.strip().split('\n'):
        k,v = line.split(': ', 1)
        assert k.startswith("u'")
        assert k.endswith("'")
        key = k[2:-1]
        assert key in work_units
        # We should check the right-hand side of this too, but there
        # is some ickiness around Unicode round-tripping
        found.add(key)
    assert sorted(found) == sorted(work_units.keys())

def test_failed_trivial(manager, loaded, work_spec):
    manager.runcmd('failed', ['-W', work_spec['name']])
    assert manager.stdout.getvalue() == ''

def test_failed_one(manager, worker, loaded, work_spec):
    unit = manager.task_master.get_work(worker.worker_id, available_gb=16)
    assert unit.work_spec_name == work_spec['name']
    unit.fail()

    manager.runcmd('failed', ['-W', work_spec['name']])
    assert manager.stdout.getvalue() == unit.key + '\n'
    
def test_failed_one_details(manager, worker, loaded, work_spec):
    unit = manager.task_master.get_work(worker.worker_id, available_gb=16)
    assert unit.work_spec_name == work_spec['name']
    unit.fail()

    manager.runcmd('failed', ['-W', work_spec['name'], '--details'])
    assert manager.stdout.getvalue().startswith("u'" + unit.key + "': {")

def test_retry_nothing_nothing(manager, work_spec):
    manager.runcmd('retry', ['-W', work_spec['name']])
    # This case never tests whether the work spec exists
    #assert (manager.stdout.getvalue() ==
    #        'Invalid work spec \'' + work_spec['name'] + '\'.\n')
    assert manager.stdout.getvalue() == 'Nothing to do.\n'

def test_retry_a_nothing_nothing(manager, work_spec):
    manager.runcmd('retry', ['-W', work_spec['name'], '-a'])
    # This case gets an empty list for failed work units and runs silently
    #assert (manager.stdout.getvalue() ==
    #        'Invalid work spec \'' + work_spec['name'] + '\'.\n')
    assert manager.stdout.getvalue() == 'Nothing to do.\n'

def test_retry_not_loaded(manager, work_spec):
    manager.runcmd('retry', ['-W', work_spec['name'], 'key-0'])
    assert (manager.stdout.getvalue() ==
            'Invalid work spec \'' + work_spec['name'] + '\'.\n')

def test_retry_nothing(manager, loaded, work_spec):
    manager.runcmd('retry', ['-W', work_spec['name']])
    assert manager.stdout.getvalue() == 'Nothing to do.\n'

def test_retry_all_nothing(manager, loaded, work_spec):
    manager.runcmd('retry', ['-W', work_spec['name'], '-a'])
    assert manager.stdout.getvalue() == 'Nothing to do.\n'

def test_retry_missing_nothing(manager, loaded, work_spec):
    manager.runcmd('retry', ['-W', work_spec['name'], 'low-key'])
    assert manager.stdout.getvalue() == "No such failed work unit 'low-key'.\n"

def test_retry_fail_one_name(manager, worker, task_master, loaded,
                             work_spec, work_units):
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == len(work_units)

    unit = manager.task_master.get_work(worker.worker_id, available_gb=16)
    assert unit.work_spec_name == work_spec['name']
    unit_name = unit.key
    unit.fail()

    assert task_master.num_failed(work_spec['name']) == 1
    assert task_master.num_available(work_spec['name']) == len(work_units)-1

    manager.runcmd('retry', ['-W', work_spec['name'], unit_name])
    assert manager.stdout.getvalue() == ''
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == len(work_units)

def test_retry_fail_one_all(manager, worker, task_master, loaded,
                            work_spec, work_units):
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == len(work_units)

    unit = manager.task_master.get_work(worker.worker_id, available_gb=16)
    assert unit.work_spec_name == work_spec['name']
    unit.fail()

    assert task_master.num_failed(work_spec['name']) == 1
    assert task_master.num_available(work_spec['name']) == len(work_units)-1

    manager.runcmd('retry', ['-W', work_spec['name'], '-a'])
    assert manager.stdout.getvalue() == ''
    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_available(work_spec['name']) == len(work_units)

def test_mode(manager):
    def mode(args, response):
        manager.runcmd('mode', args)
        assert manager.stdout.getvalue() == response + '\n'
        manager.stdout.truncate(0)

    mode([], "IDLE")
    mode(['run'], "set mode to 'run'")
    mode([], "RUN")
    mode(['terminate'], "set mode to 'terminate'")
    mode([], "TERMINATE")
    mode(['idle'], "set mode to 'idle'")
    mode([], "IDLE")
    
    with pytest.raises(SystemExit):
        manager.runcmd('mode', ['other'])

def test_workers_trivial(manager):
    manager.runcmd('workers', [])
    assert manager.stdout.getvalue() == ''

def test_workers_one(manager, worker):
    manager.runcmd('workers', [])
    assert manager.stdout.getvalue() == worker.worker_id + ' (IDLE)\n'

def test_workers_one_all(manager, worker):
    manager.runcmd('workers', ['--all']) # not very interesting
    assert manager.stdout.getvalue() == worker.worker_id + ' (IDLE)\n'

def test_workers_one_details(manager, worker):
    manager.runcmd('workers', ['--details'])
    lines = manager.stdout.getvalue().strip().split('\n')
    assert lines[0] == worker.worker_id + ' (IDLE)'
    assert len(lines) > 1
    for l in lines[1:]:
        assert l.startswith('  ')

def test_run_worker_args(manager, tmpdir):
    with pytest.raises(SystemExit):
        manager.runcmd('run_worker', ['--pidfile', 'foo']) # not absolute
    with pytest.raises(SystemExit):
        manager.runcmd('run_worker', ['--logpath', 'foo'])
    with pytest.raises(SystemExit):
        manager.runcmd('run_worker', ['--pidfile', # missing dir
                                      str(tmpdir.join('missing', 'pidfile'))])
    with pytest.raises(SystemExit):
        manager.runcmd('run_worker', ['--logpath', # missing dir
                                      str(tmpdir.join('missing', 'logpath'))])

def test_run_worker_minimal(manager, tmpdir, global_config):
    # The *only* reliable feedback we can get here is via pidfile.
    # Also, we *must* run this in a subprocess or we'll fail.
    # And, finally, we can't just fork() because of logging concerns.
    pidfile = str(tmpdir.join('pid'))
    logfile = str(tmpdir.join('log'))
    cfgfile = str(tmpdir.join('config.yaml'))
    with open(cfgfile, 'w') as f:
        f.write(yaml.dump(global_config))        
    
    rc = subprocess.call([sys.executable,
                          '-m', 'rejester.run', '-c', cfgfile,
                          'run_worker',
                          '--pidfile', pidfile, '--logpath', logfile])
    assert rc == 0

    pid = None
    for i in xrange(10):
        if os.path.exists(pidfile):
            with open(pidfile, 'r') as f:
                pid = int(f.read())
            break
        time.sleep(0.1)
    assert pid, "pid file never appeared"

    # If this succeeds, the process exists
    os.kill(pid, 0)

    # Attempt a clean shutdown
    try:
        manager.runcmd('mode', ['terminate'])
        for i in xrange(60): # checks every 1-5 seconds
            try:
                os.kill(pid, 0)
            except OSError, e:
                if e.errno == errno.ESRCH: # no such process
                    pid = None
                    break
                raise
            time.sleep(0.1)
        assert pid is None, "worker failed to stop"
    finally:
        if pid is not None:
            os.kill(pid, signal.SIGKILL) # 'kill -9 pid'

def test_run_worker_sigterm(tmpdir, global_config):
    '''same as test_run_worker_minimal but shut down with a signal'''
    # The *only* reliable feedback we can get here is via pidfile.
    # Also, we *must* run this in a subprocess or we'll fail.
    # And, finally, we can't just fork() because of logging concerns.
    pidfile = str(tmpdir.join('pid'))
    logfile = str(tmpdir.join('log'))
    cfgfile = str(tmpdir.join('config.yaml'))
    with open(cfgfile, 'w') as f:
        f.write(yaml.dump(global_config))        
    
    rc = subprocess.call([sys.executable,
                          '-m', 'rejester.run', '-c', cfgfile,
                          'run_worker',
                          '--pidfile', pidfile, '--logpath', logfile])
    assert rc == 0

    pid = None
    for i in xrange(10):
        if os.path.exists(pidfile):
            with open(pidfile, 'r') as f:
                pid = int(f.read())
            break
        time.sleep(0.1)
    assert pid, "pid file never appeared"

    # If this succeeds, the process exists
    os.kill(pid, 0)

    # Shut down with a signal
    try:
        os.kill(pid, signal.SIGTERM) # 'kill pid'
        # This really seems like it should die nearly instantly, but
        # in practice it seems to block on...something
        for i in xrange(100):
            try:
                os.kill(pid, 0)
            except OSError, e:
                if e.errno == errno.ESRCH: # no such process
                    pid = None
                    break
                raise
            time.sleep(0.1)
        assert pid is None, "worker failed to stop"
    finally:
        if pid is not None:
            os.kill(pid, signal.SIGKILL) # 'kill -9 pid'
