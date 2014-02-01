'''
rejester is a task manager written in python

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import sys
import tty
import json
import yaml
import daemon
import termios
import logging
import logging.handlers
import argparse
import lockfile
import traceback

from rejester import TaskMaster
from rejester.workers import run_worker, MultiWorker
from rejester._logging import logger, formatter

def stderr(m, newline='\n'):
    sys.stderr.write(m)
    sys.stderr.write(newline)
    sys.stderr.flush()

def getch():
    '''
    capture one char from stdin for responding to Y/N prompt
    '''
    fd = sys.stdin.fileno()
    if not os.isatty(fd):
        return sys.stdin.read(1)
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


class Manager(object):
    def __init__(self, registry_addresses, app_name, namespace):
        self.config = dict(
            app_name=app_name,
            namespace=namespace,
            registry_addresses=registry_addresses,
        )
        self.task_master = TaskMaster(self.config)

    def _get_work_spec(self, **kwargs):
        work_spec_path  = kwargs.get('work_spec_path')
        if not os.path.exists(work_spec_path):
            sys.exit( 'Path does not exist: %r' % work_spec_path )
        work_spec = yaml.load(open(work_spec_path))
        return work_spec

    def load(self, **kwargs):
        '''loads work_units into a namespace for a given work_spec
        '''
        work_spec = self._get_work_spec(**kwargs)

        work_units_path = kwargs.get('work_units_path')
        if work_units_path == '-':
            work_units_fh = sys.stdin
        elif work_units_path.endswith('.gz'):
            work_units_fh = gzip.open(work_units_path)
        else:
            work_units_fh = open(work_units_path)
        print 'loading work units from %r' % work_units_fh
        work_units = dict()
        for line in work_units_fh:
            work_unit = json.loads(line)
            work_units.update(work_unit)
        print 'pushing work units'
        self.task_master.update_bundle(work_spec, work_units)
        print 'finished writing %d work units to work_spec=%r' \
            % (len(work_units), work_spec['name'])

    def delete(self, **kwargs):
        'make sure the user means to delete'
        namespace = self.config['namespace']
        stderr('Delete everything in %r?  Enter namespace: ' % namespace, newline='')
        if kwargs.get('assume_yes', False):
            stderr('... assuming yes.\n')
            do_delete = True
        else:
            idx = 0
            assert len(namespace) > 0
            while idx < len(namespace):
                ch = getch()
                if ch == namespace[idx]:
                    idx += 1
                    do_delete = True
                else:
                    do_delete = False
                    break

        if do_delete:
            stderr('\nDeleting ...')
            sys.stdout.flush()
            self.task_master.registry.delete_namespace()
            stderr('')

        else:
            stderr(' ... Aborting.')

    def status(self, **kwargs):
        work_spec = self._get_work_spec(**kwargs)
        print json.dumps(self.task_master.status(work_spec['name']), indent=4, sort_keys=True)

    def set_IDLE(self, **kwargs):
        self.task_master.set_mode(self.task_master.IDLE)
        print 'set mode to IDLE'

    def set_RUN(self, **kwargs):
        self.task_master.set_mode(self.task_master.RUN)
        print 'set mode to RUN'

    def set_TERMINATE(self, **kwargs):
        self.task_master.set_mode(self.task_master.TERMINATE)
        print 'set mode to TERMINATE'

    def run_worker(self, **kwargs):
        pidfile = kwargs.get('pidfile')
        logpath = kwargs.get('logpath')
        if not os.path.exists(os.path.dirname(logpath)):
            sys.exit('logpath dir does not exist: %r' % os.path.dirname(logpath))
        if not os.path.exists(os.path.dirname(pidfile)):
            sys.exit('pidfile dir does not exist: %r' % os.path.dirname(pidfile))
        if pidfile:
            pidfile_lock = lockfile.FileLock(pidfile)
        else:
            pidfile_lock = None
        context = daemon.DaemonContext(pidfile=pidfile_lock)
        logger.debug('entering daemon context, pidfile=%r', pidfile)
        with context:
            try:
                open(pidfile,'w').write(str(os.getpid()))
                if logpath:
                    # TODO: do we want byte-size RotatingFileHandler or TimedRotatingFileHandler?
                    handler = logging.handlers.RotatingFileHandler(
                        logpath, maxBytes=10000000, backupCount=3)
                    handler.setFormatter(formatter)
                    logger.addHandler(handler)
                logger.info('inside daemon context')
                run_worker(MultiWorker, self.config)        
                logger.info('run_worker exited')
            except Exception, exc:
                #catastrophe_log = os.path.join(os.path.dirname(logpath), 'rejester-failure-%d.log' % os.getpid())
                #catastrophe_log = os.path.join('/tmp', 'rejester-failure-%d.log' % os.getpid())
                catastrophe_log = os.path.join('/tmp', 'rejester-failure.log')
                open(catastrophe_log, 'wb').write(traceback.format_exc(exc))
                raise
        logger.debug('exited daemon context, pidfile=%r', pidfile)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', help='must be one of: load, delete, status, run')
    parser.add_argument('namespace', help='data namespace in which to execute ACTION')
    parser.add_argument('--app-name', help='app_name, defaults to "rejester"', default='rejester')
    parser.add_argument('--registry-address', action='append', default=[], dest='registry_addresses',
                        help='specify hostname:port for a registry server')
    parser.add_argument('--pidfile', default=None, help='PID lock file for use with action=run')
    parser.add_argument('-y', '--yes', default=False, action='store_true', dest='assume_yes',
                        help='Assume "yes" and require no input for confirmation questions.')
    parser.add_argument('-w', '--work-spec', dest='work_spec_path',
                        help='path to a YAML or JSON file containing a Work Spec')
    parser.add_argument('-u', '--work-units', default='-', dest='work_units_path',
                        help='path to file with one JSON record per line, each describing a Work Unit')
    parser.add_argument('--logpath', default=None)
    args = parser.parse_args()

    ## Split actions by comma, and execute them in sequence
    actions = args.action.split(',')

    #logger.critical(actions)

    for action_string in actions:
        if action_string not in Manager.__dict__:
            sys.exit( 'ACTION=%r is not in %r' % (action_string, Manager.__dict__) )

    if len(args.registry_addresses) == 0:
        ## default to public testing instance
        args.registry_addresses.append( 'redis.diffeo.com:6379' )

    mgr = Manager(args.registry_addresses, args.app_name, args.namespace)

    for action_string in actions:
        logger.critical(action_string)
        action = getattr(mgr, action_string)
        action(**args.__dict__)


if __name__ == '__main__':
    print 'hi'
    main()
