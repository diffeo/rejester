'''
rejester is a task manager written in python

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import argparse
import json
import lockfile
import logging
import os
from StringIO import StringIO
import sys
import termios
import traceback
import tty

import daemon
import yaml

from dblogger import configure_logging, FixedWidthFormatter
from rejester import TaskMaster
from rejester.workers import run_worker, MultiWorker
from yakonfig import set_global_config

logger = logging.getLogger(__name__)

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
    def __init__(self, config):
        self.config = config
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
                    formatter = FixedWidthFormatter()
                    # TODO: do we want byte-size RotatingFileHandler or TimedRotatingFileHandler?
                    handler = logging.handlers.RotatingFileHandler(
                        logpath, maxBytes=10000000, backupCount=3)
                    handler.setFormatter(formatter)
                    logging.getLogger('').addHandler(handler)
                logger.debug('inside daemon context')
                run_worker(MultiWorker, self.config)        
                logger.debug('run_worker exited')
            except Exception, exc:
                logp = logpath or os.path.join('/tmp', 'rejester-failure.log')
                open(logp, 'w').write(traceback.format_exc(exc))
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
    parser.add_argument('-c', '--config', default=None, metavar='config.yaml',
                        help='path to configuration file')
    args = parser.parse_args()

    # If we were given a config file, load it
    if args.config is not None:
        config = set_global_config(path=args.config)
    else:
        config = set_global_config(stream=StringIO('{}'))

    # Fill in more config options from args
    rejester_config = config.setdefault('rejester', {})
    if args.app_name is not None: rejester_config['app_name'] = args.app_name
    if args.registry_addresses != []:
        rejester_config['registry_addresses'] = args.registry_addresses
    rejester_config['namespace'] = args.namespace

    # Default values
    rejester_config.setdefault('app_name', 'rejester')
    rejester_config.setdefault('registry_addresses', ['redis.diffeo.com:6379'])

    configure_logging(config)

    # Split actions by comma, and execute them in sequence
    actions = args.action.split(',')
    for action_string in actions:
        if action_string not in Manager.__dict__:
            stderr('Unrecognized action "{}"'.format(action_string))
            return

    mgr = Manager(rejester_config)

    for action_string in actions:
        logger.info('Running action "{}"'.format(action_string))
        action = getattr(mgr, action_string)
        action(**args.__dict__)


if __name__ == '__main__':
    main()
