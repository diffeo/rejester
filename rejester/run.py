'''
rejester is a task manager written in python

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
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
import rejester
from rejester._task_master import TaskMaster
from rejester.workers import run_worker, MultiWorker
import yakonfig

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

class MissingArgumentError(Exception):
    """Exception if more information needed to be given at the command line"""
    pass

class Manager(object):
    def __init__(self):
        self.config = yakonfig.get_global_config('rejester')
        self._task_master = None

    @property
    def task_master(self):
        """A `TaskMaster` object for manipulating work"""
        if self._task_master is None:
            self._task_master = TaskMaster(self.config)
        return self._task_master

    def _get_work_spec(self, **kwargs):
        work_spec_path  = kwargs.get('work_spec_path')
        if work_spec_path is None:
            raise MissingArgumentError('give a path to a work spec file '
                                       'with -w')
        if not os.path.exists(work_spec_path):
            raise MissingArgumentError('work spec file {!r} does not exist'
                                       .format(work_spec_path))
        work_spec = yaml.load(open(work_spec_path))
        return work_spec

    def _get_work_spec_name(self, **kwargs):
        name = kwargs.get('work_spec_name')
        if name is not None: return name
        if kwargs.get('work_spec_path') is None:
            raise MissingArgumentError('give a path to a work spec file '
                                       'with -w, or the work spec name '
                                       'with -W')
        work_spec = self._get_work_spec(**kwargs)
        return work_spec['name']

    def load(self, **kwargs):
        '''loads work_units into a namespace for a given work_spec
        '''
        work_spec = self._get_work_spec(**kwargs)

        work_units_path = kwargs.get('work_units_path')
        if work_units_path is None:
            raise MissingArgumentError('give a path to a work unit file '
                                       'with -u')
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
        work_spec_name = self._get_work_spec_name(**kwargs)
        print json.dumps(self.task_master.status(work_spec_name), indent=4, sort_keys=True)

    def work_units(self, **kwargs):
        work_spec_name = self._get_work_spec_name(**kwargs)
        work_units = self.task_master.list_work_units(work_spec_name)
        for k in sorted(work_units.keys()):
            if kwargs.get('verbose'):
                print '{!r}: {!r}'.format(k, work_units[k])
            else:
                print k

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
        if pidfile is not None:
            if not os.path.isabs(pidfile):
                raise MissingArgumentError('--pidfile requires an '
                                           'absolute path')
            if not os.path.exists(os.path.dirname(pidfile)):
                raise MissingArgumentError('--pidfile path {!r} does not exist'
                                           .format(os.path.dirname(pidfile)))

        logpath = kwargs.get('logpath')
        if logpath is not None:
            if not os.path.isabs(logpath):
                raise MissingArgumentError('--logpath requires an '
                                           'absolute path')
            if not os.path.exists(os.path.dirname(logpath)):
                raise MissingArgumentError('--logpath path {!r} does not exist'
                                           .format(os.path.dirname(logpath)))

        if pidfile:
            pidfile_lock = lockfile.FileLock(pidfile)
        else:
            pidfile_lock = None
        context = daemon.DaemonContext(pidfile=pidfile_lock)
        with context:
            try:
                if pidfile:
                    open(pidfile,'w').write(str(os.getpid()))
                # Holding loggers open across DaemonContext is a big
                # problem; establish them for the first time here
                configure_logging(yakonfig.get_global_config())
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
                open(logp, 'a').write(traceback.format_exc(exc))
                raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', help='must be one of: load, delete, status, run')
    parser.add_argument('namespace', help='data namespace in which to execute ACTION')
    parser.add_argument('--pidfile', default=None, help='PID lock file for use with action=run')
    parser.add_argument('-y', '--yes', default=False, action='store_true', dest='assume_yes',
                        help='Assume "yes" and require no input for confirmation questions.')
    parser.add_argument('-w', '--work-spec', dest='work_spec_path',
                        help='path to a YAML or JSON file containing a Work Spec')
    parser.add_argument('-W', '--work-spec-name',
                        help='name of a work spec for queries')
    parser.add_argument('-u', '--work-units', default='-', dest='work_units_path',
                        help='path to file with one JSON record per line, each describing a Work Unit')
    parser.add_argument('--logpath', default=None)
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='include more information in output')
    args = yakonfig.parse_args(parser, [yakonfig, rejester])

    # Split actions by comma, and execute them in sequence
    actions = args.action.split(',')
    for action_string in actions:
        if action_string not in Manager.__dict__:
            parser.error('Unrecognized action "{}"'.format(action_string))

    # run_worker is Very Special; anything else needs to set up logging now
    if 'run_worker' not in actions:
        configure_logging(yakonfig.get_global_config())

    mgr = Manager()

    for action_string in actions:
        action = getattr(mgr, action_string)
        try:
            action(**args.__dict__)
        except MissingArgumentError, e:
            parser.error('{}: {!s}'.format(action_string, e))

if __name__ == '__main__':
    main()
