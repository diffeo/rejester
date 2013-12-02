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
import daemon
import termios
import logging
import logging.handlers
import argparse

from rejester.workers import run_worker, MultiWorker
from rejester._logging import logger

def stderr(m, newline='\n'):
    sys.stderr.write(m)
    sys.stderr.write(newline)
    sys.stderr.flush()

def getch():
    '''
    capture one char from stdin for responding to Y/N prompt
    '''
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


class Manager(object):
    def __init__(self, namespace, registry_addresses):
        self.config = dict(namespace=namespace,
                           registry_addresses=registry_addresses)

    def load(self, **kwargs):
        if self.args.loaddatapaths:
            input_paths = self.args.loaddatapaths
        elif self.args.input == '-':
            input_paths = sys.stdin
        else:
            input_paths = open(self.args.input, 'r')
        ## all forms of input_paths are iterable
        #do loading

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
            TreeStorage(self.config['bigtree']).delete_namespace()
            bigtree.storage.cleanup(self.config['bigtree'])
            stderr('')

        else:
            stderr(' ... Aborting.')

    def status(self, **kwargs):
        pass

    def run(self, **kwargs):
        pidfile = kwargs.get('pidfile')
        handler = logging.handlers.SysLogHandler(address = '/var/log/rejester')
        logger.addHandler(handler)
        logger.critical('entering daemon context')
        #with daemon.DaemonContext(pidfile=pidfile):
        if 1:
            open(pidfile).write(os.getpid())
            logger.critical('inside daemon context')
            run_worker(MultiWorker, self.config)        


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', help='must be one of: load, delete, status, run')
    parser.add_argument('namespace', help='data namespace in which to execute ACTION')
    parser.add_argument('--registry-address', action='append', default=[], dest='registry_addresses',
                        help='specify hostname:port for a registry server')
    parser.add_argument('--pidfile', default=None, help='PID lock file for use with action=run')
    parser.add_argument('-y', '--yes', default=False, action='store_true', dest='assume_yes',
                        help='Assume "yes" and require no input for confirmation questions.')
    parser.add_argument('-w', '--work-spec', 
                        help='path to a YAML or JSON file containing a Work Spec')
    parser.add_argument('-u', '--work-units', default='-',
                        help='path to file with one JSON record per line, each describing a Work Unit')
    parser.add_argument('--loaddata', default=[], action='append', dest='loaddatapaths',
                        help='paths to files with data entities in them [may be repeated]')
    args = parser.parse_args()

    ## Split actions by comma, and execute them in sequence
    actions = args.action.split(',')

    logger.critical(actions)
    print actions

    for action_string in actions:
        if action_string not in Manager.__dict__:
            sys.exit( 'ACTION=%r is not in %r' % (action_string, Manager.__dict__) )

    if len(args.registry_addresses) == 0:
        ## default to public testing instance
        args.registry_addresses.append( 'redis.diffeo.com:6379' )

    mgr = Manager(args.namespace, args.registry_addresses)

    for action_string in actions:
        logger.critical(action_string)
        action = getattr(mgr, action_string)
        action(**args.__dict__)


if __name__ == '__main__':
    print 'hi'
    main()
