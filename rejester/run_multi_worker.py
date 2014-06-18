'''Command-line :mod:`rejester_worker` tool for launching the
MultiWorker daemon.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. option:: --pidfile /path/to/file.pid

    If ``--pidfile`` is specified, the process ID of the worker is
    written to the named file, which must be an absolute path.

.. option:: --logpath /path/to/file.log

    If ``--logpath`` is specified, log messages from the worker will
    be written to the specified file, which again must be an absolute
    path; this is in addition to any logging specified in the
    configuration file.

 Start a worker as a background task.  The worker may be shut down by
 globally switching to ``mode terminate``, or by ``kill $(cat
 /path/to/file.pid)``.

'''
from __future__ import absolute_import
import argparse
import lockfile
import logging
import os
import sys
import time
import traceback

import daemon

import dblogger
import rejester
from rejester.exceptions import NoSuchWorkSpecError, NoSuchWorkUnitError
from rejester._task_master import TaskMaster
from rejester.run import absolute_path
from rejester.workers import run_worker, MultiWorker, SingleWorker
import yakonfig

logger = logging.getLogger(__name__)

def args_run_worker(parser):
    parser.add_argument('--pidfile', metavar='FILE', type=absolute_path,
                        help='file to hold process ID of worker')
    parser.add_argument('--logpath', metavar='FILE', type=absolute_path,
                        help='file to receive local logs')

def fork_worker(args):
    pidfile = args.pidfile
    logpath = args.logpath

    gconfig = yakonfig.get_global_config()
    if pidfile:
        pidfile_lock = lockfile.FileLock(pidfile)
    else:
        pidfile_lock = None
    context = daemon.DaemonContext(pidfile=pidfile_lock)
    with context:
        try:
            if pidfile:
                open(pidfile,'w').write(str(os.getpid()))
            # Reestablish loggers
            yakonfig.set_default_config([dblogger], config=gconfig)
            if logpath:
                formatter = dblogger.FixedWidthFormatter()
                # TODO: do we want byte-size RotatingFileHandler or TimedRotatingFileHandler?
                handler = logging.handlers.RotatingFileHandler(
                    logpath, maxBytes=10000000, backupCount=3)
                handler.setFormatter(formatter)
                logging.getLogger('').addHandler(handler)
            logger.debug('inside daemon context')
            config = gconfig['rejester']
            run_worker(MultiWorker, config)
            logger.debug('run_worker exited')
        except Exception, exc:
            logp = logpath or os.path.join('/tmp', 'rejester-failure.log')
            open(logp, 'a').write(traceback.format_exc(exc))
            raise

def main():
    parser = argparse.ArgumentParser(
        description='manage the rejester distributed work system')
    args_run_worker(parser)
    args = yakonfig.parse_args(parser, [yakonfig, rejester])
    fork_worker(args)


if __name__ == '__main__':
    main()
