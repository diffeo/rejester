'''Command-line :mod:`rejester` management tool.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

This provides a centralized user interface to explore and affect
various parts of the :mod:`rejester` system.  Of note, this is the
principal way to start a worker process to run jobs.  Typical use is:

1. Set up a configuration file containing rejester configuration,
   such as:

    .. code-block:: yaml

        rejester:
          app_name: rejester
          namespace: mynamespace
          registry_addresses: [ "redis.example.com:6379" ]

2. Run some process to generate rejester tasks.

3. Copy the configuration files to other systems as required, and
   run ``rejester -c config.yaml run_worker`` to start workers.

4. Run (anywhere, but only once) ``rejester -c config.yaml mode run``
   to actually start execution.

5. Wait for the work units to finish.

6. Run (anywhere, but only once) ``rejester -c config.yaml mode terminate``
   to ask the workers to shut down.

.. program:: rejester

The :program:`rejester` tool supports the standard
:option:`--config <yakonfig --config>`,
:option:`--dump-config <yakonfig --dump-config>`,
:option:`--verbose <dblogger --verbose>`,
:option:`--quiet <dblogger --quiet>`, and
:option:`--debug <dblogger --debug>` options.  This, and any other tool
that integrates with :mod:`rejester`, supports the following additional
options:

.. option:: --app-name <name>

Provide the application name for database access.  This is combined
with the namespace string.

.. option:: --namespace <name>

Provide the namespace name.  This is qualified by the application
name.  All work units and workers are associated with a single
namespace in a single application.

.. option:: --registry-address <host:port>

Provide the location of a Redis server.

If no further options are given, start an interactive shell to monitor
and control :mod:`rejester`.  Alternatively, a single command can be
given on the command line.  The tool provides the following commands:

.. describe:: load --work-spec file.yaml --work-units file2.json

    Loads a set of work units.  The work spec (``-w``) and work units
    (``-u``) must both be provided as external files.  The work spec
    file is the YAML serialization of a work spec definition; see
    :class:`rejester.TaskMaster` for details of what this looks like.
    The work unit file is a series of JSON records, one to a line,
    each of which is a dictionary of a single ``{"key": {"unit":
    "definition", "dictionary": "values"}}``.

.. describe:: delete

    Deletes the entire namespace.  Prompts for confirmation, unless
    ``-y`` or ``--yes`` is given as an argument.

.. describe:: work_spec --work-spec-name name

    Prints out the definition of a work spec, assuming it has already
    been loaded.  The work spec name may be given with a
    ``--work-spec-name`` or ``-W`` option; or, a ``--work-spec`` or
    ``-w`` option may name a work spec file compatible with the
    ``load`` command.

.. describe:: status --work-spec-name name

    Prints out a summary of the jobs in some work spec.  Provide the
    work spec name the same way as for the ``work_spec`` command.

.. describe:: work_units --work-spec-name name

    Prints out a listing of the work units that have not yet completed
    for some work spec.  Provide the work spec name the same way as
    for the ``work_spec`` command.  This includes the work units that
    the ``status`` command would report as "available" or "pending",
    but not other statuses.  If ``--details`` is given as an argument,
    print the definition of the work unit along with its name.

.. describe:: failed --work-spec-name name

    The same as ``work_units``, but only prints out "failed" work
    units.  ``failed -W name --details`` should include a traceback
    indicating why the work unit failed.

.. describe:: mode [idle|run|terminate]

    With no arguments, print out the current rejester mode; otherwise
    set it.  In "run" mode, workers will start new jobs as they become
    available.  In "idle" mode, workers will not start new jobs but
    also will continue to execute; if the mode is switched back to
    "run" they will start running jobs again.  In "terminate" mode,
    workers will stop execution as soon as they finish their running
    jobs.

.. describe:: workers

    List all of the known workers.  With ``--all`` include workers
    that haven't checked in recently.  With ``--details`` include all
    known details.

.. describe:: run_worker [--pidfile /path/to/file.pid] [--logpath /path/to/file.log]

    Start a worker as a background task.  This should generally not be
    run from the interactive shell.  If ``--pidfile`` is specified,
    the process ID of the worker is written to the named file, which
    must be an absolute path.  If ``--logpath`` is specified, log
    messages from the worker will be written to the specified file,
    which again must be an absolute path; this is in addition to any
    logging specified in the configuration file.  The worker may be
    shut down by globally switching to ``mode terminate``, or by
    ``kill $(cat /path/to/file.pid)``.

'''
from __future__ import absolute_import
import argparse
import json
import lockfile
import logging
import logging.config
import os
import sys
import traceback

import daemon
import yaml

import dblogger
import rejester
from rejester.exceptions import NoSuchWorkSpecError, NoSuchWorkUnitError
from rejester._task_master import TaskMaster
from rejester.workers import run_worker, MultiWorker
import yakonfig
from yakonfig.cmd import ArgParseCmd

logger = logging.getLogger(__name__)

def existing_path(string):
    '''"Convert" a string to a string that is a path to an existing file.'''
    if not os.path.exists(string):
        msg = 'path {!r} does not exist'.format(string)
        raise argparse.ArgumentTypeError(msg)
    return string

def existing_path_or_minus(string):
    '''"Convert" a string like :func:`existing_path`, but also accept "-".'''
    if string == '-': return string
    return existing_path(string)

def absolute_path(string):
    '''"Convert" a string to a string that is an absolute existing path.'''
    if not os.path.isabs(string):
        msg = '{!r} is not an absolute path'.format(string)
        raise argparse.ArgumentTypeError(msg)
    if not os.path.exists(os.path.dirname(string)):
        msg = 'path {!r} does not exist'.format(string)
        raise argparse.ArgumentTypeError(msg)
    return string

class Manager(ArgParseCmd):
    def __init__(self):
        ArgParseCmd.__init__(self)
        self._config = None
        self._task_master = None

    @property
    def config(self):
        if self._config is None:
            self._config = yakonfig.get_global_config('rejester')
        return self._config

    @property
    def task_master(self):
        """A `TaskMaster` object for manipulating work"""
        if self._task_master is None:
            self._task_master = TaskMaster(self.config)
        return self._task_master

    def _add_work_spec_args(self, parser):
        '''Add ``--work-spec`` to an :mod:`argparse` `parser`.'''
        parser.add_argument('-w', '--work-spec', dest='work_spec_path',
                            metavar='FILE', type=existing_path,
                            required=True,
                            help='path to a YAML or JSON file')
    def _get_work_spec(self, args):
        '''Get the contents of the work spec from the arguments.'''
        with open(args.work_spec_path) as f:
            return yaml.load(f)

    def _add_work_spec_name_args(self, parser):
        '''Add either ``--work-spec`` or ``--work-spec-name`` to an
        :mod:`argparse` `parser`.'''
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-w', '--work-spec', dest='work_spec_path',
                           metavar='FILE', type=existing_path,
                           help='path to a YAML or JSON file')
        group.add_argument('-W', '--work-spec-name', metavar='NAME',
                           help='name of a work spec for queries')

    def _get_work_spec_name(self, args):
        '''Get the name of the work spec from the arguments.

        This assumes :meth:`_add_work_spec_name_args` has been called,
        but will also work if just :meth:`_add_work_spec_args` was
        called instead.

        '''
        if getattr(args, 'work_spec_name', None):
            return args.work_spec_name
        return self._get_work_spec(args)['name']

    def args_load(self, parser):
        self._add_work_spec_args(parser)
        parser.add_argument('-u', '--work-units', metavar='FILE',
                            dest='work_units_path', required=True,
                            type=existing_path_or_minus,
                            help='path to file with one JSON record per line')
    def do_load(self, args):
        '''loads work_units into a namespace for a given work_spec'''
        work_spec = self._get_work_spec(args)
        if args.work_units_path == '-':
            work_units_fh = sys.stdin
        elif args.work_units_path.endswith('.gz'):
            work_units_fh = gzip.open(args.work_units_path)
        else:
            work_units_fh = open(args.work_units_path)
        self.stdout.write('loading work units from {!r}\n'
                          .format(work_units_fh))
        work_units = dict()
        for line in work_units_fh:
            work_unit = json.loads(line)
            work_units.update(work_unit)
        self.stdout.write('pushing work units\n')
        self.task_master.update_bundle(work_spec, work_units)
        self.stdout.write('finished writing {} work units to work_spec={!r}\n'
                          .format(len(work_units), work_spec['name']))

    def args_delete(self, parser):
        parser.add_argument('-y', '--yes', default=False, action='store_true',
                            dest='assume_yes',
                            help='assume "yes" and require no input for '
                            'confirmation questions.')
    def do_delete(self, args):
        '''delete the entire contents of the current namespace'''
        namespace = self.config['namespace']
        if not args.assume_yes:
            response = raw_input('Delete everything in {!r}?  Enter namespace: '
                                 .format(namespace))
            if response != namespace:
                self.stdout.write('not deleting anything\n')
                return
        self.stdout.write('deleting namespace {!r}\n'.format(namespace))
        self.task_master.registry.delete_namespace()

    def args_work_spec(self, parser):
        self._add_work_spec_name_args(parser)
    def do_work_spec(self, args):
        '''dump the contents of an existing work spec'''
        work_spec_name = self._get_work_spec_name(args)
        spec = self.task_master.get_work_spec(work_spec_name)
        self.stdout.write(json.dumps(spec, indent=4, sort_keys=True) + '\n')

    def args_status(self, parser):
        self._add_work_spec_name_args(parser)
    def do_status(self, args):
        '''print the number of work units in an existing work spec'''
        work_spec_name = self._get_work_spec_name(args)
        status = self.task_master.status(work_spec_name)
        self.stdout.write(json.dumps(status, indent=4, sort_keys=True) + '\n')

    def args_work_units(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-n', '--limit', type=int, metavar='N',
                            help='only print N work units')
        parser.add_argument('--details', action='store_true',
                            help='also print the contents of the work units')
    def do_work_units(self, args):
        '''list work units that have not yet completed'''
        work_spec_name = self._get_work_spec_name(args)
        work_units = self.task_master.list_work_units(work_spec_name)
        work_unit_names = sorted(work_units.keys())
        if args.limit: work_unit_names = work_unit_names[:args.limit]
        for k in work_unit_names:
            if args.details:
                self.stdout.write('{!r}: {!r}\n'.format(k, work_units[k]))
            else:
                self.stdout.write('{}\n'.format(k))

    def args_failed(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-n', '--limit', type=int, metavar='N',
                            help='only print N work units')
        parser.add_argument('--details', action='store_true',
                            help='also print the contents of the work units')
    def do_failed(self, args):
        '''list failed work units'''
        work_spec_name = self._get_work_spec_name(args)
        work_units = self.task_master.list_failed_work_units(work_spec_name)
        work_unit_names = sorted(work_units.keys())
        if args.limit: work_unit_names = work_unit_names[:args.limit]
        for k in work_unit_names:
            if args.details:
                self.stdout.write('{!r}: {!r}\n'.format(k, work_units[k]))
            else:
                self.stdout.write('{}\n'.format(k))

    def args_retry(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-a', '--all', action='store_true',
                            help='retry all failed jobs')
        parser.add_argument('unit', nargs='*',
                            help='work unit name(s) to retry')
    def do_retry(self, args):
        '''retry a specific failed job'''
        work_spec_name = self._get_work_spec_name(args)
        if args.all:
            try:
                units = self.task_master.list_failed_work_units(work_spec_name)
            # NB: rejester never actually raises this exception, we get
            # an empty "units" list instead
            except NoSuchWorkSpecError, e:
                self.stdout.write('Invalid work spec {!r}.\n'
                                  .format(work_spec_name))
                return
        else:
            units = args.unit
        if not units:
            self.stdout.write('Nothing to do.\n')
            return
        for unit in units:
            try:
                self.task_master.retry(work_spec_name, unit)
            except NoSuchWorkSpecError, e:
                self.stdout.write('Invalid work spec {!r}.\n'
                                  .format(work_spec_name))
                return
            except NoSuchWorkUnitError, e:
                self.stdout.write('No such failed work unit {!r}.\n'
                                  .format(unit))
                # and continue to the next unit

    def args_mode(self, parser):
        parser.add_argument('mode', choices=['idle', 'run', 'terminate'],
                            nargs='?',
                            help='set rejester worker mode to MODE')
    def do_mode(self, args):
        '''get or set the global rejester worker mode'''
        if args.mode:
            mode = { 'idle': self.task_master.IDLE,
                     'run': self.task_master.RUN,
                     'terminate': self.task_master.TERMINATE }[args.mode]
            self.task_master.set_mode(mode)
            self.stdout.write('set mode to {!r}\n'.format(args.mode))
        else:
            mode = self.task_master.get_mode()
            self.stdout.write('{!s}\n'.format(mode))

    def args_workers(self, parser):
        parser.add_argument('--all', action='store_true',
                            help='list all workers (even dead ones)')
        parser.add_argument('--details', action='store_true',
                            help='include more details if available')
    def do_workers(self, args):
        '''list all known workers'''
        workers = self.task_master.workers(alive=not args.all)
        for k in sorted(workers.iterkeys()):
            self.stdout.write('{} ({})\n'.format(k, workers[k]))
            if args.details:
                heartbeat = self.task_master.get_heartbeat(k)
                for hk, hv in heartbeat.iteritems():
                    self.stdout.write('  {}: {}\n'.format(hk, hv))

    def args_run_worker(self, parser):
        parser.add_argument('--pidfile', metavar='FILE', type=absolute_path,
                            help='file to hold process ID of worker')
        parser.add_argument('--logpath', metavar='FILE', type=absolute_path,
                            help='file to receive local logs')
    def do_run_worker(self, args):
        '''run a rejester worker as a background process'''
        pidfile = args.pidfile
        logpath = args.logpath

        # Shut off all logging...it can cause problems
        logging.config.dictConfig({
            'version': 1,
        })
        gconfig = yakonfig.get_global_config()
        yakonfig.clear_global_config()
        # Don't try to log to the console or debug if set
        if 'logging' in gconfig:
            if 'root' in gconfig['logging']:
                if 'handlers' in gconfig['logging']['root']:
                    handlers = gconfig['logging']['root']['handlers']
                    for handler in 'console', 'debug':
                        if handler in handlers:
                            handlers.remove(handler)

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
                run_worker(MultiWorker, self.config)
                logger.debug('run_worker exited')
            except Exception, exc:
                logp = logpath or os.path.join('/tmp', 'rejester-failure.log')
                open(logp, 'a').write(traceback.format_exc(exc))
                raise


def main():
    parser = argparse.ArgumentParser(
        description='manage the rejester distributed work system')
    mgr = Manager()
    mgr.add_arguments(parser)
    args = yakonfig.parse_args(parser, [yakonfig, dblogger, rejester])
    mgr.main(args)

if __name__ == '__main__':
    main()
