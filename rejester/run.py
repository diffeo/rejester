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

.. describe:: summary

    Print a tabular listing of all of the running work specs and
    how many work units are in each state.

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

.. describe:: work_specs

    Prints out the names of all of the work specs.

.. describe:: work_spec --work-spec-name name

    Prints out the definition of a work spec, assuming it has already
    been loaded.  The work spec name may be given with a
    ``--work-spec-name`` or ``-W`` option; or, a ``--work-spec`` or
    ``-w`` option may name a work spec file compatible with the
    ``load`` command.

.. describe:: status --work-spec-name name

    Prints out a summary of the jobs in some work spec.  Provide the
    work spec name the same way as for the ``work_spec`` command.

.. describe:: work_units --work-spec-name name [--status status]

    Prints out a listing of the work units that have not yet completed
    for some work spec.  Provide the work spec name the same way as
    for the ``work_spec`` command.  This includes the work units that
    the ``status`` command would report as "available" or "pending",
    but not other statuses.  If ``-s`` or ``--status`` is given with
    one of the status strings "available", "pending", "blocked",
    "failed", or "finished", only print work units with that status.
    If ``--details`` is given as an argument, print the definition of
    the work unit (and the traceback for failed work units) along with
    its name.

.. describe:: failed --work-spec-name name

    Identical to ``work_units --status failed``.  May be removed at
    a future time.

.. describe:: work_unit --work-spec-name name unitname

    Prints out basic details for a work unit, in any state.

.. describe:: retry --work-spec-name name [--all|unitname...]

    Retry failed work units, removing their traceback and moving them
    back to "available" status.  If ``-a`` or ``--all`` is given,
    retry all failed work units; otherwise, only retry the specific
    work units named on the command line.

.. describe:: clear --work-spec-name name [--status status] [unitname...]

    Remove work units from the system.  With no additional arguments,
    remove all work units from the specified work spec.  If ``-s`` or
    ``--status`` is given, remove only work units with this status;
    see the description of the `work_units` subcommand for possible
    values.  If any ``unitname`` values are given, only remove those
    specific work units, provided they in fact have the specified
    status.

.. describe:: mode [idle|run|terminate]

    With no arguments, print out the current rejester mode; otherwise
    set it.  In "run" mode, workers will start new jobs as they become
    available.  In "idle" mode, workers will not start new jobs but
    also will continue to execute; if the mode is switched back to
    "run" they will start running jobs again.  In "terminate" mode,
    workers will stop execution as soon as they finish their running
    jobs.

.. describe:: run_one

    Get a single task, and run it.

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
import time
import traceback

import daemon
import yaml

import dblogger
import rejester
from rejester.exceptions import NoSuchWorkSpecError, NoSuchWorkUnitError
from rejester._task_master import TaskMaster
from rejester.workers import run_worker, MultiWorker, SingleWorker
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
        self.prompt = 'rejester> '
        self._config = None
        self._task_master = None
        self.exitcode = 0

    def precmd(self, line):
        self.exitcode = 0
        return ArgParseCmd.precmd(self, line)

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

    def args_work_specs(self, parser):
        pass
    def do_work_specs(self, args):
        '''print the names of all of the work specs'''
        for name in sorted(self.task_master.list_work_specs().keys()):
            self.stdout.write('{}\n'.format(name))

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

    def args_summary(self, parser):
        pass
    def do_summary(self, args):
        '''print a summary of running rejester work'''
        self.stdout.write('Work spec               Avail  Pending  Blocked'
                          '   Failed Finished    Total\n')
        self.stdout.write('==================== ======== ======== ========'
                          ' ======== ======== ========\n')
        for name in sorted(self.task_master.list_work_specs().keys()):
            status = self.task_master.status(name)
            self.stdout.write('{0:20s} {1[num_available]:8d} '
                              '{1[num_pending]:8d} {1[num_blocked]:8d} '
                              '{1[num_failed]:8d} {1[num_finished]:8d} '
                              '{1[num_tasks]:8d}\n'.format(name, status))

    def args_work_units(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-n', '--limit', type=int, metavar='N',
                            help='only print N work units')
        parser.add_argument('-s', '--status',
                            choices=['available', 'pending', 'blocked',
                                     'finished', 'failed'],
                            help='print work units in STATUS')
        parser.add_argument('--details', action='store_true',
                            help='also print the contents of the work units')
    def do_work_units(self, args):
        '''list work units that have not yet completed'''
        work_spec_name = self._get_work_spec_name(args)
        fns = { 'available': self.task_master.list_available_work_units,
                'pending': self.task_master.list_pending_work_units,
                'blocked': self.task_master.list_blocked_work_units,
                'finished': self.task_master.list_finished_work_units,
                'failed': self.task_master.list_failed_work_units }
        fn = fns.get(args.status, self.task_master.list_work_units)
        work_units = fn(work_spec_name, limit=args.limit)
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
        '''list failed work units (deprecated)'''
        args.status = 'failed'
        return self.do_work_units(args)

    def args_work_unit(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('unit', nargs='*',
                            help='work unit name(s)')
    def do_work_unit(self, args):
        '''print basic details about work units'''
        work_spec_name = self._get_work_spec_name(args)
        for work_unit_name in args.unit:
            status = self.task_master.get_work_unit_status(work_spec_name,
                                                           work_unit_name)
            self.stdout.write('{} ({})\n'
                              .format(work_unit_name, status['status']))
            if 'expiration' in status:
                when = time.ctime(status['expiration'])
                if status == 'available':
                    if status['expiration'] == 0:
                        self.stdout.write('  Never scheduled\n')
                    else:
                        self.stdout.write('  Available since: {}\n'
                                          .format(when))
                else:
                    self.stdout.write('  Expires: {}\n'.format(when))
            if 'worker_id' in status:
                heartbeat = self.task_master.get_heartbeat(status['worker_id'])
                if heartbeat:
                    hostname = (heartbeat.get('fqdn', None) or
                                heartbeat.get('hostname', None) or
                                '')
                    ipaddrs = ', '.join(heartbeat.get('ipaddrs', ()))
                    if hostname and ipaddrs:
                        summary = '{} on {}'.format(hostname, ipaddrs)
                    else:
                        summary = hostname + ipaddrs
                else:
                    summary = 'No information'
                self.stdout.write('  Worker: {} ({})\n'.format(
                    status['worker_id'], summary))
            if 'traceback' in status:
                self.stdout.write('  Traceback:\n{}\n'.format(
                    status['traceback']))
            if 'depends_on' in status:
                self.stdout.write('  Depends on:\n')
                for what in status['depends_on']:
                    self.stdout.write('    {!r}\n'.format(what))

    def args_retry(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-a', '--all', action='store_true',
                            help='retry all failed jobs')
        parser.add_argument('unit', nargs='*',
                            help='work unit name(s) to retry')
    def do_retry(self, args):
        '''retry a specific failed job'''
        work_spec_name = self._get_work_spec_name(args)
        retried = 0
        complained = False
        try:
            if args.all:
                while True:
                    units = self.task_master.list_failed_work_units(
                        work_spec_name, limit=1000)
                    if not units: break
                    try:
                        self.task_master.retry(work_spec_name, *units)
                        retried += len(units)
                    except NoSuchWorkUnitError, e:
                        # Because of this sequence, this probably means
                        # something else retried the work unit.  If we
                        # try again, we shouldn't see it in the failed
                        # list...so whatever
                        pass
            else:
                units = args.unit
                try:
                    self.task_master.retry(work_spec_name, *units)
                    retried += len(units)
                except NoSuchWorkUnitError, e:
                    unit = e.work_unit_name
                    self.stdout.write('No such failed work unit {!r}.\n'
                                      .format(unit))
                    complained = True
                    units.remove(unit)
                    # and try again
        except NoSuchWorkSpecError, e:
            # NB: you are not guaranteed to get this, especially with --all
            self.stdout.write('Invalid work spec {!r}.\n'
                              .format(work_spec_name))
            return
        if retried == 0 and not complained:
            self.stdout.write('Nothing to do.\n')
        elif retried == 1:
            self.stdout.write('Retried {} work unit.\n'.format(retried))
        elif retried > 1:
            self.stdout.write('Retried {} work units.\n'.format(retried))

    def args_clear(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-s', '--status',
                            choices=['available', 'pending', 'blocked',
                                     'finished', 'failed'],
                            help='print work units in STATUS')
        parser.add_argument('unit', nargs='*',
                            help='work unit name(s) to remove')
    def do_clear(self, args):
        '''remove work units from a work spec'''
        # Which units?
        work_spec_name = self._get_work_spec_name(args)
        units = args.unit or None
        # What to do?
        count = 0
        if args.status == 'available' or args.status is None:
            count += self.task_master.remove_available_work_units(
                work_spec_name, units)
        if args.status == 'pending' or args.status is None:
            count += self.task_master.remove_pending_work_units(
                work_spec_name, units)
        if args.status == 'blocked' or args.status is None:
            count += self.task_master.remove_blocked_work_units(
                work_spec_name, units)
        if args.status == 'finished' or args.status is None:
            count += self.task_master.remove_finished_work_units(
                work_spec_name, units)
        if args.status == 'failed' or args.status is None:
            count += self.task_master.remove_failed_work_units(
                work_spec_name, units)
        self.stdout.write('Removed {} work units.\n'.format(count))

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

    def args_run_one(self, parser):
        pass
    def do_run_one(self, args):
        '''run a single job'''
        worker = SingleWorker(self.config)
        worker.register()
        rc = False
        try:
            rc = worker.run()
        finally:
            worker.unregister()
        if not rc:
            self.exitcode = 2

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
            'disable_existing_loggers': False
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
    sys.exit(mgr.exitcode)

if __name__ == '__main__':
    main()
