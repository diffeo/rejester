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
   run ``rejester_worker -c config.yaml`` to start workers.

4. Run (anywhere, but only once) ``rejester -c config.yaml mode run``
   to actually start execution.

5. Wait for the work units to finish.

6. Run (anywhere, but only once) ``rejester -c config.yaml mode terminate``
   to ask the workers to shut down.

In the configuration, ``namespace`` and ``registry_addresses`` are
required unless passed on the command line.  Additionally,
``registry_addresses`` can be detected from environment variables
``REDIS_PORT_6379_TCP_ADDR`` and ``REDIS_PORT_6379_TCP_PORT``,
which will be set by `Docker <http://www.docker.com/>`_ if a
rejester program is run in a container that is ``--link`` connected
to another container with the name ``redis``.

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

.. describe:: global_lock [--purge]

    The system maintains a global lock to make sequences of database
    requests atomic, but if workers fail, the global lock can be leaked.
    With no arguments, prints the worker ID that owns the global lock
    and details that are known about it.  If ``-p`` or ``--purge`` is
    given, clear out the lock if anybody holds it.

.. describe:: workers

    List all of the known workers.  With ``--all`` include workers
    that haven't checked in recently.  With ``--details`` include all
    known details.

'''
from __future__ import absolute_import
import argparse
import gzip
import json
import logging
import os
import pprint
import sys
import time

import yaml

import dblogger
import rejester
from rejester.exceptions import NoSuchWorkSpecError, NoSuchWorkUnitError
from rejester._task_master import build_task_master
from rejester.workers import SingleWorker
import yakonfig
from yakonfig.cmd import ArgParseCmd

logger = logging.getLogger(__name__)

def existing_path(string):
    '''"Convert" a string to a string that is a path to an existing file.'''
    if not os.path.exists(string):
        msg = 'path {0!r} does not exist'.format(string)
        raise argparse.ArgumentTypeError(msg)
    return string

def existing_path_or_minus(string):
    '''"Convert" a string like :func:`existing_path`, but also accept "-".'''
    if string == '-': return string
    return existing_path(string)

def absolute_path(string):
    '''"Convert" a string to a string that is an absolute existing path.'''
    if not os.path.isabs(string):
        msg = '{0!r} is not an absolute path'.format(string)
        raise argparse.ArgumentTypeError(msg)
    if not os.path.exists(os.path.dirname(string)):
        msg = 'path {0!r} does not exist'.format(string)
        raise argparse.ArgumentTypeError(msg)
    return string

class Manager(ArgParseCmd):
    def __init__(self):
        ArgParseCmd.__init__(self)
        self.prompt = 'rejester> '
        # storage for lazy config getter property self.config
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
            self._task_master = build_task_master(self.config)
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

    def _read_work_units_file(self, work_units_fh):
        '''work_units_fh is iterable on lines (could actually be ['{}',...])
        '''
        count = 0
        for line in work_units_fh:
            if not line:
                continue
            line = line.strip()
            if not line:
                continue
            if line[0] == '#':
                continue
            try:
                count += 1
                work_unit = json.loads(line)
                #work_units.update(work_unit)
                for k,v in work_unit.iteritems():
                    yield k,v
            except:
                logger.error('failed handling work_unit on line %s: %r', count, line, exc_info=True)
                raise

    def _work_units_fh_from_path(self, work_units_path):
        work_units_fh = None
        if work_units_path == '-':
            work_units_fh = sys.stdin
        elif work_units_path is not None:
            if work_units_path.endswith('.gz'):
                work_units_fh = gzip.open(work_units_path)
            else:
                work_units_fh = open(work_units_path)
        return work_units_fh

    def args_load(self, parser):
        self._add_work_spec_args(parser)
        parser.add_argument('-n', '--nice', default=0, type=int,
                            help='specify a nice level for these jobs')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-u', '--work-units', metavar='FILE',
                           dest='work_units_path',
                           type=existing_path_or_minus,
                           help='path to file with one JSON record per line')
        group.add_argument('--no-work', default=False, action='store_true',
                           help='set no work units, just the work spec')

    def do_load(self, args):
        '''loads work_units into a namespace for a given work_spec

        --work-units file is JSON, one work unit per line, each line a
        JSON dict with the one key being the work unit key and the
        value being a data dict for the work unit.

        '''
        work_spec = self._get_work_spec(args)
        work_spec['nice'] = args.nice
        self.task_master.set_work_spec(work_spec)

        if args.no_work:
            work_units_fh = []  # it just has to be an iterable with no lines
        else:
            work_units_fh = self._work_units_fh_from_path(args.work_units_path)
            if work_units_fh is None:
                raise RuntimeError('need -u/--work-units or --no-work')
        self.stdout.write('loading work units from {0!r}\n'
                          .format(work_units_fh))
        work_units = dict(self._read_work_units_file(work_units_fh))

        if work_units:
            self.stdout.write('pushing work units\n')
            self.task_master.add_work_units(work_spec['name'], work_units.items())
            self.stdout.write('finished writing {0} work units to work_spec={1!r}\n'
                              .format(len(work_units), work_spec['name']))
        else:
            self.stdout.write('no work units. done.\n')

    def args_addwork(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-u', '--work-units', metavar='FILE',
                            dest='work_units_path',
                            type=existing_path_or_minus,
                            help='path to file with one JSON record per line')

    def do_addwork(self, args):
        work_spec_name = self._get_work_spec_name(args)
        work_units_fh = self._work_units_fh_from_path(args.work_units_path)
        work_units = [kv for kv in self._read_work_units_file(work_units_fh)]
        if work_units_fh is None:
            raise RuntimeError('need -u/--work-units')

        self.task_master.add_work_units(work_spec_name, work_units)

    def args_delete(self, parser):
        parser.add_argument('-y', '--yes', default=False, action='store_true',
                            dest='assume_yes',
                            help='assume "yes" and require no input for '
                            'confirmation questions.')
    def do_delete(self, args):
        '''delete the entire contents of the current namespace'''
        namespace = self.config['namespace']
        if not args.assume_yes:
            response = raw_input('Delete everything in {0!r}?  Enter namespace: '
                                 .format(namespace))
            if response != namespace:
                self.stdout.write('not deleting anything\n')
                return
        self.stdout.write('deleting namespace {0!r}\n'.format(namespace))
        self.task_master.clear()

    def args_work_specs(self, parser):
        pass
    def do_work_specs(self, args):
        '''print the names of all of the work specs'''
        work_spec_names = [x['name'] for x in self.task_master.iter_work_specs()]
        work_spec_names.sort()
        for name in work_spec_names:
            self.stdout.write('{0}\n'.format(name))

    def args_work_spec(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('--json', default=False, action='store_true',
                            help='write output in json')
        parser.add_argument('--yaml', default=False, action='store_true',
                            help='write output in yaml (default)')
    def do_work_spec(self, args):
        '''dump the contents of an existing work spec'''
        work_spec_name = self._get_work_spec_name(args)
        spec = self.task_master.get_work_spec(work_spec_name)
        if args.json:
            self.stdout.write(json.dumps(spec, indent=4, sort_keys=True) +
                              '\n')
        else:
            yaml.safe_dump(spec, self.stdout)

    def args_status(self, parser):
        self._add_work_spec_name_args(parser)

    def do_status(self, args):
        '''print the number of work units in an existing work spec'''
        work_spec_name = self._get_work_spec_name(args)
        status = self.task_master.status(work_spec_name)
        self.stdout.write(json.dumps(status, indent=4, sort_keys=True) +
                          '\n')

    def args_summary(self, parser):
        parser.add_argument('--json', default=False, action='store_true',
                            help='write output in json')
    def do_summary(self, args):
        '''print a summary of running rejester work'''
        if args.json:
            xd = {}
            for ws in self.task_master.iter_work_specs():
                name = ws['name']
                status = self.task_master.status(name)
                xd[name] = status
            xd['_NOW'] = time.time()
            self.stdout.write(json.dumps(xd) + '\n')
            return

        self.stdout.write('Work spec               Avail  Pending  Blocked'
                          '   Failed Finished    Total\n')
        self.stdout.write('==================== ======== ======== ========'
                          ' ======== ======== ========\n')
        for ws in self.task_master.iter_work_specs():
            name = ws['name']
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
        if args.status:
            status = args.status.upper()
            statusi = getattr(self.task_master, status, None)
            if statusi is None:
                self.stdout.write('unknown status {0!r}\n'.format(args.status))
                return
        else:
            statusi = None
        work_units = dict(self.task_master.get_work_units(
            work_spec_name, state=statusi, limit=args.limit))
        work_unit_names = sorted(work_units.keys())
        if args.limit:
            work_unit_names = work_unit_names[:args.limit]
        for k in work_unit_names:
            if args.details:
                tback = work_units[k].get('traceback', '')
                if tback:
                    tback += '\n'
                    work_units[k]['traceback'] = 'displayed below'
                self.stdout.write(
                    '{0!r}: {1}\n{2}'
                    .format(k, pprint.pformat(work_units[k], indent=4),
                            tback))
            else:
                self.stdout.write('{0}\n'.format(k))

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
            self.stdout.write('{0} ({1!r})\n'
                              .format(work_unit_name, status['status']))
            if 'expiration' in status:
                when = time.ctime(status['expiration'])
                if status == 'available':
                    if status['expiration'] == 0:
                        self.stdout.write('  Never scheduled\n')
                    else:
                        self.stdout.write('  Available since: {0}\n'
                                          .format(when))
                else:
                    self.stdout.write('  Expires: {0}\n'.format(when))
            if 'worker_id' in status:
                try:
                    heartbeat = self.task_master.get_heartbeat(status['worker_id'])
                except:
                    heartbeat = None
                if heartbeat:
                    hostname = (heartbeat.get('fqdn', None) or
                                heartbeat.get('hostname', None) or
                                '')
                    ipaddrs = ', '.join(heartbeat.get('ipaddrs', ()))
                    if hostname and ipaddrs:
                        summary = '{0} on {1}'.format(hostname, ipaddrs)
                    else:
                        summary = hostname + ipaddrs
                else:
                    summary = 'No information'
                self.stdout.write('  Worker: {0} ({1})\n'.format(
                    status['worker_id'], summary))
            if 'traceback' in status:
                self.stdout.write('  Traceback:\n{0}\n'.format(
                    status['traceback']))
            if 'depends_on' in status:
                self.stdout.write('  Depends on:\n')
                for what in status['depends_on']:
                    self.stdout.write('    {0!r}\n'.format(what))

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
                    units = self.task_master.get_work_units(
                        work_spec_name, limit=1000,
                        state=self.task_master.FAILED)
                    units = [u[0] for u in units]  # just need wu key
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
                    self.stdout.write('No such failed work unit {0!r}.\n'
                                      .format(unit))
                    complained = True
                    units.remove(unit)
                    # and try again
        except NoSuchWorkSpecError, e:
            # NB: you are not guaranteed to get this, especially with --all
            self.stdout.write('Invalid work spec {0!r}.\n'
                              .format(work_spec_name))
            return
        if retried == 0 and not complained:
            self.stdout.write('Nothing to do.\n')
        elif retried == 1:
            self.stdout.write('Retried {0} work unit.\n'.format(retried))
        elif retried > 1:
            self.stdout.write('Retried {0} work units.\n'.format(retried))

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
        if args.status is None:
            all = units is None
            count += self.task_master.del_work_units(work_spec_name, work_unit_keys=units, all=all)
        elif args.status == 'available':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.AVAILABLE)
        elif args.status == 'pending':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.PENDING)
        elif args.status == 'blocked':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.BLOCKED)
        elif args.status == 'finished':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.FINISHED)
        elif args.status == 'failed':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.FAILED)
        self.stdout.write('Removed {0} work units.\n'.format(count))

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
            self.stdout.write('set mode to {0!r}\n'.format(args.mode))
        else:
            mode = self.task_master.get_mode()
            self.stdout.write('{0!s}\n'.format(mode))

    def args_global_lock(self, parser):
        parser.add_argument('--purge', '-p', action='store_true',
                            help='forcibly clear the lock')
    def do_global_lock(self, args):
        '''read (or clear) the global lock'''
        if args.purge:
            self.task_master.registry.force_clear_lock()
        else:
            owner = self.task_master.registry.read_lock()
            if owner:
                heartbeat = self.task_master.get_heartbeat(owner)
                if 'hostname' in heartbeat:
                    self.stdout.write('{0} ({1})\n'.format(owner,
                                                         heartbeat['hostname']))
                else:
                    self.stdout.write('{0}\n'.format(owner))
            else:
                self.stdout.write('(unlocked)\n')

    def args_workers(self, parser):
        parser.add_argument('--all', action='store_true',
                            help='list all workers (even dead ones)')
        parser.add_argument('--details', action='store_true',
                            help='include more details if available')
    def do_workers(self, args):
        '''list all known workers'''
        workers = self.task_master.workers(alive=not args.all)
        for k in sorted(workers.iterkeys()):
            self.stdout.write('{0} ({1})\n'.format(k, workers[k]))
            if args.details:
                heartbeat = self.task_master.get_heartbeat(k)
                for hk, hv in heartbeat.iteritems():
                    self.stdout.write('  {0}: {1}\n'.format(hk, hv))

    def args_run_one(self, parser):
        parser.add_argument('--from-work-spec', action='append', default=[], help='workspec name to accept work from, may be repeated.')
        parser.add_argument('--limit-seconds', default=None, type=int, metavar='N', help='stop after running for N seconds')
        parser.add_argument('--limit-count', default=None, type=int, metavar='N', help='stop after running for N work units')
    def do_run_one(self, args):
        '''run a single job'''
        work_spec_names = args.from_work_spec or None
        worker = SingleWorker(self.config, task_master=self.task_master, work_spec_names=work_spec_names)
        worker.register()
        rc = False
        starttime = time.time()
        count = 0
        try:
            while True:
                rc = worker.run()
                if not rc:
                    break
                count += 1
                if (args.limit_seconds is None) and (args.limit_count is None):
                    # only do one
                    break
                if (args.limit_seconds is not None) and ((time.time() - starttime) >= args.limit_seconds):
                    break
                if (args.limit_count is not None) and (count >= args.limit_count):
                    break
        finally:
            worker.unregister()
        if not rc:
            self.exitcode = 2

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
