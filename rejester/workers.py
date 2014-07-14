'''Rejester workers.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

The standard worker infrastructure in the classes below calls
:meth:`rejester.WorkUnit.run` on individual work units as they become
available.  In normal use, a caller will use
:meth:`rejester.TaskMaster.update_bundle` to submit jobs, then expect
an external caller to run ``rejester run_worker``, which will create a
:class:`MultiWorker` object that runs those jobs.

Other implementation strategies are definitely possible.  The
:class:`SingleWorker` class here will run exactly one job when
invoked.  It is also possible for a program that intends to do some
work, possibly even in parallel, but wants to depend on rejester for
queueing, to call :meth:`rejester.TaskMaster.get_work` itself and do
work based on whatever information is in the work spec; that would not
use this worker infrastructure at all.

.. autoclass:: SingleWorker
    :members:
    :show-inheritance:

.. autoclass:: MultiWorker
    :members:
    :show-inheritance:

.. autoclass:: HeadlessWorker
    :members:
    :show-inheritance:

.. autoclass:: ForkWorker
    :members:
    :show-inheritance:

.. autofunction:: run_worker

'''
from __future__ import absolute_import
from collections import deque
import errno
import logging
import multiprocessing
from operator import itemgetter
import os
import psutil
import random
import Queue
import signal
import struct
import sys
import threading
import time
import uuid

from setproctitle import setproctitle

import dblogger
import rejester
from rejester import TaskMaster, Worker
from rejester._task_master import WORKER_OBSERVED_MODE, WORKER_STATE_
from rejester._registry import nice_identifier
import yakonfig

logger = logging.getLogger(__name__)

def test_work_program(work_unit):
    ## just to show that this works, we get the config from the data
    ## and *reconnect* to the registry with a second instances instead
    ## of using work_unit.registry
    config = work_unit.data['config']
    sleeptime = float(work_unit.data.get('sleep', 9.0))
    task_master = TaskMaster(config)
    logger.info('executing work_unit %r ... %s', work_unit.key, sleeptime)
    time.sleep(sleeptime)  # pretend to work
    logger.info('finished %r' % work_unit)

def run_worker(worker_class, *args, **kwargs):
    '''Bridge function to run a worker under :mod:`multiprocessing`.

    The :mod:`multiprocessing` module cannot
    :meth:`~multiprocessing.Pool.apply_async` to a class constructor,
    even if the ``__init__`` calls ``.run()``, so this simple wrapper
    calls ``worker_class(*args, **kwargs)`` and logs any exceptions
    before re-raising them.

    This is usually only used to create a :class:`HeadlessWorker`, but
    it does run through the complete
    :meth:`~rejester.Worker.register`, :meth:`~rejester.Worker.run`,
    :meth:`~rejester.Worker.unregister` sequence with some logging
    on worker-level failures.

    '''
    try:
        worker = worker_class(*args, **kwargs)
    except Exception, exc:
        logger.critical('failed to create worker {!r}'.format(worker_class),
                        exc_info=True)
        raise
    try:
        worker.register()
        worker.run()
    except Exception, exc:
        logger.error('worker {!r} died'.format(worker_class), exc_info=True)
        raise
    finally:
        logger.debug('preparing to worker.unregister() for %r', worker)
        worker.unregister()


class HeadlessWorker(Worker):
    '''Child worker to do work under :mod:`multiprocessing`.

    The :meth:`run` method expects to run a single
    :class:`~rejester.WorkUnit`, which it will receive from its
    parent :class:`MultiWorker`.  This class expects to be the only
    thing run in a :mod:`multiprocessing` child process.

    '''

    def __init__(self, config, worker_id, work_spec_name, work_unit_key):
        # Do a complete reset of logging right now before we do anything else.
        #
        # multiprocessing.Pool is super super asynchronous: when you
        # apply_async() your job description goes into a queue, which
        # a thread moves to another queue, a second thread tries every
        # 0.1s to make sure the subprocesses exist, and the subprocess
        # actually pulls the job off the queue.  If your main thread is
        # doing something when that 0.1s timer fires, it's possible that
        # the os.fork()ed child is actually forked holding some lock from
        # the parent.
        #
        # See:  http://bugs.python.org/issue6721
        #
        # logging seems to be the most prominent thing that causes
        # trouble here.  There is both a global logging._lock,
        # plus every logging.Handler instance has a lock.  If we just
        # clean up these locks we'll be good.
        logging._lock = threading.RLock()
        for handler in logging._handlers.itervalues():
            handler.createLock()

        # Now go on as normal
        super(HeadlessWorker, self).__init__(config)
        for sig_num in [signal.SIGTERM, signal.SIGHUP, signal.SIGABRT]:
            signal.signal(sig_num, self.terminate)
        self.work_unit = self.task_master.get_assigned_work_unit(
            worker_id, work_spec_name, work_unit_key)
        ## carry this to overwrite self.worker_id after .register()
        self._pre_assigned_worker_id = worker_id

    def register(self):
        super(HeadlessWorker, self).register()
        self.worker_id = self._pre_assigned_worker_id

    def run(self):
        self.work_unit.run()

    def terminate(self, sig_num, frame):
        logger.info('received %d, ending work unit', sig_num)
        self.work_unit.terminate()
        sys.exit()

class MultiWorker(Worker):
    '''Parent worker that runs multiple jobs continuously.
    
    This uses :mod:`multiprocessing` to run one child
    :class:`HeadlessWorker` per core on the system, and averages
    system memory to report `available_gb`.  This class manages the
    :class:`~rejester.TaskMaster` interactions and sends
    :class:`~rejester.WorkUnit` instances to its managed child
    processes.

    This class is normally invoked from the command line by
    running ``rejester run_worker``, which runs this class as a
    daemon process.

    Instances of this class, running across many machines in a
    cluster, are controlled by :meth:`rejester.TaskMaster.get_mode`.
    The :meth:`run` method will exit if the current mode is
    :attr:`~rejester.TaskMaster.TERMINATE`.  If the mode is
    :attr:`~rejester.TaskMaster.IDLE` then the worker will stay
    running but will not start new jobs.  New jobs will be started
    only when the mode becomes :attr:`~rejester.TaskMaster.RUN`.  The
    system defaults to :attr:`~rejester.TaskMaster.IDLE` state, but if
    workers exit immediately, it may be because the mode has been left
    at :attr:`~rejester.TaskMaster.TERMINATE` from a previous
    execution.

    If `tasks_per_cpu` is set in the configuration block for rejester,
    then that many child process will be launched for each CPU on the
    machine.

    '''
    def __init__(self, config):
        super(MultiWorker, self).__init__(config)
        self._event_queue = multiprocessing.Queue()
        self._mode = None
        self.pool = None
        logger.debug('MultiWorker initialized')

    _available_gb = None

    @classmethod
    def available_gb(cls):
        if cls._available_gb is None:
            mem = psutil.phymem_usage()
            cls._available_gb = float(mem.free) / multiprocessing.cpu_count()
        return cls._available_gb

    def _finish_callback(self, *args):
        # We don't actually get anything useful from the work call, so
        # just post an event that causes us to wake up and poll all
        # the slots.
        self._event_queue.put(True)

    def _poll_async_result(self, async_result, work_unit, do_update=True):
        if async_result is None:
            return
        assert work_unit is not None
        if not async_result.ready():
            if do_update:
                logger.debug('not ready %r, update', work_unit.key)
                work_unit.update()
            return
        try:
            async_result.get(0)
        except multiprocessing.TimeoutError:
            if do_update:
                logger.debug('get timeout update %r', work_unit.key)
                work_unit.update()
            return
        except Exception, exc:
            logger.error('trapped child exception', exc_info=True)
            work_unit.fail(exc)
        else:
            ## if it gets here, slot should always be finished
            assert async_result.ready()
            work_unit.finish()
        ## either failed or finished
        assert work_unit.failed or work_unit.finished

    def _get_and_start_work(self):
        "return (async_result, work_unit) or (None, None)"
        worker_id = nice_identifier()
        work_unit = self.task_master.get_work(worker_id, available_gb=self.available_gb())
        if work_unit is None:
            return None, None
        async_result = self.pool.apply_async(
            run_worker,
            (HeadlessWorker, self.task_master.registry.config,
             worker_id,
             work_unit.work_spec_name,
             work_unit.key),
            callback=self._finish_callback)
        return async_result, work_unit

    def _poll_slots(self, slots, mode=None, do_update=False):
        hasWork = True
        for i in xrange(len(slots)):
            async_result, work_unit = slots[i]
            if (async_result is not None) and (work_unit is not None):
                self._poll_async_result(async_result, work_unit, do_update=do_update)
                if work_unit.failed or work_unit.finished:
                    slots[i] = (None, None)
            if (slots[i][0] is None) and (mode == self.task_master.RUN):
                if hasWork:
                    slots[i] = self._get_and_start_work()
                    if slots[i][0] is None:
                        # If we fail to get work, don't hammer the
                        # taskmaster with requests for work. Wait
                        # until after a sleep and the next _poll_slots
                        # cycle.
                        hasWork = False

    def run(self):
        '''Fetch and dispatch jobs as long as the system is running.

        This periodically checks the :class:`rejester.TaskMaster` mode
        and asks it for more work.  It will normally run forever in a
        loop until the mode becomes
        :attr:`~rejester.TaskMaster.TERMINATE`, at which point it
        waits for all outstanding jobs to finish and exits.

        This will :func:`~rejester.Worker.heartbeat` and check for new
        work whenever a job finishes, or otherwise on a random
        interval between 1 and 5 seconds.

        '''

        tm = self.task_master
        num_workers = multiprocessing.cpu_count()
        if 'tasks_per_cpu' in self.config:
            num_workers *= self.config.get('tasks_per_cpu') or 1
        if self.pool is None:
            self.pool = multiprocessing.Pool(num_workers, maxtasksperchild=1)
        ## slots is a fixed-length list of [AsyncRsults, WorkUnit]
        slots = [[None, None]] * num_workers
        logger.info('MultiWorker starting with %s workers', num_workers)
        min_loop_time = 2.0
        lastFullPoll = time.time()
        while True:
            mode = self.heartbeat()
            if mode != self._mode:
                logger.info('worker {} changed to mode {}'
                            .format(self.worker_id, mode))
                self._mode = mode
            now = time.time()
            should_update = (now - lastFullPoll) > min_loop_time
            self._poll_slots(slots, mode=mode, do_update=should_update)
            if should_update:
                lastFullPoll = now

            if mode == tm.TERMINATE:
                num_waiting = sum(map(int, map(bool, map(itemgetter(0), slots))))
                if num_waiting == 0:
                    logger.info('MultiWorker all children finished')
                    break
                else:
                    logger.info('MultiWorker waiting for %d children to finish', num_waiting)

            sleepsecs = random.uniform(1,5)
            sleepstart = time.time()
            try:
                self._event_queue.get(block=True, timeout=sleepsecs)
                logger.debug('woken by event looptime=%s sleeptime=%s', sleepstart - now, time.time() - sleepstart)
            except Queue.Empty, empty:
                logger.debug('queue timed out. be exhausting, looptime=%s sleeptime=%s', sleepstart - now, time.time() - sleepstart)
                # it's cool, timed out, do the loop of checks and stuff.

        logger.info('MultiWorker exiting')

class SingleWorker(Worker):
    '''Worker that runs exactly one job when called.

    This is used by the :meth:`rejester.run.Manager.do_run_one`
    command to run a single job; that just calls :meth:`run`.  This
    is also invoked as the child process by :class:`ForkWorker`,
    which calls :meth:`as_child`.

    '''
    def run(self, set_title=False):
        '''Get exactly one job, run it, and return.

        Does nothing (but returns :const:`False`) if there is no work
        to do.  Ignores the global mode; this will do work even
        if :func:`rejester.TaskMaster.get_mode` returns
        :attr:`~rejester.TaskMaster.TERMINATE`.

        :param set_title: if true, set the process's title with the
          work unit name
        :return: :const:`True` if there was a job (even if it failed)

        '''
        available_gb = MultiWorker.available_gb()
        unit = self.task_master.get_work(self.worker_id, available_gb)
        if unit is None: return False
        try:
            if set_title:
                setproctitle('rejester worker {} {}'
                             .format(unit.work_spec_name, unit.key))
            unit.run()
            unit.finish()
        except Exception, e:
            unit.fail(e)
        return True

    #: Exit code from :meth:`as_child` if it ran a work unit (maybe
    #: unsuccessfully).
    EXIT_SUCCESS = 0
    #: Exit code from :meth:`as_child` if there was a failure getting
    #: the work unit.
    EXIT_EXCEPTION = 1
    #: Exit code from :meth:`as_child` if there was no work to do.
    EXIT_BORED = 2

    @classmethod
    def as_child(cls, global_config):
        '''Run a single job in a child process.

        This method never returns; it always calls :func:`sys.exit`
        with an error code that says what it did.

        '''
        try:
            setproctitle('rejester worker')
            random.seed()  # otherwise everyone inherits the same seed
            yakonfig.set_default_config([yakonfig, dblogger, rejester],
                                        config=global_config)
            worker = cls(yakonfig.get_global_config(rejester.config_name))
            worker.register()
            did_work = worker.run(set_title=True)
            worker.unregister()
            if did_work:
                sys.exit(cls.EXIT_SUCCESS)
            else:
                sys.exit(cls.EXIT_BORED)
        except Exception, e:
            # There's some off chance we have logging.
            # You will be here if redis is down, for instance,
            # and the yakonfig dblogger setup runs but then
            # the get_work call fails with an exception.
            if len(logging.root.handlers) > 0:
                logger.critical('failed to do any work', exc_info=e)
            sys.exit(cls.EXIT_EXCEPTION)

class ForkWorker(Worker):
    '''Parent worker that runs multiple jobs concurrently.

    This manages a series of child processes, each of which runs
    a :class:`SingleWorker`.  It runs as long as the global rejester
    state is not :class:`rejester.TaskMaster.TERMINATE`.

    This takes some additional optional configuration options.  A
    typical configuration will look like:

    .. code-block:: yaml

        rejester:
          # required rejester configuration
          registry_addresses: [ 'redis.example.com:6379' ]
          app_name: rejester
          namespace: namespace

          # indicate which worker to use
          worker: fork_worker
          fork_worker:
            # set this or num_workers; num_workers takes precedence
            num_workers_per_core: 1
            # how often to check if there is more work
            poll_interval: 1
            # how often to start more workers
            spawn_interval: 0.01

    This spawns child processes to do work.  Each child process does at
    most one work unit.  If `num_workers` is set, at most this many
    concurrent workers will be running at a time.  If `num_workers` is
    not set but `num_workers_per_core` is, the maximum number of workers
    is a multiple of the number of processor cores available.  The
    default setting is 1 worker per core, but setting this higher can
    be beneficial if jobs are alternately network- and CPU-bound.

    The parent worker runs a fairly simple state machine.  It awakens
    on startup, whenever a child process exits, or after a timeout.
    When it awakens, it checks on the status of all of its children,
    and collects the exit status of those that have finished.  If any
    failed or reported no more work, the timeout is set to
    `poll_interval`, and no more workers are started until that
    timeout has passed.  Otherwise, if it is not running the maximum
    number of workers, it starts one exactly and sets the timeout to
    `spawn_interval`.

    This means that if the system is operating normally, and there is
    work to do, then it will start all of its workers in `num_workers`
    times `spawn_interval` time.  If `spawn_interval` is 0, then any
    time the system thinks it may have work to do, it will spawn the
    maximum number of processes immediately, each of which will
    connect to Redis.  If the system runs out of work, or if it starts
    all of its workers, it will check for work or system shutdown
    every `poll_interval`.  This latter check is fairly lightweight
    but does involve contacting the Redis server.

    .. todo:: This should become the default process-level worker,
              replacing :class:`MultiWorker`.

    '''
    
    '''Several implementation notes:
    
    fork() and logging don't mix.  While we're better off here than
    using :mod:`multiprocessing` (which forks from a thread, so the
    child can start up with a log handler locked) there are still
    several cases like syslog or dblogger handlers that depend on a
    network connection that won't be there in the child.

    That means that *nothing in the parent gets to log at all*.  We
    need to maintain a separate child process to do logging.

    ``debug_worker`` is a hidden additional configuration option.  It
    can cause a lot of boring information to get written to the log.
    It is a list of keywords.  ``children`` logs all process creation
    and destruction.  ``loop`` shows what we're thinking in the main
    loop.

    '''

    config_name = 'fork_worker'
    default_config = {
        'num_workers': None,
        'num_workers_per_core': 1,
        'poll_interval': 1,
        'spawn_interval': 0.01,
        'debug_worker': None,
    }
    @classmethod
    def config_get(cls, config, key):
        return config.get(key, cls.default_config[key])

    def __init__(self, config):
        '''Create a new forking worker.

        This starts up with no children and no logger, and it will
        set itself up and contact Redis as soon as :meth:`run` is called.

        :param dict config: ``rejester`` config dictionary

        '''
        super(ForkWorker, self).__init__(config)
        c = config.get(self.config_name, {})
        self.num_workers = self.config_get(c, 'num_workers')
        if not self.num_workers:
            num_cores = 0
            # This replicates the Good Unix case of multiprocessing.cpu_count()
            try:
                num_cores = os.sysconf('SC_NPROCESSORS_ONLN')
            except (ValueError, OSError, AttributeError):
                pass
            num_cores = max(1, num_cores)
            num_workers_per_core = self.config_get(c, 'num_workers_per_core')
            self.num_workers = num_workers_per_core * num_cores
        self.poll_interval = self.config_get(c, 'poll_interval')
        self.spawn_interval = self.config_get(c, 'spawn_interval')
        self.debug_worker = c.get('debug_worker', [])
        self.children = set()
        self.log_child = None
        self.log_fd = None
        self.shutting_down = False
        self.last_mode = None
        self.old_sigabrt = None
        self.old_sigint = None
        self.old_sigpipe = None
        self.old_sigterm = None

    @staticmethod
    def pid_is_alive(pid):
        try:
            os.kill(pid, 0)
            # This doesn't actually send a signal, but does still do the
            # "is pid alive?" check.  If we didn't get an exception, it is.
            return True
        except OSError, e:
            if e.errno == errno.ESRCH:
                # "No such process"
                return False
            raise

    def set_signal_handlers(self):
        '''Set some signal handlers.

        These react reasonably to shutdown requests, and keep the
        logging child alive.

        '''
        def handler(f):
            def wrapper(signum, backtrace):
                return f()
            return wrapper

        self.old_sigabrt = signal.signal(signal.SIGABRT,
                                         handler(self.scram))
        self.old_sigint = signal.signal(signal.SIGINT,
                                        handler(self.stop_gracefully))
        self.old_sigpipe = signal.signal(signal.SIGPIPE,
                                         handler(self.live_log_child))
        signal.siginterrupt(signal.SIGPIPE, False)
        self.old_sigterm = signal.signal(signal.SIGTERM,
                                         handler(self.stop_gracefully))

    def clear_signal_handlers(self):
        '''Undo :meth:`set_signal_handlers`.

        Not only must this be done on shutdown, but after every fork
        call too.

        '''
        signal.signal(signal.SIGABRT, self.old_sigabrt)
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGPIPE, self.old_sigpipe)
        signal.signal(signal.SIGTERM, self.old_sigterm)

    def log(self, level, message):
        '''Write a log message via the child process.

        The child process must already exist; call :meth:`live_log_child`
        to make sure.  If it has died in a way we don't expect then
        this will raise :const:`signal.SIGPIPE`.

        '''
        if self.log_fd is not None:
            prefix = struct.pack('ii', level, len(message))
            os.write(self.log_fd, prefix)
            os.write(self.log_fd, message)

    def debug(self, group, message):
        '''Maybe write a debug-level log message.

        In particular, this gets written if the hidden `debug_worker`
        option contains `group`.

        '''
        if group in self.debug_worker:
            if 'stdout' in self.debug_worker:
                print message
            self.log(logging.DEBUG, message)

    def log_spewer(self, gconfig, fd):
        '''Child process to manage logging.

        This reads pairs of lines from `fd`, which are alternating
        priority (Python integer) and message (unformatted string).

        '''
        setproctitle('rejester fork_worker log task')
        yakonfig.set_default_config([yakonfig, dblogger], config=gconfig)
        try:
            while True:
                prefix = os.read(fd, struct.calcsize('ii'))
                level, msglen = struct.unpack('ii', prefix)
                msg = os.read(fd, msglen)
                logger.log(level, msg)
        except Exception, e:
            logger.critical('log writer failed', exc_info=e)
            raise
        
    def start_log_child(self):
        '''Start the logging child process.'''
        self.stop_log_child()
        gconfig = yakonfig.get_global_config()
        read_end, write_end = os.pipe()
        pid = os.fork()
        if pid == 0:
            # We are the child
            self.clear_signal_handlers()
            os.close(write_end)
            yakonfig.clear_global_config()
            self.log_spewer(gconfig, read_end)
            sys.exit(0)
        else:
            # We are the parent
            self.debug('children', 'new log child with pid {}'.format(pid))
            self.log_child = pid
            os.close(read_end)
            self.log_fd = write_end

    def stop_log_child(self):
        '''Stop the logging child process.'''
        if self.log_fd:
            os.close(self.log_fd)
            self.log_fd = None
        if self.log_child:
            try:
                self.debug('children', 'stopping log child with pid {}'
                           .format(self.log_child))
                os.kill(self.log_child, signal.SIGTERM)
                os.waitpid(self.log_child, 0)
            except OSError, e:
                if e.errno == errno.ESRCH or e.errno == errno.ECHILD:
                    # already gone
                    pass
                else:
                    raise
            self.log_child = None

    def live_log_child(self):
        '''Start the logging child process if it died.'''
        if not (self.log_child and self.pid_is_alive(self.log_child)):
            self.start_log_child()

    def do_some_work(self, can_start_more):
        '''Run one cycle of the main loop.

        If the log child has died, restart it.  If any of the worker
        children have died, collect their status codes and remove them
        from the child set.  If there is a worker slot available, start
        exactly one child.

        :param bool can_start_more: Allowed to start a child?
        :return:  Time to wait before calling this function again

        '''
        any_happy_children = False
        any_sad_children = False
        any_bored_children = False

        self.debug('loop', 'starting work loop, can_start_more={!r}'
                   .format(can_start_more))
        
        # See if anyone has died
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except OSError, e:
                if e.errno == errno.ECHILD:
                    # No children at all
                    pid = 0
                else:
                    raise
            if pid == 0:
                break
            elif pid == self.log_child:
                self.debug('children',
                           'log child with pid {} exited'.format(pid))
                self.start_log_child()
            elif pid in self.children:
                self.children.remove(pid)
                if os.WIFEXITED(status):
                    code = os.WEXITSTATUS(status)
                    self.debug('children',
                               'worker {} exited with code {}'
                               .format(pid, code))
                    if code == SingleWorker.EXIT_SUCCESS:
                        any_happy_children = True
                    elif code == SingleWorker.EXIT_EXCEPTION:
                        self.log(logging.WARNING,
                                 'child {} reported failure'.format(pid))
                        any_sad_children = True
                    elif code == SingleWorker.EXIT_BORED:
                        any_bored_children = True
                    else:
                        self.log(logging.WARNING,
                                 'child {} had odd exit code {}'
                                 .format(pid, code))
                elif os.WIFSIGNALED(status):
                    self.log(logging.WARNING,
                             'child {} exited with signal {}'
                             .format(pid, os.WTERMSIG(status)))
                    any_sad_children = True
                else:
                    self.log(logging.WARNING,
                             'child {} went away with unknown status {}'
                             .format(pid, status))
                    any_sad_children = True
            else:
                self.log(logging.WARNING,
                         'child {} exited, but we don\'t recognize it'
                         .format(pid))
        
        # ...what next?
        # (Don't log anything here; either we logged a WARNING message
        # above when things went badly, or we're in a very normal flow
        # and don't want to spam the log)
        if any_sad_children:
            self.debug('loop', 'exit work loop with sad child')
            return self.poll_interval

        if any_bored_children:
            self.debug('loop', 'exit work loop with no work')
            return self.poll_interval

        # This means we get to start a child, maybe.
        if can_start_more and len(self.children) < self.num_workers:
            pid = os.fork()
            if pid == 0:
                # We are the child
                self.clear_signal_handlers()
                if self.log_fd:
                    os.close(self.log_fd)
                SingleWorker.as_child(yakonfig.get_global_config())
                # This should never return, but just in case
                sys.exit(SingleWorker.EXIT_EXCEPTION)
            else:
                # We are the parent
                self.debug('children', 'new worker with pid {}'.format(pid))
                self.children.add(pid)
                self.debug('loop', 'exit work loop with a new worker')
                return self.spawn_interval

        # Absolutely nothing is happening; which means we have all
        # of our potential workers and they're doing work
        self.debug('loop', 'exit work loop with full system')
        return self.poll_interval

    def stop_gracefully(self):
        '''Refuse to start more processes.

        This runs in response to SIGINT or SIGTERM; if this isn't a
        background process, control-C and a normal ``kill`` command
        cause this.

        '''
        if self.shutting_down:
            self.log(logging.INFO,
                     'second shutdown request, shutting down now')
            self.scram()
        else:
            self.log(logging.INFO, 'shutting down after current jobs finish')
            self.shutting_down = True

    def stop_all_children(self):
        '''Kill all workers.'''
        # There's an unfortunate race condition if we try to log this
        # case: we can't depend on the logging child actually receiving
        # the log message before we kill it off.  C'est la vie...
        self.stop_log_child()
        for pid in self.children:
            try:
                os.kill(pid, signal.SIGTERM)
                os.waitpid(pid, 0)
            except OSError, e:
                if e.errno == errno.ESRCH or e.errno == errno.ECHILD:
                    # No such process
                    pass
                else:
                    raise

    def scram(self):
        '''Kill all workers and die ourselves.

        This runs in response to SIGABRT, from a specific invocation
        of the ``kill`` command.  It also runs if
        :meth:`stop_gracefully` is called more than once.

        '''
        self.stop_all_children()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        sys.exit(2)

    def run(self):
        '''Run the main loop.

        This is fairly invasive: it sets a bunch of signal handlers
        and spawns off a bunch of child processes.

        '''
        setproctitle('rejester fork_worker for namespace {}'
                     .format(self.config.get('namespace', None)))
        self.set_signal_handlers()
        try:
            self.start_log_child()
            while True:
                can_start_more = not self.shutting_down
                mode = self.heartbeat()
                if mode != self.last_mode:
                    self.log(logging.INFO,
                             'rejester global mode is {!r}'.format(mode))
                    self.last_mode = mode
                if mode != TaskMaster.RUN:
                    can_start_more = False
                interval = self.do_some_work(can_start_more)
                # Normal shutdown case
                if len(self.children) == 0:
                    if mode == TaskMaster.TERMINATE:
                        self.log(logger.INFO,
                                 'stopping for rejester global shutdown')
                        break
                    if self.shutting_down:
                        self.log(logger.INFO,
                                 'stopping in response to signal')
                        break
                time.sleep(interval)
        finally:
            self.clear_signal_handlers()
