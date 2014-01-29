"""Redis-based distributed priority queue.

Purpose
=======

``RejesterQueue`` provides a priority queue, where string values can
be inserted with some priority.  Items can be pulled from the queue in
priority order, and must be returned to the queue within some time
limit.  A queue item can also reserve other queue items, preventing
them from being checked out or reserved until the main item finishes.

Implementation Details
======================


This software is released under an MIT/X11 open source license.

Copyright 2014 Diffeo, Inc.
"""

from __future__ import absolute_import

import time

import redis

from rejester.exceptions import ItemInUseError, LostLease
from rejester._logging import logger as relogger
from rejester._redis import RedisBase

logger = relogger.getChild('Queue')

class RejesterQueue(RedisBase):
    """A Redis-based priority queue.

    Queue items should generally be (short) strings.  Each item has
    some associated priority.  ``check_out_item()`` will return some
    item with the highest available priority.  The item must be
    ``renew_item()`` or ``return_item()`` within a specified
    expiration limit, or the item will be lost and may be given to
    another client.

    If you have a queue item checked out, you may also reserve other
    queue items.  This prevents other clients from checking out or
    reserving these items.  When you return the main queue item, or if
    your check out expires, reserved items will be released.

    Since the queue is based on Redis, there may be multiple queue
    clients on the same queue on different systems.  Each ``RejesterQueue``
    instance has a distinct worker ID, held in the ``worker_id`` property.
    You can manually specify it in the constructor or manually set it;
    otherwise, a unique ID will be obtained via the database on first use.

    """

    def __init__(self, config, name, worker_id=None):
        """Initialize the queue using a configuration object.

        ``config`` should be a dictionary with the following keys:

        ``registry_addresses``
          list of ``host:port`` for the Redis server(s)
        ``app_name``
          application name (set to "queue" if not provided)
        ``namespace``
          application invocation namespace name (should be unique per run)

        ``name`` is the name of the queue.  If ``worker_id`` is specified
        it sets the worker ID, otherwise the database will be used to get
        a unique one on first use.

        """
        config.setdefault('app_name', 'queue')
        super(RejesterQueue, self).__init__(config)
        self.name = name
        self._worker_id = worker_id

    def _conn(self):
        """Get a new redis connection."""
        return redis.StrictRedis(connection_pool=self.pool)

    # key name construction
    def _key_worker(self): return self._namespace('worker')
    def _key_for(self, k): return self._namespace(self.name + k)
    def _key_available(self): return self._key_for('_AVAILABLE')
    def _key_priorities(self): return self._key_for('_PRIORITIES')
    def _key_expiration(self): return self._key_for('_EXPIRATION')
    def _key_workers(self): return self._key_for('_WORKERS')
    def _key_reservations(self, i): return self._key_for('_RESERVATIONS_' + i)

    def dump_queue(self, *names):
        """Debug-log some of the queues.

        ``names`` may include any of "worker", "available", "priorities",
        "expiration", "workers", or "reservations_ITEM" filling in some
        specific item.

        """
        conn = redis.StrictRedis(connection_pool=self.pool)
        for name in names:
            if name == 'worker':
                logger.debug('last worker: ' + conn.get(self._key_worker()))
            elif name == 'available':
                logger.debug('available: ' +
                             str(conn.zrevrange(self._key_available(), 0, -1,
                                                withscores=True)))
            elif name == 'priorities':
                logger.debug('priorities: ' +
                             str(conn.hgetall(self._key_priorities())))
            elif name == 'expiration':
                logger.debug('expiration: ' +
                             str(conn.zrevrange(self._key_expiration(), 0, -1,
                                                withscores=True)))
            elif name == 'workers':
                logger.debug('workers: ' +
                             str(conn.hgetall(self._key_workers())))
            elif name.startswith('reservations_'):
                item = name[len('reservations_'):]
                logger.debug('reservations for ' + item + ': ' +
                             str(conn.smembers(self._key_reservations(item))))

    @property
    def worker_id(self):
        """A unique identifier for this queue instance and the items it owns."""
        if self._worker_id is not None: return self._worker_id
        return self._get_worker_id(self._conn())
    @worker_id.setter
    def worker_id(self, w):
        """Set the worker ID for this queue instance.

        If this is set to None, the property will be reset to a unique
        worker ID on next use.

        """
        self._worker_id = w

    def _get_worker_id(self, conn):
        """Get the worker ID, using a preestablished connection."""
        if self._worker_id is None:
            self._worker_id = conn.incr(self._key_worker())
        return self._worker_id

    def add_item(self, item, priority):
        """Add ``item`` to this queue.

        It will have the specified ``priority`` (highest priority runs
        first).  If it is already in the queue, fail if it is checked
        out or reserved, or change its priority to ``priority``
        otherwise.

        """
        conn = self._conn()
        self._run_expiration(conn)
        script = conn.register_script("""
        if (redis.call("hexists", KEYS[2], ARGV[1]) ~= 0) and
           not(redis.call("zscore", KEYS[1], ARGV[1]))
        then
            return -1
        end
        redis.call("zadd", KEYS[1], ARGV[2], ARGV[1])
        redis.call("hset", KEYS[2], ARGV[1], ARGV[2])
        return 0
        """)
        result = script(keys=[self._key_available(), self._key_priorities()],
                        args=[item, priority])
        if result == -1:
            raise ItemInUseError(item)
        return

    def check_out_item(self, expiration):
        """Get the highest-priority item out of this queue.

        Returns the item, or None if no items are available.  The item
        must be either ``return_item()`` or ``renew_item()`` before
        ``expiration`` seconds pass, or it will become available to
        future callers.  The item will be marked as being owned by
        ``worker_id``.

        """
        conn = redis.StrictRedis(connection_pool=self.pool)
        self._run_expiration(conn)
        expiration += time.time()
        script = conn.register_script("""
        local item = redis.call("zrevrange", KEYS[1], 0, 0)
        if #item == 0 then return nil end
        item = item[1]
        redis.call("zrem", KEYS[1], item)
        redis.call("zadd", KEYS[2], ARGV[1], item)
        redis.call("hset", KEYS[3], "i" .. item, "w" .. ARGV[2])
        redis.call("hset", KEYS[3], "w" .. ARGV[2], "i" .. item)
        return item
        """)
        result = script(keys=[self._key_available(), self._key_expiration(),
                              self._key_workers()],
                        args=[expiration, self._get_worker_id(conn)])
        return result

    def renew_item(self, item, expiration):
        """Update the expiration time for ``item``.

        The item will remain checked out for ``expiration`` seconds
        beyond the current time.  This queue instance must have
        already checked out ``item``, and this method can fail if
        ``item`` is already overdue.

        """
        conn = self._conn()
        self._run_expiration(conn)
        expiration += time.time()
        script = conn.register_script("""
        -- already expired?
        if redis.call("hget", KEYS[2], "i" .. ARGV[1]) ~= "w" .. ARGV[3]
        then return -1 end

        -- otherwise just update the expiration
        redis.call("zadd", KEYS[1], ARGV[2], ARGV[1])
        return 0
        """)
        result = script(keys=[self._key_expiration(), self._key_workers()],
                        args=[item, expiration, self._get_worker_id(conn)])
        if result == -1:
            raise LostLease(item)
        return

    def return_item(self, item, priority):
        """Complete work on an item from ``check_out_item()``.

        If this instance no longer owns ``item``, raise ``LostLease``.
        If ``priority`` is None, the item is removed from the queue;
        otherwise it is re-added with the specified priority.  Any
        locked items associated with this item are unlocked.

        """
        conn = redis.StrictRedis(connection_pool=self.pool)
        self._run_expiration(conn)
        script = conn.register_script("""
        -- expired?
        if redis.call("hget", KEYS[4], "i" .. ARGV[1]) ~= "w" .. ARGV[3]
        then return -1 end

        -- will no longer expire
        redis.call("zrem", KEYS[2], ARGV[1])
        -- update priority, readd to available list
        if ARGV[2] == "None"
        then
          redis.call("hdel", KEYS[3], ARGV[1])
        else
          redis.call("hset", KEYS[3], ARGV[1], ARGV[2])
          redis.call("zadd", KEYS[1], ARGV[2], ARGV[1])
        end
        -- release all reservations
        local reservations = redis.call("smembers", KEYS[5])
        for i = 1, #reservations do
          local item = reservations[i]
          local pri = redis.call("hget", KEYS[3], item)
          redis.call("zadd", KEYS[1], pri, item)
        end
        -- clear out workers
        redis.call("hdel", KEYS[4], "i" .. ARGV[1])
        redis.call("hdel", KEYS[4], "w" .. ARGV[3])
        
        return 0
        """)
        # work around python -> redis -> lua marshaling
        if priority is None: priority = "None"
        result = script(keys=[self._key_available(), self._key_expiration(),
                              self._key_priorities(), self._key_workers(),
                              self._key_reservations(item)],
                        args=[item, priority, self._get_worker_id(conn)])
        if result == -1: raise LostLease(item)
        return

    def reserve_items(self, parent_item, *items):
        """Reserve a set of items until a parent item is returned.

        Prevent ``check_out_item()`` from returning any of ``items``
        until ``parent_item`` is completed or times out.  For each
        item, if it is not already checked out or reserved by some
        other parent item, it is associated with ``parent_item``, and
        the reservation will be released when ``parent_item``
        completes or times out.  Returns a list that is a subset of
        ``items`` for which we could get the reservation.

        Raises ``LostLease`` if this queue instance no longer owns
        ``parent_item``.  If any of the items do not exist, they are
        silently ignored.

        """
        conn = redis.StrictRedis(connection_pool=self.pool)
        self._run_expiration(conn)
        script = conn.register_script("""
        -- expired?
        if redis.call("hget", KEYS[2], "i" .. ARGV[1]) ~= "w" .. ARGV[2]
        then return -1 end

        -- loop through each item
        local result = {}
        for i = 3, #ARGV do
          local item = ARGV[i]
          -- item must be available to reserve
          if redis.call("zscore", KEYS[1], item) then
            redis.call("zrem", KEYS[1], item)
            redis.call("sadd", KEYS[3], item)
            result[#result + 1] = item
          end
        end
        return result
        """)
        result = script(keys=[self._key_available(), self._key_workers(),
                              self._key_reservations(parent_item)],
                        args=([parent_item, self._get_worker_id(conn)] +
                              list(items)))
        if result == -1:
            raise LostLease(parent_item)
        return result

    def _run_expiration(self, conn):
        """Return any items that have expired."""
        # The logic here is sufficiently complicated, and we need
        # enough random keys (Redis documentation strongly encourages
        # not constructing key names in scripts) that we'll need to
        # do this in multiple steps.  This means that, when we do
        # go in and actually expire things, we need to first check
        # that they're still running.

        # Get, and clear out, the list of expiring items
        now = time.time()
        script = conn.register_script("""
        local result = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1])
        redis.call("zremrangebyscore", KEYS[1], 0, ARGV[1])
        return result
        """)
        expiring = script(keys=[self._key_expiration()], args=[time.time()])
        
        # Manually expire each item one by one
        script = conn.register_script("""
        -- item may have fallen out of the worker list, if someone finished
        -- at just the very last possible moment (phew!)
        local wworker = redis.call("hget", KEYS[3], "i" .. ARGV[1])
        if not wworker then return end

        -- we want to return item, plus everything it's reserved
        local to_return = redis.call("smembers", KEYS[4])
        to_return[#to_return + 1] = ARGV[1]
        for i = 1, #to_return do
          local pri = redis.call("hget", KEYS[2], to_return[i])
          redis.call("zadd", KEYS[1], pri, to_return[i])
        end
        -- already removed from expiration list
        -- remove from worker list too
        redis.call("hdel", KEYS[3], "i" .. ARGV[1])
        redis.call("hdel", KEYS[3], wworker)
        """)
        for item in expiring:
            script(keys=[self._key_available(), self._key_priorities(),
                         self._key_workers(), self._key_reservations(item)],
                   args=[item])
