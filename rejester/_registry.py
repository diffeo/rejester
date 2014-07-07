'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import time
import uuid
import json
import logging
import os
import random
import socket
import struct
import atexit
import contextlib
from uuid import UUID
from functools import wraps
from collections import defaultdict
from operator import mul

import redis

from rejester.exceptions import EnvironmentError, LockError, \
    ProgrammerError
from rejester._redis import RedisBase

logger = logging.getLogger(__name__)

def nice_identifier():
    'do not use uuid.uuid4, because it can block'
    big = reduce(mul, struct.unpack('<LLLL', os.urandom(16)), 1)
    big = big % 2**128
    return uuid.UUID(int=big).hex

class Registry(RedisBase):
    '''Store string-keyed dictionaries in Redis.

    Provides a centralized storage mechanism for dictionaries,
    including atomic operations for moving (key, value) pairs between
    dictionaries, and incrementing counts.

    Many operations on the registry require getting a lock via
    the database, for instance

    >>> with registry.lock() as session:
    ...   value = session.get(k1, k2)

    The lock mechanism ensures that no two Registry objects do work
    concurrently, even running on separate systems.  Specific method
    descriptions will note if they can run without a lock.  In
    general, read-only operations will always run successfully without
    a lock but will check for the correct lock if one is given;
    certain very simple operations do no lock checking at all; and
    read-write operations always require a lock.

    The basic data object is a string-keyed dictionary stored under
    some key.  The dictionary keys are also stored in a prioritized
    list.  This in effect provides two levels of dictionary, using the
    Redis key and the dictionary key.  The registry makes an effort to
    store all types of object as values, potentially serializing them
    into JSON.

    '''

    # This makes extensive use of Lua scripts in Redis; see
    # http://redis.io/commands/eval for basics.  This provides atomicity
    # (Redis never runs anything in parallel with a Lua script), but
    # also means that much of the logic is in a hard-to-watch script,
    # and that performance of these scripts is pretty critical.

    def __init__(self, config):
        '''
        Initialize the registry using a config
        '''
        super(Registry, self).__init__(config)

        ## populated only when lock is acquired
        self._session_lock_identifier = None

    def _conn(self):
        '''debugging aid to easily grab a connection from the connection pool

        '''
        conn = redis.Redis(connection_pool=self.pool)
        return conn

    @property
    def _lock_name(self):
        '''Name of the key for the global lock.'''
        return self._namespace('global_registry_lock')

    def _acquire_lock(self, identifier, atime=30, ltime=5):
        '''Acquire a lock for a given identifier.

        If the lock cannot be obtained immediately, keep trying at random
        intervals, up to 3 seconds, until `atime` has passed.  Once the
        lock has been obtained, continue to hold it for `ltime`.

        :param str identifier: lock token to write
        :param int atime: maximum time (in seconds) to acquire lock
        :param int ltime: maximum time (in seconds) to own lock
        :return: `identifier` if the lock was obtained, :const:`False`
          otherwise

        '''
        conn = redis.Redis(connection_pool=self.pool)
        end = time.time() + atime
        while end > time.time():
            if conn.set(self._lock_name, identifier, ex=ltime, nx=True):
                # logger.debug("won lock %s" % self._lock_name)
                return identifier
            sleep_time = random.uniform(0, 3)
            time.sleep(sleep_time)

        logger.warn('failed to acquire lock %s for %f seconds',
                    self._lock_name, atime)
        return False


    def re_acquire_lock(self, ltime=5):
        '''Re-acquire the lock.

        You must already own the lock; this is best called from
        within a :meth:`lock` block.

        :param int ltime: maximum time (in seconds) to own lock
        :return: the session lock identifier
        :raise rejester.exceptions.EnvironmentError:
          if we didn't already own the lock

        '''
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
          return redis.call("expire", KEYS[1], ARGV[2])
        else
          return -1
        end
        ''')
        ret = script(keys=[self._lock_name],
                     args=[self._session_lock_identifier, ltime])
        if ret != 1:
            raise EnvironmentError('failed to re-acquire lock')

        # logger.debug('re-acquired lock %s', self._lock_name)
        return self._session_lock_identifier

    def _release_lock(self, identifier):
        '''Release the lock.

        This requires you to actually have owned the lock.  On return
        you definitely do not own it, but if somebody else owned it
        before calling this function, they still do.

        :param str identifier: the session lock identifier
        :return: :const:`True` if you actually did own the lock,
          :const:`False` if you didn't

        '''
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            return redis.call("del", KEYS[1])
        else
            return -1
        end
        ''')
        num_keys_deleted = script(keys=[self._lock_name],
                                  args=[identifier])
        return (num_keys_deleted == 1)

    @contextlib.contextmanager
    def lock(self, atime=30, ltime=5, identifier=None):
        '''Context manager to acquire the namespace global lock.

        This is typically used for multi-step registry operations,
        such as a read-modify-write sequence::

            with registry.lock() as session:
                d = session.get('dict', 'key')
                del d['traceback']
                session.set('dict', 'key', d)

        Callers may provide their own `identifier`; if they do, they
        must ensure that it is reasonably unique (e.g., a UUID).
        Using a stored worker ID that is traceable back to the lock
        holder is a good practice.

        :param int atime: maximum time (in seconds) to acquire lock
        :param int ltime: maximum time (in seconds) to own lock
        :param str identifier: worker-unique identifier for the lock

        '''
        if identifier is None:
            identifier = nice_identifier()
        if self._acquire_lock(identifier, atime, ltime) != identifier:
            raise LockError("could not acquire lock")
        try:
            self._session_lock_identifier = identifier
            yield self
        finally:
            self._release_lock(identifier)
            self._session_lock_identifier = None

    def read_lock(self):
        '''Find out who currently owns the namespace global lock.

        This is purely a diagnostic tool.  If you are trying to get
        the global lock, it is better to just call :meth:`lock`, which
        will atomically get the lock if possible and retry.

        :return: session identifier of the lock holder, or :const:`None`

        '''
        return redis.Redis(connection_pool=self.pool).get(self._lock_name)

    def force_clear_lock(self):
        '''Kick out whoever currently owns the namespace global lock.

        This is intended as purely a last-resort tool.  If another
        process has managed to get the global lock for a very long time,
        or if it requested the lock with a long expiration and then
        crashed, this can make the system functional again.  If the
        original lock holder is still alive, its session calls may fail
        with exceptions.

        '''
        return redis.Redis(connection_pool=self.pool).delete(self._lock_name)

    def _encode(self, data):
        '''Redis hash's store strings in the keys and values.  Since we want
        to store UUIDs and tuples of UUIDs, we cannot just use json.
        Instead, we employ a very simply decoding to turn various
        strings into datatypes we understand.
        '''
        if isinstance(data, tuple):
            return 't:' + '-'.join([self._encode(item) for item in data])
        elif isinstance(data, UUID):
            return 'u:' + str(data.int)
        else:
            try:
                return 'j:' + json.dumps(data)
            except Exception, exc:
                logger.error('Fail to encode data %r', data, exc_info=True)
                raise TypeError

    def _decode(self, string):
        '''Redis hash's store strings in the keys and values.  Since we want
        to store UUIDs and tuples of UUIDs, we cannot just use json.
        Instead, we employ a very simply decoding to turn various
        strings into datatypes we understand.

        Note, that redis would convert things into a string
        representation that we could 'eval' here, but it is not a good
        practice from a security standpoint to eval data from a
        database.
        '''
        if len(string) == 0:
            return None
        if string[0] == 't':
            return tuple([self._decode(item) for item in
                          string[2:].split('-')])
        elif string[0] == 'u':
            return UUID(int=int(string[2:]))
        elif string[0] == 'j':
            try:
                if string[2:]:
                    return json.loads(string[2:])
                else:
                    None
            except ValueError, exc:
                logger.critical('%r --> tried to json.loads(%r)', string, string[2:], exc_info=True)
                raise
        else:
            try:
                return float(string)
            except:
                pass
            ## Raise a type error on all other types of data
            logger.error('Fail to decode string %r', string, exc_info=True)
            raise TypeError

    def update(self, dict_name, mapping=None, priorities=None, expire=None,
               locks=None):
        '''Add mapping to a dictionary, replacing previous values

        Can be called with only dict_name and expire to refresh the
        expiration time.

        NB: locks are only enforced if present, so nothing prevents
        another caller from coming in an modifying data without using
        locks.

        :param mapping: a dict of keys and values to update in
          dict_name.  Must be specified if priorities is specified.
        :param priorities: a dict with the same keys as those in
          mapping that provides a numerical value indicating the
          priority to assign to that key.  Default sets 0 for all keys.
        :param int expire: if specified, then dict_name will be set to
          expire in that many seconds.
        :param locks: a dict with the same keys as those in the
          mapping.  Before making any particular update, this function
          checks if a key is present in a 'locks' table for this dict,
          and if so, then its value must match the value provided in the
          input locks dict for that key.  If not, then the value
          provided in the locks dict is inserted into the 'locks' table.
          If the locks parameter is None, then no lock checking is
          performed.
        '''
        if self._session_lock_identifier is None:
            raise ProgrammerError('must acquire lock first')
        if priorities is None:
            ## set all priorities to zero
            priorities = defaultdict(int)
        if locks is None:
            ## set all locks to None
            locks = defaultdict(lambda: '')
        if not (expire is None or isinstance(expire, int)):
            raise ProgrammerError('expire must be int or unspecified')
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            for i = 3, #ARGV, 4  do
                if ARGV[i+3] ~= 'j:""' then
                    local curr_lock = redis.call("hget",  KEYS[4], ARGV[i])
                    if curr_lock and curr_lock ~= ARGV[i+3] then
                        return {-1, ARGV[i], curr_lock, ARGV[i+3]}
                    end
                    redis.call("hset",  KEYS[4], ARGV[i], ARGV[i+3])
                end
            end
            for i = 3, #ARGV, 4  do
                redis.call("hset",  KEYS[2], ARGV[i], ARGV[i+1])
                redis.call("zadd",  KEYS[3], ARGV[i+2], ARGV[i])
            end
            if tonumber(ARGV[2]) ~= nil then
                redis.call("expire", KEYS[2], ARGV[2])
                redis.call("expire", KEYS[3], ARGV[2])
            end
            return {1, 0}
        else
            -- ERROR: No longer own the lock
            return {0, 0}
        end
        ''')
        dict_name = self._namespace(dict_name)
        if mapping is None:
            mapping = {}
        items = []
        ## This flattens the dictionary into a list
        for key, value in mapping.iteritems():
            items.append(self._encode(key))
            items.append(self._encode(value))
            items.append(priorities[key])
            items.append(self._encode(locks[key]))

        #logger.debug('update %r %r', dict_name, items)
        res = script(keys=[self._lock_name,
                           dict_name,
                           dict_name + 'keys',
                           dict_name + '_locks'],
                     args=[self._session_lock_identifier, expire] + items)
        if res[0] == 0:
            raise EnvironmentError(
                'Unable to add items to %s in registry' % dict_name)
        elif res[0] == -1:
            raise EnvironmentError(
                'lost lock on key=%r owned by %r not %r in %s' 
                % (self._decode(res[1]), res[2], res[3], dict_name))


    def reset_priorities(self, dict_name, priority):
        '''set all priorities in dict_name to priority

        :type priority: float or int
        '''
        if self._session_lock_identifier is None:
            raise ProgrammerError('must acquire lock first')
        ## see comment above for script in update
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            local keys = redis.call('ZRANGE', KEYS[2], 0, -1)
            for i, next_key in ipairs(keys) do
                redis.call("zadd",  KEYS[2], ARGV[2], next_key)
            end
            return 1
        else
            -- ERROR: No longer own the lock
            return 0
        end
        ''')
        dict_name = self._namespace(dict_name)
        res = script(keys=[self._lock_name,
                           dict_name + 'keys'],
                     args=[self._session_lock_identifier,
                           priority])
        if not res:
            # We either lost the lock or something else went wrong
            raise EnvironmentError(
                'Unable to add items to %s in registry' % dict_name)


    def popmany(self, dict_name, *keys):
        '''Remove one or more keys from a dictionary.

        If any of the `keys` are not present, they are silently ignored.
        
        The actual deletion operation is atomic and does not require a
        session lock, but nothing stops another operation from creating
        the deleted keys immediately afterwards.  You may call this with
        or without a session lock, but the operation will fail if some
        other worker holds one.

        :param str dict_name: name of dictionary to modify
        :param str keys: names of keys to remove
        :returns: number of keys removed
        :raises rejester.exceptions.LockError: if the session lock timed
          out, or if this was called without a session lock and some other
          worker holds one
        '''
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        local lock_holder = redis.call("get", KEYS[1])
        if lock_holder == ARGV[1]
        then
            local count = 0
            for i = 2, #ARGV do
                if (redis.call("hexists", KEYS[2], ARGV[i]) == 1)
                then
                    count = count + 1
                    redis.call("hdel", KEYS[2], ARGV[i])
                    redis.call("zrem", KEYS[3], ARGV[i])
                end
            end
            return count
        else
            -- ERROR: No longer own the lock
            return -1
        end
        ''')
        res = script(keys=[self._lock_name,
                           self._namespace(dict_name),
                           self._namespace(dict_name) + "keys"],
                     args=([self._session_lock_identifier] +
                           [self._encode(key) for key in keys]))
        if res < 0:
            raise LockError()
        return res

    def len(self, dict_name, priority_min='-inf', priority_max='+inf'):
        '''Get the number of items in (part of) a dictionary.

        Returns number of items in `dict_name` within
        [`priority_min`, `priority_max`].  This is similar to
        ``len(filter(dict_name, priority_min, priority_max))`` but
        does not actually retrieve the items.

        This is a read-only operation that does not require or
        honor a session lock.

        :param str dict_name: dictionary name to query
        :param float priority_min: lowest priority score
        :param float priority_max: highest priority score
        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        return conn.zcount(dict_name + 'keys', priority_min, priority_max)

    def getitem_reset(
            self, dict_name, priority_min='-inf', priority_max='+inf',
            new_priority=0,
            lock=None
        ):
        '''Select an item and update its priority score.

        The item comes from `dict_name`, and has the lowest score at
        least `priority_min` and at most `priority_max`.  If some item
        is found, change its score to `new_priority` and return it.

        This runs as a single atomic operation but still requires a
        session lock.

        :param str dict_name: source dictionary
        :param float priority_min: lowest score
        :param float priority_max: highest score
        :param float new_priority: new score
        :param str lock: lock value for the item
        :return: pair of (key, value) if an item was reprioritized, or
          :const:`None`

        '''
        if self._session_lock_identifier is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- remove next item of dict_name
            local next_key, next_priority = redis.call("zrangebyscore",
                KEYS[3], ARGV[2], ARGV[3], "WITHSCORES", "LIMIT", 0, 1)[1]

            if not next_key then
                return {}
            end

            local next_val = redis.call("hget", KEYS[2], next_key)
            redis.call("zadd", KEYS[3], ARGV[4], next_key)
            if ARGV[5] then
                redis.call("hset", KEYS[4], next_key, ARGV[5])
                redis.call("hset", KEYS[4], ARGV[5], next_key)
            else
                local old_key2 = redis.call("hget", KEYS[4], next_key)
                redis.call("hdel", KEYS[4], next_key)
                redis.call("hdel", KEYS[4], old_key2)
            end

            return {next_key, next_val}
        else
            -- ERROR: No longer own the lock
            return -1
        end
        ''')
        dict_name = self._namespace(dict_name)
        key_value = script(keys=[self._lock_name,
                                 dict_name,
                                 dict_name + 'keys',
                                 dict_name + '_locks'],
                           args=[self._session_lock_identifier,
                                 priority_min, priority_max,
                                 new_priority,
                                 self._encode(lock)])
        if key_value == -1:
            raise KeyError(
                'Registry failed to return an item from %s' % dict_name)

        if key_value == []:
            return None

        return self._decode(key_value[0]), self._decode(key_value[1])

    def popitem(self, dict_name, priority_min='-inf', priority_max='+inf'):
        '''Select an item and remove it.

        The item comes from `dict_name`, and has the lowest score
        at least `priority_min` and at most `priority_max`.  If some
        item is found, remove it from `dict_name` and return it.

        This runs as a single atomic operation but still requires a
        session lock.

        :param str dict_name: source dictionary
        :param float priority_min: lowest score
        :param float priority_max: highest score
        :return: pair of (key, value) if an item was popped, or
          :const:`None`

        '''
        if self._session_lock_identifier is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- remove next item of dict_name
            local next_key, next_priority = redis.call("zrangebyscore",
                KEYS[3], ARGV[2], ARGV[3], "WITHSCORES", "LIMIT", 0, 1)[1]

            if not next_key then
                return {}
            end
            
            redis.call("zrem", KEYS[3], next_key)
            local next_val = redis.call("hget", KEYS[2], next_key)
            -- zrem removed it from list, so also remove from hash
            redis.call("hdel", KEYS[2], next_key)
            return {next_key, next_val}
        else
            -- ERROR: No longer own the lock
            return -1
        end
        ''')
        dict_name = self._namespace(dict_name)
        key_value = script(keys=[self._lock_name,
                                 dict_name,
                                 dict_name + "keys"],
                           args=[self._session_lock_identifier,
                                 priority_min,
                                 priority_max])
        if key_value == -1:
            raise KeyError(
                'Registry failed to return an item from %s' % dict_name)

        if key_value == []:
            return None

        return self._decode(key_value[0]), self._decode(key_value[1])

    def popitem_move(self, from_dict, to_dict,
                     priority_min='-inf', priority_max='+inf'):
        '''Select an item and move it to another dictionary.

        The item comes from `from_dict`, and has the lowest score
        at least `priority_min` and at most `priority_max`.  If some
        item is found, remove it from `from_dict`, add it to `to_dict`,
        and return it.

        This runs as a single atomic operation but still requires a
        session lock.

        :param str from_dict: source dictionary
        :param str to_dict: destination dictionary
        :param float priority_min: lowest score
        :param float priority_max: highest score
        :return: pair of (key, value) if an item was moved, or
          :const:`None`

        '''
        if self._session_lock_identifier is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- find the next key and priority
            local next_items = redis.call("zrangebyscore", KEYS[3],
                ARGV[2], ARGV[3], "WITHSCORES", "LIMIT", 0, 1)
            local next_key = next_items[1]
            local next_priority = next_items[2]
            
            if not next_key then
                return {}
            end

            -- remove next item of from_dict
            redis.call("zrem", KEYS[3], next_key)

            local next_val = redis.call("hget", KEYS[2], next_key)
            -- zrem removed it from list, so also remove from hash
            redis.call("hdel", KEYS[2], next_key)

            -- put it in to_dict
            redis.call("hset", KEYS[4], next_key, next_val)
            redis.call("zadd", KEYS[5], next_priority, next_key)

            return {next_key, next_val, next_priority}
        else
            -- ERROR: No longer own the lock
            return -1
        end
        ''')
        key_value = script(keys=[self._lock_name, 
                                 self._namespace(from_dict),
                                 self._namespace(from_dict) + 'keys',
                                 self._namespace(to_dict),
                                 self._namespace(to_dict) + 'keys'],
                           args=[self._session_lock_identifier,
                                 priority_min, priority_max])

        if key_value == []:
            return None

        if None in key_value or key_value == -1:
            raise KeyError(
                'Registry.popitem_move(%r, %r) --> %r' %
                (from_dict, to_dict, key_value))

        return self._decode(key_value[0]), self._decode(key_value[1])

    def move(self, from_dict, to_dict, mapping, priority=None):
        '''Move keys between dictionaries, possibly with changes.

        Every key in `mapping` is removed from `from_dict`, and added to
        `to_dict` with its corresponding value.  The priority will be
        `priority`, if specified, or else its current priority.

        This operation on its own is atomic and does not require a
        session lock; however, it does require you to pass in the
        values, which probably came from a previous query call.  If
        you do not call this with a session lock but some other caller
        has one, you will get :class:`rejester.LockError`.  If you do
        have a session lock, this will check that you still have it.

        :param str from_dict: name of original dictionary
        :param str to_dict: name of target dictionary
        :param dict mapping: keys to move with new values
        :param int priority: target priority, or :const:`None` to use existing
        :raise rejester.LockError: if the session lock timed out
        :raise rejester.EnvironmentError: if some items didn't move

        '''
        items = []
        ## This flattens the dictionary into a list
        for key, value in mapping.iteritems():
            key = self._encode(key)
            items.append(key)
            value = self._encode(value)
            items.append(value)

        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        local lock_holder = redis.call("get", KEYS[1])
        if (not lock_holder) or (lock_holder == ARGV[1])
        then
            local count = 0
            for i = 3, #ARGV, 2  do
                -- find the priority of the item
                local next_priority = ARGV[2]
                if next_priority == "" then
                    next_priority = redis.call("zscore", KEYS[3], ARGV[i])
                end
                -- remove item from the sorted set and the real data
                redis.call("hdel", KEYS[2], ARGV[i])
                redis.call("zrem", KEYS[3], ARGV[i])
                -- put it in to_dict
                redis.call("hset", KEYS[4], ARGV[i], ARGV[i+1])
                redis.call("zadd", KEYS[5], next_priority, ARGV[i])
                count = count + 1
            end
            return count
        else
            -- ERROR: No longer own the lock
            return -1
        end
        ''')

        # Experimental evidence suggests that redis-py passes
        # *every* value as a string, maybe unless it's obviously
        # a number.  Empty string is an easy "odd" value to test for.
        if priority is None: priority = ''
        num_moved = script(keys=[self._lock_name,
                                 self._namespace(from_dict),
                                 self._namespace(from_dict) + "keys",
                                 self._namespace(to_dict),
                                 self._namespace(to_dict) + "keys"],
                           args=[self._session_lock_identifier,
                                 priority] + items)
        if num_moved == -1:
            raise LockError()
        if num_moved != len(items) / 2:
            raise EnvironmentError(
                'Registry failed to move all: num_moved = %d != %d len(items)'
                % (num_moved, len(items)))


    def move_all(self, from_dict, to_dict):
        '''Move everything from one dictionary to another.

        This can be expensive if the source dictionary is large.

        This always requires a session lock.

        :param str from_dict: source dictionary
        :param str to_dict: destination dictionary

        '''
        if self._session_lock_identifier is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            local count = 0
            local keys = redis.call('ZRANGE', KEYS[3], 0, -1)
            for i, next_key in ipairs(keys) do
                -- get the value and priority for this key
                local next_val = redis.call("hget", KEYS[2], next_key)
                local next_priority = redis.call("zscore", KEYS[3], next_key)
                -- remove item of from_dict
                redis.call("zrem", KEYS[3], next_key)
                -- also remove from hash
                redis.call("hdel", KEYS[2], next_key)
                -- put it in to_dict
                redis.call("hset",  KEYS[4], next_key, next_val)
                redis.call("zadd", KEYS[5], next_priority, next_key)
                count = count + 1
            end
            return count
        else
            -- ERROR: No longer own the lock
            return 0
        end
        ''')
        num_moved = script(keys=[self._lock_name,
                                 self._namespace(from_dict), 
                                 self._namespace(from_dict) + 'keys',
                                 self._namespace(to_dict),
                                 self._namespace(to_dict) + 'keys'],
                           args=[self._session_lock_identifier])
        return num_moved


    def pull(self, dict_name):
        '''Get the entire contents of a single dictionary.

        This operates without a session lock, but is still atomic.  In
        particular this will run even if someone else holds a session
        lock and you do not.

        This is only suitable for "small" dictionaries; if you have
        hundreds of thousands of items or more, consider
        :meth:`filter` instead to get a subset of a dictionary.

        :param str dict_name: name of the dictionary to retrieve
        :return: corresponding Python dictionary

        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        res = conn.hgetall(dict_name)
        split_res = {self._decode(key):
                     self._decode(value)
                     for key, value in res.iteritems()}
        return split_res

    def filter(self, dict_name, priority_min='-inf', priority_max='+inf',
               start=0, limit=None):
        '''Get a subset of a dictionary.

        This retrieves only keys with priority scores greater than or
        equal to `priority_min` and less than or equal to `priority_max`.
        Of those keys, it skips the first `start` ones, and then returns
        at most `limit` keys.

        With default parameters, this retrieves the entire dictionary,
        making it a more expensive version of :meth:`pull`.  This can
        be used to limit the dictionary by priority score, for instance
        using the score as a time stamp and only retrieving values
        before or after a specific time; or it can be used to get
        slices of the dictionary if there are too many items to use
        :meth:`pull`.

        This is a read-only operation and does not require a session
        lock, but if this is run in a session context, the lock will
        be honored.

        :param str dict_name: name of the dictionary to retrieve
        :param float priority_min: lowest score to retrieve
        :param float priority_max: highest score to retrieve
        :param int start: number of items to skip
        :param int limit: number of items to retrieve
        :return: corresponding (partial) Python dictionary
        :raise rejester.LockError: if the session lock timed out

        '''
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if (ARGV[1] == "") or (redis.call("get", KEYS[1]) == ARGV[1])
        then
            -- find all the keys and priorities within range
            local next_keys = redis.call("zrangebyscore", KEYS[3],
                                         ARGV[2], ARGV[3],
                                         "limit", ARGV[4], ARGV[5])
            
            if not next_keys[1] then
                return {}
            end

            local t = {}
            for i = 1, #next_keys  do
                local next_val = redis.call("hget", KEYS[2], next_keys[i])
                table.insert(t, next_keys[i])
                table.insert(t, next_val)
            end

            return t
        else
            -- ERROR: No longer own the lock
            return -1
        end
        ''')
        if limit is None: limit = -1
        res = script(keys=[self._lock_name,
                           self._namespace(dict_name),
                           self._namespace(dict_name) + 'keys'],
                     args=[self._session_lock_identifier or '',
                           priority_min, priority_max, start, limit])
        if res == -1:
            raise LockError()
        split_res = {self._decode(res[i]):
                     self._decode(res[i+1])
                     for i in xrange(0, len(res)-1, 2)}
        return split_res

    def set_1to1(self, dict_name, key1, key2):
        '''Set two keys to be equal in a 1-to-1 mapping.

        Within `dict_name`, `key1` is set to `key2`, and `key2` is set
        to `key1`.

        This always requires a session lock.

        :param str dict_name: dictionary to update
        :param str key1: first key/value
        :param str key2: second key/value

        '''
        if self._session_lock_identifier is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            redis.call("hset", KEYS[2], ARGV[2], ARGV[3])
            redis.call("hset", KEYS[2], ARGV[3], ARGV[2])
        else
            -- ERROR: No longer own the lock
            return -1
        end
        ''')
        res = script(keys=[self._lock_name,
                           self._namespace(dict_name)],
                     args=[self._session_lock_identifier,
                           self._encode(key1),
                           self._encode(key2)])
        if res == -1:
            raise EnvironmentError()

    def get(self, dict_name, key, default=None, include_priority=False):
        '''Get the value for a specific key in a specific dictionary.

        If `include_priority` is false (default), returns the value
        for that key, or `default` (defaults to :const:`None`) if it
        is absent.  If `include_priority` is true, returns a pair of
        the value and its priority, or of `default` and :const:`None`.

        This does not use or enforce the session lock, and is read-only,
        but inconsistent results are conceivably possible if the caller
        does not hold the lock and `include_priority` is set.

        :param str dict_name: name of dictionary to query
        :param str key: key in dictionary to query
        :param default: default value if `key` is absent
        :param bool include_priority: include score in results
        :return: value from dictionary, or pair of value and priority

        '''
        dict_name = self._namespace(dict_name)
        key = self._encode(key)
        conn = redis.Redis(connection_pool=self.pool)
        val = conn.hget(dict_name, key)
        _val = val and self._decode(val) or default
        if include_priority:
            if val:
                priority = conn.zscore(dict_name + 'keys', key)
                return _val, priority
            else:
                return _val, None
        return _val

    def set(self, dict_name, key, value, priority=None):
        '''Set a single value for a single key.

        This requires a session lock.

        :param str dict_name: name of the dictionary to update
        :param str key: key to update
        :param str value: value to assign to `key`
        :param int priority: priority score for the value (if any)

        '''
        if priority is not None:
            priorities = {key: priority}
        else:
            priorities = None
        self.update(dict_name, {key: value}, priorities=priorities)

    def delete(self, dict_name):
        '''Delete an entire dictionary.

        This operation on its own is atomic and does not require a
        session lock, but a session lock is honored.

        :param str dict_name: name of the dictionary to delete
        :raises rejester.exceptions.LockError: if called with a session
          lock, but the system does not currently have that lock; or if
          called without a session lock but something else holds it
        '''
        conn = redis.Redis(connection_pool=self.pool)
        script = conn.register_script('''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            redis.call("del", KEYS[2], KEYS[3])
            return 0
        else
            return -1
        end
        ''')
        res = script(keys=[self._lock_name,
                           self._namespace(dict_name),
                           self._namespace(dict_name) + 'keys'],
                     args=[self._session_lock_identifier])
        if res == -1:
            raise LockError()

    def direct_call(self, *args):
        '''execute a direct redis call against this Registry instances
        namespaced keys.  This is low level is should only be used for
        prototyping.

        arg[0] = redis function
        arg[1] = key --- will be namespaced before execution
        args[2:] = args to function

        :returns raw return value of function:

        Neither args nor return values are encoded/decoded
        '''
        conn = redis.Redis(connection_pool=self.pool)
        command = args[0]
        key = self._namespace(args[1])
        args = args[2:]
        func = getattr(conn, command)
        return func(key, *args)


    def increment(self, dict_name, key, value=1.0):
        '''
        increment the value stored at dict_name(key) by value
        '''
        conn = redis.Redis(connection_pool=self.pool)
        if isinstance(value, int):
            value = float(value)
            ## while redis will allow you to go from int to float, you
            ## cannot go back to using hincrby if a hash has floats in
            ## it.
            #conn.hincrby(self._namespace(dict_name), self._encode(key), value)
        if isinstance(value, float):
            conn.hincrbyfloat(self._namespace(dict_name), self._encode(key), value)
        else:
            raise TypeError('%r is not int or float' % value)

