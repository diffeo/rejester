'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import time
import uuid
import json
import redis
import random
import socket
import atexit
import logging
import contextlib
from uuid import UUID
from functools import wraps
from collections import defaultdict

from rejester._logging import logger
from rejester.exceptions import EnvironmentError, LockError, PriorityRangeEmpty

class Registry(object):
    '''provides a centralized storage mechanism for dictionaries,
including atomic operations for moving (key, value) pairs between
dictionaries, and incrementing counts.

This makes extensive use of Lua scripts in redis.
    http://redis.io/commands/eval

Atomicity of scripts: Redis uses the same Lua interpreter to run all
the commands. Also Redis guarantees that a script is executed in an
atomic way: no other script or Redis command will be executed while a
script is being executed. This semantics is very similar to the one of
MULTI / EXEC. From the point of view of all the other clients the
effects of a script are either still not visible or already completed.

However this also means that executing slow scripts is not a good
idea. It is not hard to create fast scripts, as the script overhead is
very low, but if you are going to use slow scripts you should be aware
that while the script is running no other client can execute commands
since the server is busy.

    '''

    def __init__(self, config):
        '''
        Initialize the registry using a config
        '''
        self.config = config
        if 'registry_addresses' not in config:
            raise ProgrammerError('registry_addresses not set')
        redis_address, redis_port = config['registry_addresses'][0].split(':')
        redis_port = int(redis_port)
        self._local_ip = self._ipaddress(redis_address, redis_port)

        if 'app_name' not in config:
            raise ProgrammerError('app_name must be specified to configure Registry')

        self._namespace_str = config['app_name'] + '_' + config['namespace']
        self.pool = redis.ConnectionPool(host=redis_address, port=redis_port)
        
        ## populated only when lock is acquired
        self._lock_name = None
        self._session_lock_identifier = None

        self._startup()
        atexit.register(self._exit)

        logger.debug('worker_id=%r  starting up on hostname=%r', 
                     self.worker_id, socket.gethostbyname(socket.gethostname()))

    def _ipaddress(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip

    def _startup(self):
        conn = redis.Redis(connection_pool=self.pool)

        ## Get a unique worker id from the registry
        self.worker_id = conn.incrby(self._namespace('worker_ids'), 1)

        ## Add worker id to list of current workers
        conn.sadd(self._namespace('cur_workers'), self.worker_id)

        ## store the local_ip of this worker for debugging
        conn.set(self._namespace(self.worker_id) + '_ip', self._local_ip)

    def _exit(self):
        conn = redis.Redis(connection_pool=self.pool)

        ## cleanup the worker's state
        #with self.lock() as session:
        #    session...

        ## Remove the worker from the list of workers
        conn.srem(self._namespace('cur_workers'), self.worker_id)
        logger.debug('atexit: worker_id=%r closed registry client', self.worker_id)

    def _conn(self):
        '''debugging aid to easily grab a connection from the connection pool

        '''
        conn = redis.Redis(connection_pool=self.pool)
        return conn

    def _all_workers(self):
        conn = redis.Redis(connection_pool=self.pool)
        return conn.smembers(self._namespace('cur_workers'))

    def delete_namespace(self):
        '''Remove all keys from the namespace

        '''
        conn = redis.Redis(connection_pool=self.pool)
        keys = conn.keys("%s*" % self._namespace_str)
        if keys:
            conn.delete(*keys)
        logger.debug('tearing down %r', self._namespace_str)

    def _namespace(self, name):
        return "%s_%s" % (self._namespace_str, name)

    def _acquire_lock(self, lock_name, identifier, atime=600, ltime=600):
        '''Acquire a lock for a given identifier.  Contend for the lock for
        atime, and hold the lock for ltime.
        '''
        conn = redis.Redis(connection_pool=self.pool)
        end = time.time() + atime
        while end > time.time():
            if conn.set(lock_name, identifier, ex=ltime, nx=True):
                logger.debug("won lock %s" % lock_name)
                return identifier
            sleep_time = random.uniform(0, 3)
            time.sleep(sleep_time)

        logger.warn(
            "failed to acquire lock %s for %f seconds" % (lock_name, atime))
        return False

    def _release_lock(self, lock_name, identifier):
        '''This follows a 'test and delete' pattern.  See redis documentation
        http://redis.io/commands/set

        This is needed because the lock could have expired before we
        deleted it.  We can only delete the lock if we still own it.
        '''
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            return redis.call("del", KEYS[1])
        else
            return -1
        end
        '''
        conn = redis.Redis(connection_pool=self.pool)
        num_keys_deleted = conn.eval(script, 1, lock_name, identifier)
        if num_keys_deleted == -1:
            ## registry_lock no long owned by this process
            raise EnvironmentError(
                'Lost lock in registry already')
        return True

    @contextlib.contextmanager
    def lock(self, lock_name='global_registry_lock', atime=600, ltime=600):
        '''
        A context manager wrapper around acquring a lock
        '''
        ## Prepend namespace to lock_name
        lock_name = self._namespace(lock_name)

        identifier = str(uuid.uuid4())
        if self._acquire_lock(lock_name, identifier, atime,
                              ltime) != identifier:
            raise LockError("could not acquire lock")
        try:
            self._lock_name = lock_name
            self._session_lock_identifier = identifier
            yield self
        finally:
            self._release_lock(lock_name, identifier)
            self._lock_name = None
            self._session_lock_identifier = None


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
        if string[0] == 't':
            return tuple([self._decode(item) for item in
                          string[2:].split('-')])
        elif string[0] == 'u':
            return UUID(int=int(string[2:]))
        elif string[0] == 'j':
            return json.loads(string[2:])
        else:
            try:
                return float(string)
            except:
                pass
            ## Raise a type error on all other types of data
            logger.error('Fail to decode string %r', string, exc_info=True)
            raise TypeError

    def update(self, dict_name, mapping, priorities=None):
        '''Add mapping to a dictionary, replacing previous values

        :param priorities: a dict with the same keys as those in
        mapping that provides a numerical value indicating the
        priority to assign to that key.  Default sets 0 for all keys.

        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        ## script is evaluated with numkeys=2, so KEYS[1] is lock_name,
        ## KEYS[2] is dict_name, ARGV[1] is identifier, and ARGV[i>=2]
        ## are keys, values, and priorities arranged in triples.
        if priorities is None:
            ## set all priorities to zero
            priorities = defaultdict(int)
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            for i = 2, #ARGV, 3  do
                redis.call("hset",  KEYS[2], ARGV[i], ARGV[i+1])
                redis.call("zadd",  KEYS[2] .. "keys", ARGV[i+2], ARGV[i])
            end
            return 1
        else
            -- ERROR: No longer own the lock
            return 0
        end
        '''
        dict_name = self._namespace(dict_name)
        items = []
        ## This flattens the dictionary into a list
        for key, value in mapping.iteritems():
            key = self._encode(key)
            items.append(key)
            value = self._encode(value)
            items.append(value)
            priority = priorities[key]
            items.append(priority)

        conn = redis.Redis(connection_pool=self.pool)
        res = conn.eval(script, 2, self._lock_name, dict_name, self._session_lock_identifier, *items)
        if not res:
            # We either lost the lock or something else went wrong
            raise EnvironmentError(
                'Unable to add items to %s in registry' % dict_name)


    def popmany(self, dict_name, *keys):
        '''
        map(D.pop, keys), remove specified keys
        If key is not found, do nothing

        :returns None:
        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        ## see comment above for script in update
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            for i = 2, #ARGV do
                redis.call("hdel", KEYS[2], ARGV[i])
                redis.call("zrem", KEYS[2] .. "keys", ARGV[i])
            end
            return 1
        else
            -- ERROR: No longer own the lock
            return 0
        end
        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        encoded_keys = [self._encode(key) for key in keys]
        res = conn.eval(script, 2, self._lock_name, dict_name,
                        self._session_lock_identifier, *encoded_keys)        
        if len(keys) > 0 and res == 0:
            # We either lost the lock or something else went wrong
            raise EnvironmentError(
                'Unable to remove items from %s in registry: %r' 
                % (dict_name, res))

    def len(self, dict_name):
        'Length of dictionary'
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        return conn.hlen(dict_name)

    def getitem_reset(
            self, dict_name, priority_min='-inf', priority_max='+inf',
            new_priority=0,
        ):
        '''D.getitem_reset() -> (key, value), return some pair that has a
        priority that satisfies the priority_min/max constraint; but
        raise PriorityRangeEmpty if that range is empty.

        (key, value) stays in the dictionary and is assigned new_priority
        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        ## see comment above for script in update
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- remove next item of dict_name
            local next_key, next_priority = redis.call("zrangebyscore", KEYS[2] .. "keys", ARGV[2], ARGV[3], "WITHSCORES")[1]

            if not next_key then
                return {}
            end

            local next_val = redis.call("hget", KEYS[2], next_key)
            redis.call("zadd", KEYS[2] .. "keys", ARGV[4], next_key)

            return {next_key, next_val}
        else
            -- ERROR: No longer own the lock
            return -1
        end
        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        logger.debug('getitem_reset: %s %s %s',
                     self._lock_name, self._session_lock_identifier, dict_name)
        key_value = conn.eval(script, 2, self._lock_name, dict_name, 
                              self._session_lock_identifier,
                              priority_min, priority_max,
                              new_priority,
                              )
        if key_value == -1:
            raise KeyError(
                'Registry failed to return an item from %s' % dict_name)

        if key_value == []:
            raise PriorityRangeEmpty()

        return self._decode(key_value[0]), self._decode(key_value[1])

    def popitem(self, dict_name, priority_min='-inf', priority_max='+inf'):
        '''
        D.popitem() -> (k, v), remove and return some (key, value) pair as a
        2-tuple; but raise KeyError if D is empty.
        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        ## see comment above for script in update
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- remove next item of dict_name
            local next_key, next_priority = redis.call("zrangebyscore", KEYS[2] .. "keys", ARGV[2], ARGV[3], "WITHSCORES")[1]

            if not next_key then
                return {}
            end
            
            redis.call("zrem", KEYS[2] .. "keys", next_key)
            local next_val = redis.call("hget", KEYS[2], next_key)
            -- zrem removed it from list, so also remove from hash
            redis.call("hdel", KEYS[2], next_key)
            return {next_key, next_val}
        else
            -- ERROR: No longer own the lock
            return -1
        end
        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        logger.critical('popitem: %s %s %s' 
                        % (self._lock_name, self._session_lock_identifier, dict_name))
        key_value = conn.eval(script, 2, self._lock_name, dict_name, 
                              self._session_lock_identifier,
                              priority_min, priority_max,
                              )
        if key_value == -1:
            raise KeyError(
                'Registry failed to return an item from %s' % dict_name)

        if key_value == []:
            raise PriorityRangeEmpty()

        return self._decode(key_value[0]), self._decode(key_value[1])

    def popitem_move(self, from_dict, to_dict, priority_min='-inf', priority_max='+inf'):
        '''
        Pop an item out of from_dict, store it in to_dict, and return it
        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- remove next item of from_dict
            local next_key, next_priority = redis.call("zrangebyscore", KEYS[2] .. "keys", ARGV[2], ARGV[3], "WITHSCORES")[1]
            
            if not next_key then
                return {}
            end

            redis.call("zrem", KEYS[2] .. "keys", next_key)

            local next_val = redis.call("hget", KEYS[2], next_key)
            -- lpop removed it from list, so also remove from hash
            redis.call("hdel", KEYS[2], next_key)

            -- put it in to_dict
            redis.call("hset",  KEYS[3], next_key, next_val)
            redis.call("zadd", KEYS[3] .. "keys", next_priority, next_key)

            return {next_key, next_val}
        else
            -- ERROR: No longer own the lock
            return -1
        end
        '''
        conn = redis.Redis(connection_pool=self.pool)
        key_value = conn.eval(script, 3, self._lock_name, 
                              self._namespace(from_dict), 
                              self._namespace(to_dict), 
                              self._session_lock_identifier,
                              priority_min, priority_max,
                              )

        if key_value == []:
            raise PriorityRangeEmpty()

        if None in key_value or key_value == -1:
            raise KeyError(
                'Registry.popitem_move(%r, %r) --> %r' % (from_dict, to_dict, key_value))

        return self._decode(key_value[0]), self._decode(key_value[1])

    def move(self, from_dict, to_dict, mapping):
        '''
        Pop mapping out of from_dict, and update them in to_dict
        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            local count = 0
            for i = 2, #ARGV, 2  do
                -- remove next item of from_dict
                local next_key = redis.call("zrange", KEYS[2] .. "keys", 0, 0)[1]
                local next_priority = redis.call("zscore", KEYS[2] .. "keys", next_key)
                redis.call("zrem", KEYS[2] .. "keys", next_key)
                local next_val = redis.call("hget", KEYS[2], next_key)
                -- lpop removed it from list, so also remove from hash
                redis.call("hdel", KEYS[2], next_key)
                -- put it in to_dict
                redis.call("hset",  KEYS[3], ARGV[i], ARGV[i+1])
                redis.call("zadd", KEYS[3] .. "keys", next_priority, ARGV[i])
                count = count + 1
            end
            return count
        else
            -- ERROR: No longer own the lock
            return 0
        end
        '''
        items = []
        ## This flattens the dictionary into a list
        for key, value in mapping.iteritems():
            key = self._encode(key)
            items.append(key)
            value = self._encode(value)
            items.append(value)

        conn = redis.Redis(connection_pool=self.pool)

        num_moved = conn.eval(script, 3, self._lock_name, 
                              self._namespace(from_dict), 
                              self._namespace(to_dict), 
                              self._session_lock_identifier, *items)
        if num_moved != len(items) / 2:
            raise EnvironmentError(
                'Registry failed to move all: num_moved = %d != %d len(items)'
                % (num_moved, len(items)))


    def pull(self, dict_name):
        '''
        Pull entire dictionary
        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        res = conn.hgetall(dict_name)
        split_res = {self._decode(key):
                     self._decode(value)
                     for key, value in res.iteritems()}
        return split_res

    def get(self, dict_name, key, default=None):
        '''
        get value for key, if missing return default if provided
        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        val = conn.hget(dict_name, self._encode(key))
        return val and self._decode(val) or default

    def set(self, dict_name, key, value):
        '''
        set value for key
        '''
        self.update(dict_name, {key: value})

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


    def increment(self, dict_name, key, value=1):
        '''
        increment the value stored at dict_name(key) by value
        '''
        conn = redis.Redis(connection_pool=self.pool)
        if isinstance(value, int):
            conn.hincrby(self._namespace(dict_name), self._encode(key), value)
        elif isinstance(value, float):
            conn.hincrbyfloat(self._namespace(dict_name), self._encode(key), value)
        else:
            raise TypeError('%r is not int or float' % value)

