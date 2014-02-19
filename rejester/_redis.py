"""Common base class for Redis-based distributed worker systems.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
"""

from __future__ import absolute_import

import logging
import socket

import redis

logger = logging.getLogger(__name__)

class RedisBase(object):
    """Common base class for Redis-based distributed worker systems.

    This class stores common metadata for systems based on the Redis
    in-memory database (http://redis.io/).

    The work being done is identified by two strings, the _application name_
    and the _namespace_.  These two strings are concatenated together and
    prepended to most Redis keys by ``_namespace()``.  To avoid leaking
    database space, it is important to clean up the namespace, for instance
    with ``delete_namespace()``, when the application is done.

    """

    def __init__(self, config):
        """Initialize the registry using a configuration object.

        ``config`` should be a dictionary with the following keys:

        ``registry_addresses``
          list of ``host:port`` for the Redis server(s)
        ``app_name``
          application name (typically fixed, e.g. "rejester")
        ``namespace``
          application invocation namespace name (should be unique per run)

        """
        super(RedisBase, self).__init__()
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

    def _ipaddress(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip

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

