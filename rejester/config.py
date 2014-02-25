'''Yakonfig declarations for rejester.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import yakonfig

config_name = 'rejester'

default_config = {
    'app_name': 'rejester',
}

def add_arguments(parser):
    parser.add_argument('--app-name', metavar='NAME',
                        help='application name prefix')
    parser.add_argument('--registry-address', metavar='HOST:PORT',
                        action='append', dest='registry_addresses',
                        help='location of the Redis registry server')

runtime_keys = {
    'app_name': 'app_name',
    'registry_addresses': 'registry_addresses',
    'namespace': 'namespace',
}

def check_config(config, name):
    for k in ['registry_addresses', 'app_name', 'namespace']:
        if k not in config or config[k] is None:
            raise yakonfig.ConfigurationError(
                '{} requires configuration for {}'
                .format(name, k))
    if len(config['registry_addresses']) == 0:
        raise yakonfig.ConfigurationError(
            '{} requires at least one registry_addresses'
            .format(name))
    for addr in config['registry_addresses']:
        if ':' not in addr:
            raise yakonfig.ConfigurationError(
                '{} registry_addresses must be HOST:PORT, not {!r}'
                .format(name, addr))
