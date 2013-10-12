'''
Logging utilities for rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import os
import sys
import logging
import traceback

class FixedWidthFormatter(logging.Formatter):
    '''
    Provides fixed-width formatting for console logging, see:
    http://stackoverflow.com/questions/6692248/python-logging-string-formatting
    '''
    filename_width = 17
    levelname_width = 8

    def format(self, record):
        max_filename_width = self.filename_width - 3 - len(str(record.lineno))
        filename = record.filename
        if len(record.filename) > max_filename_width:
            filename = record.filename[:max_filename_width]
        a = "%s:%s" % (filename, record.lineno)
        record.fixed_width_filename_lineno = a.ljust(self.filename_width)
        levelname = record.levelname
        levelname_padding = ' ' * (self.levelname_width - len(levelname))
        record.fixed_width_levelname = levelname + levelname_padding
        return super(FixedWidthFormatter, self).format(record)

ch = logging.StreamHandler()
formatter = FixedWidthFormatter('%(asctime)s pid=%(process)d %(fixed_width_filename_lineno)s %(fixed_width_levelname)s %(message)s')
ch.setLevel('DEBUG')
ch.setFormatter(formatter)

logger = logging.getLogger('registry')
logger.setLevel('DEBUG')
logger.handlers = []
logger.addHandler(ch)

def reset_log_level( log_level ):
    '''
    set logging framework to log_level

    initial default is DEBUG
    '''
    global ch, logger
    ch.setLevel( log_level )
    logger.setLevel( log_level )
