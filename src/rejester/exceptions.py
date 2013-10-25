'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

class RejesterException(Exception):
    'base exception for rejester package'
    pass

class EnvironmentError(RejesterException):
    '''indicates that the registry lost a lock or experienced a similar
failure that probably indicates a network or remote server failure'''
    pass

class LockError(RejesterException):
    'attempt to get a lock exceeded acquire time (atime)'
    pass

class ProgrammerError(RejesterException):
    pass

class PriorityRangeEmpty(RejesterException):
    '''
    given the priority_min/max, no item is available to be returned
    '''
    pass
