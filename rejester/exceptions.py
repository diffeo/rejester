'''
http://github.com/diffeo/rejester

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
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

class NoSuchWorkSpecError(RejesterException):
    """A `TaskMaster` function was called with a nonexistent work spec"""
    pass

class ProgrammerError(RejesterException):
    pass

class PriorityRangeEmpty(RejesterException):
    '''
    given the priority_min/max, no item is available to be returned
    '''
    pass

class LostLease(RejesterException):
    '''worker waited too long between calls to update and another worker
got the WorkItem'''
    pass

class ItemInUseError(RejesterException):
    '''tried to add an item to a queue that was already in use'''
    pass
