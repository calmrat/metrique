#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Utility functions for testing metrique / db integration
'''

from functools import wraps


def runner(func):
    @wraps(func)
    def _runner(*args, **kwargs):
        # DO SOMETHING BEFORE
        func()
        # DO SOMETHING AFTER
    return _runner
