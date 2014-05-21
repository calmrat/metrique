#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Utility functions for testing metrique / db integration
'''

from functools import wraps
import os
import shutil


def runner(func):
    @wraps(func)
    def _runner(*args, **kwargs):
        # DO SOMETHING BEFORE
        func()
        # DO SOMETHING AFTER
    return _runner


def is_in(obj, key, value):
    return bool(key in obj and obj[key] == value)


def set_env():
    # if HOME environment variable is set, use that
    # useful when running 'as user' with root (supervisord)
    home = os.environ['METRIQUE_HOME'] = os.environ.get(
        'METRIQUE_HOME', os.path.expanduser('~/'))
    prefix = os.environ['METRIQUE_PREFIX'] = os.environ.get(
        'METRIQUE_PREFIX', os.path.join(home, '.metrique'))
    os.environ['METRIQUE_ETC'] = os.environ.get(
        'METRIQUE_ETC', os.path.join(prefix, 'etc'))
    os.environ['METRIQUE_LOGS'] = os.environ.get(
        'METRIQUE_LOGS', os.path.join(prefix, 'logs'))
    os.environ['METRIQUE_TMP'] = os.environ.get(
        'METRIQUE_TMP', os.path.join(prefix, 'tmp'))
    os.environ['METRIQUE_CACHE'] = os.environ.get(
        'METRIQUE_CACHE', os.path.join(prefix, 'cache'))
    return os.environ


def qremove(path):
    if os.path.isfile(path):
        rm = os.remove
    else:
        rm = shutil.rmtree
    try:
        rm(path)
    except Exception:
        print 'Failed to remove %s' % path
