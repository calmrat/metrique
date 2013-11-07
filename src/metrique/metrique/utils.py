#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from datetime import datetime
from dateutil.parser import parse as dt_parse
import os
import pytz
import simplejson as json
import site
import sys

import metriquec

from metriqueu.utils import dt2ts

USER_CUBES_SITEDIR = '~/.metrique'
SYSTEM_CUBES_SITEDIR = metriquec.__path__[0]
json_encoder = json.JSONEncoder()


def addsitedirs(path=None):
    '''
    include these dirs along with any dirs defined in
    .pth files found into sys.path
    '''
    site_dirs = [USER_CUBES_SITEDIR, SYSTEM_CUBES_SITEDIR]
    if path:
        site_dirs.append(path)
    for site_dir in site_dirs:
        site_dir = os.path.expanduser(site_dir)
        if not os.path.exists(site_dir):
            raise IOError("invalid site_dir: %s" % site_dir)
        elif site_dir not in sys.path:
            # load .pth files, if available
            site.addsitedir(site_dir)
        else:
            pass


def csv2list(csv, delimiter=','):
    ''' convert a str(csv,csv) into a list of sorted strings '''
    if type(csv) in [list, tuple, set]:
        result = list(map(str, csv))
    elif isinstance(csv, basestring):
        result = [s.strip() for s in csv.split(delimiter)]
    elif csv is None:
        result = []
    else:
        raise TypeError(
            "Failed to convert csv string to list; got %s" % csv)
    return sorted(result)


def cube_pkg_mod_cls(cube):
    '''
    Used to dynamically importing cube classes
    based on string slug name.

    Converts 'pkg_mod' -> pkg, mod, Cls

    eg: tw_tweet -> tw, tweet, Tweet

    Assumes `Metrique Cube Naming Convention` is used
    '''
    _cube = cube.split('_')
    pkg = _cube[0]
    mod = '_'.join(_cube[1:])
    _cls = ''.join([s[0].upper() + s[1:] for s in _cube[1:]])
    return pkg, mod, _cls


def get_cube(cube, init=False, config=None, path=None):
    '''
    Wraps __import__ to dynamically locate and load a client cube.

    :param string cube:
        Name of the cube Class to be imported from given module (eg, 'Build')
    :param bool init:
        Flag to request initialized instance or uninitialized class (default)
    :param dict config:
        dictionary to use as config for initialized cube instance

    looks for *.pth file (eg, cubes.pth) which point to the
    directory where top-level cubes modules live for system
    cubes and user cubes.

    Import all the cube classes into current namespace so we can
    attempt an import and return back the class object.
    '''
    if not config:
        config = {}

    # include these dirs along with any dirs defined in
    # .pth files found into sys.path
    addsitedirs(path=path)

    pkg, mod, _cls = cube_pkg_mod_cls(cube)
    try:
        _pkg = __import__(pkg, fromlist=[mod])
    except ImportError as e:
        raise ImportError('%s; (%s)' % (e, ', '.join(sys.path)))
    _mod = getattr(_pkg, mod)
    cube = getattr(_mod, _cls)

    if init:
        if config:
            cube = cube(**config)
        else:
            cube = cube()
    return cube


def get_timezone_converter(from_timezone):
    '''
    return a function that converts a given
    datetime object from a timezone to utc
    '''
    utc = pytz.utc
    from_tz = pytz.timezone(from_timezone)

    def timezone_converter(self, dt):
        if dt is None:
            return None
        elif isinstance(dt, basestring):
            dt = dt_parse(dt)
        return from_tz.localize(dt).astimezone(utc)
    return timezone_converter


def json_encode(obj):
    '''
    Convert datetime.datetime to timestamp
    '''
    if isinstance(obj, datetime):
        return dt2ts(obj)
    else:
        return json_encoder.default(obj)
