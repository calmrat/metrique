#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from datetime import datetime
from dateutil.parser import parse as dt_parse
import os
import pytz
import simplejson as json
import sys
from distutils.sysconfig import get_python_lib

from metriqueu.utils import dt2ts

CLIENT_CUBES_PATH = '~/.metrique/cubes/'
SYSTEM_CUBES_PATH = os.path.join(get_python_lib(), 'metriquec/')

if CLIENT_CUBES_PATH not in sys.path:
    # also append default client cubes path for easy/consistent importing
    sys.path.append(CLIENT_CUBES_PATH)

if SYSTEM_CUBES_PATH not in sys.path:
    # also append system cubes path for easy/consistent importing
    sys.path.append(SYSTEM_CUBES_PATH)

json_encoder = json.JSONEncoder()


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


def get_cube(cube, path=None, init=False, config=None):
    '''
    Wraps __import__ to dynamically locate and load a client cube.

    :param string cube:
        Name of the cube Class to be imported from given module (eg, 'Build')
    :param string path:
        path to look for cubes (eg '~/.metrique/cubes/')
    '''
    set_cube_path(path)
    pkg, mod, _cls = cube_pkg_mod_cls(cube)
    _pkg = __import__(pkg, fromlist=[mod])
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


def set_cube_path(path=None):
    '''
    Add a given path to sys.path to enable calls to import.

    If no path provided, default to making *metrique.client.cubes*
    get added to the current namespace.
    '''
    if path:
        path = os.path.expanduser(path)
        if path not in sys.path:
            sys.path.append(path)
    return sys.path
