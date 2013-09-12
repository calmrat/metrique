#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from datetime import datetime as dt
import os
import pytz
import simplejson as json
import sys

from metriqueu.defaults import DEFAULT_SYSTEM_CUBES_PATH
from metriqueu.defaults import DEFAULT_CLIENT_CUBES_PATH
from metriqueu.utils import dt2ts

json_encoder = json.JSONEncoder()


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
    cls = ''.join([s[0].upper() + s[1:] for s in _cube[1:]])
    return pkg, mod, cls


def set_cube_path(path=None):
    '''
    Add a given path to sys.path to enable calls to import.

    If no path provided, default to making *metrique.client.cubes*
    get added to the current namespace.
    '''
    if not path:
        path = DEFAULT_CLIENT_CUBES_PATH
    if path:
        path = os.path.expanduser(path)
        sys.path.append(path)
    # also append system cubes path for easy/consistent importing
    sys.path.append(DEFAULT_SYSTEM_CUBES_PATH)
    sys.path = sorted(set(sys.path))  # make sure we don't have dups...
    return sys.path


def get_cube(cube, path=None):
    '''
    Wraps __import__ to dynamically locate and load a client cube.

    :param string cube:
        Name of the cube Class to be imported from given module (eg, 'Build')
    :param string path:
        path to look for cubes (eg '~/.metrique/cubes/')
    '''
    if not path:
        path = DEFAULT_CLIENT_CUBES_PATH
    set_cube_path(path)
    pkg, mod, cls = cube_pkg_mod_cls(cube)
    _pkg = __import__(pkg, fromlist=[mod])
    _mod = getattr(_pkg, mod)
    _cls = getattr(_mod, cls)
    return _cls


def perc(numerator, denominator):
    return (float(numerator) / denominator) * 100


def doublequote(item):
    ''' convert a given obj to string, double-quoted'''
    return '"%s"' % item


def list2csv(_list, quote=False):
    ''' convert a list of objects into a csv string '''
    if quote:
        _list = map(doublequote, _list)
    return ','.join(map(str, _list))


def csv2list(csv, delimiter=','):
    ''' convert a str(csv,csv) into a list of strings '''
    if type(csv) in [list, tuple, set]:
        return list(csv)
    elif csv:
        return [s.strip() for s in csv.split(delimiter)]
    elif csv is None:
        return []
    else:
        raise ValueError(
            "Failed to convert csv string to list; got %s" % csv)


def get_timezone_converter(from_timezone):
    '''
    return a function that converts a given
    datetime object from a timezone to utc
    '''
    utc = pytz.utc
    from_tz = pytz.timezone(from_timezone)

    def timezone_converter(self, dt):
        try:
            return from_tz.localize(dt).astimezone(utc)
        except Exception:
            return None
    return timezone_converter


def json_encode(obj):
    '''
    Convert datetime.datetime to timestamp
    '''
    if isinstance(obj, dt):
        return dt2ts(obj)
    else:
        return json_encoder.default(obj)
