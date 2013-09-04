#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

# FIXME: use random sha256 instead? to remove bson dep for metrique?
from bson.objectid import ObjectId
from calendar import timegm
from decorator import decorator
from datetime import datetime as dt
from dateutil.parser import parse as dt_parse
import pytz
import os
import simplejson as json
import sys

from metrique.config import DEFAULT_SYSTEM_CUBES_PATH
from metrique.config import DEFAULT_CLIENT_CUBES_PATH

json_encoder = json.JSONEncoder()


def _memo(func, *args, **kw):
    # sort and convert list items to tuple for hashability
    if type(kw) is list:
        kw = frozenset(kw)
    args = list(args)
    for k, arg in enumerate(args):
        if type(arg) is list:
            args[k] = frozenset(arg)
    # frozenset is used to ensure hashability
    key = frozenset(args), frozenset(kw.iteritems())
    cache = func.cache  # attributed added by memoize
    if key in cache:
        return cache[key]
    else:
        cache[key] = result = func(*args, **kw)
    return result


def memo(f):
    ''' memoize function output '''
    f.cache = {}
    return decorator(_memo, f)


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


def milli2sec(ts):
    ''' normalize timestamps to timestamp int's (seconds) '''
    if not ts:
        return ts
    return float(float(ts) / 1000.)  # convert milli to seconds


def batch_gen(data, batch_size):
    '''
    Usage::
        for batch in batch_gen(iter, 100):
            do_something(batch)
    '''
    if not data:
        return

    if batch_size == -1:
        # override: yield the whole list
        yield data

    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def ts2dt(ts, milli=False, tz_aware=True):
    ''' convert timestamp int's (seconds) to datetime objects '''
    if not ts:
        return ts
    elif isinstance(ts, dt):  # its a dt already
        return ts
    # ts must be float and in seconds
    elif milli:
        ts = float(ts) / 1000.  # convert milli to seconds
    else:
        ts = float(ts)  # already in seconds
    if tz_aware:
        return dt.fromtimestamp(ts, tz=pytz.utc)
    else:
        return dt.utcfromtimestamp(ts)


def dt2ts(dt):
    ''' convert datetime objects to timestamp int's (seconds) '''
    if isinstance(dt, (int, long, float, complex)):  # its a ts already
        return dt
    elif isinstance(dt, basestring):  # convert to datetime first
        return dt2ts(dt_parse(dt))
    else:
        return timegm(dt.timetuple())


def new_oid():
    '''
    Creates a new ObjectId and casts it to string,
    so it's easily serializable
    '''
    return str(ObjectId())


def json_encode(obj):
    '''
    Convert datetime.datetime to timestamp
    '''
    if isinstance(obj, dt):
        return dt2ts(obj)
    else:
        return json_encoder.default(obj)


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

    :param string module:
        Module name (eg, 'jkns.build')
    :param string cube:
        Name of the cube Class to be imported from given module (eg, 'Build')
    '''
    if not path:
        path = DEFAULT_CLIENT_CUBES_PATH
    set_cube_path(path)
    pkg, mod, cls = cube_pkg_mod_cls(cube)
    _pkg = __import__(pkg, fromlist=[mod])
    _mod = getattr(_pkg, mod)
    _cls = getattr(_mod, cls)
    return _cls
