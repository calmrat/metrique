#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from datetime import datetime
from dateutil.parser import parse as dt_parse
import os
import pytz
import simplejson as json
import sys

from metriqueu.utils import dt2ts

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


def _load_cube_pkg(pkg, cube):
    try:
        # First, assume the cube module is available
        # with the name exactly as written
        mcubes = __import__(pkg, fromlist=[cube])
        return getattr(mcubes, cube)
    except AttributeError:
        try:
            # if that fails, try to guess the cube module
            # based on cube 'standard naming convention'
            # ie, group_cube -> from group.cube import CubeClass
            _pkg, _mod, _cls = cube_pkg_mod_cls(cube)
            mcubes = __import__('%s.%s.%s' % (pkg, _pkg, _mod),
                                fromlist=[_cls])
            return getattr(mcubes, _cls)
        except ImportError:
            pass
    except ImportError:
        pass


def get_cube(cube, init=False, config=None, pkgs=None, cube_paths=None,
             **kwargs):
    '''
    Dynamically locate and load a metrique cube

    :param string cube:
        Name of the cube Class to be imported from given module (eg, 'Build')
    :param bool init:
        Flag to request initialized instance or uninitialized class (default)
    :param dict config:
        dictionary to use as config for initialized cube instance
        Setting config implies init=True
    :param list pkgs:
        list of module names to search for the cubes in
    :param string path:
        additional path to search for modules in (added to sys.path)
    '''
    if not config:
        config = {}
    config.update(**kwargs)
    if not pkgs:
        pkgs = config.get('cube_pkgs', ['cubes'])
    if isinstance(pkgs, basestring):
        pkgs = [pkgs]

    # search in the given path too, if provided
    if not cube_paths:
        cube_paths = config.get('cube_paths', [])
    if isinstance(cube_paths, basestring):
        cube_paths = [cube_paths]
    for path in cube_paths:
        path = os.path.expanduser(path)
        if path not in sys.path:
            sys.path.append(path)

    pkgs = pkgs + ['metriquec.cubes']
    for pkg in pkgs:
        _cube = _load_cube_pkg(pkg, cube)
        if _cube:
            break
    else:
        raise RuntimeError('"%s" not found! %s; %s \n%s)' % (
            cube, pkgs, cube_paths, sys.path))

    if init or config:
        _cube = _cube(**config)
    return _cube


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
