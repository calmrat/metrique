#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.utils
~~~~~~~~~~~~~~~~~

This module contains utility functions shared between
metrique sub-modules
'''

from datetime import datetime
from dateutil.parser import parse as dt_parse
import logging
import os
import pytz
import simplejson as json
import sys

from metriqueu.utils import dt2ts

logger = logging.getLogger(__name__)

json_encoder = json.JSONEncoder()

DEFAULT_PKGS = ['metriquec.cubes']


def csv2list(csv, delimiter=','):
    ''' convert a str(csv,csv) into a list of sorted strings

    :param csv: comma separated value string to convert
    :param delimiter: character(s) separating the values (default: ',')
    '''
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

    :param cube: cube name to use when searching for cube pkg.mod.class to load
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
        # if that fails, try to guess the cube module
        # based on cube 'standard naming convention'
        # ie, group_cube -> from group.cube import CubeClass
        _pkg, _mod, _cls = cube_pkg_mod_cls(cube)
        mcubes = __import__('%s.%s.%s' % (pkg, _pkg, _mod),
                            fromlist=[_cls])
        return getattr(mcubes, _cls)


def get_cube(cube, init=False, config=None, pkgs=None, cube_paths=None,
             **kwargs):
    '''
    Dynamically locate and load a metrique cube

    :param cube: name of the cube class to import from given module
    :param init: flag to request initialized instance or uninitialized class
    :param config: config dict to pass on initialization (implies init=True)
    :param pkgs: list of package names to search for the cubes in
    :param cube_path: additional paths to search for modules in (sys.path)
    :param kwargs: additional kwargs to pass to cube during initialization
    '''
    config = config or {}
    config.update(**kwargs)
    pkgs = pkgs or config.get('cube_pkgs', ['cubes'])
    pkgs = [pkgs] if isinstance(pkgs, basestring) else pkgs
    # search in the given path too, if provided
    cube_paths = cube_paths if cube_paths else config.get('cube_paths', [])
    cube_paths_is_basestring = isinstance(cube_paths, basestring)
    cube_paths = [cube_paths] if cube_paths_is_basestring else cube_paths
    cube_paths = [os.path.expanduser(path) for path in cube_paths]

    # append paths which don't already exist in sys.path to sys.path
    [sys.path.append(path) for path in cube_paths if path not in sys.path]

    pkgs = pkgs + DEFAULT_PKGS
    err = False
    for pkg in pkgs:
        try:
            _cube = _load_cube_pkg(pkg, cube)
        except ImportError as err:
            _cube = None
        if _cube:
            break
    else:
        sys.stderr.write('WARNING: %s\n' % err)
        raise RuntimeError('"%s" not found! %s; %s \n%s)' % (
            cube, pkgs, cube_paths, sys.path))

    if init:
        _cube = _cube(**config)
    return _cube


def get_timezone_converter(from_timezone):
    '''
    return a function that converts a given
    datetime object from a timezone to utc

    :param from_timezone: timezone name as string
    '''
    utc = pytz.utc
    from_tz = pytz.timezone(from_timezone)

    def timezone_converter(self, dt):
        if dt is None:
            return None
        elif isinstance(dt, basestring):
            dt = dt_parse(dt)
        if dt.tzinfo:
            # datetime instance already has tzinfo set
            # WARN if not dt.tzinfo == from_tz?
            try:
                dt = dt.astimezone(utc)
            except ValueError:
                # date has invalid timezone; replace with expected
                dt = dt.replace(tzinfo=from_tz)
                dt = dt.astimezone(utc)
        else:
            # set tzinfo as from_tz then convert to utc
            dt = from_tz.localize(dt).astimezone(utc)
        return dt
    return timezone_converter


def json_encode(obj):
    '''
    Convert datetime.datetime to timestamp

    :param obj: value to (possibly) convert
    '''
    if isinstance(obj, datetime):
        return dt2ts(obj)
    else:
        return json_encoder.default(obj)
