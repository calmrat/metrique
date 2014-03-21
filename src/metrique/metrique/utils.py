#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.utils
~~~~~~~~~~~~~~~~~

This module contains utility functions shared between
metrique sub-modules
'''

import logging
logger = logging.getLogger(__name__)

from calendar import timegm
from datetime import datetime
from dateutil.parser import parse as dt_parse
from hashlib import sha1
import locale
import os
import pql
import pytz
import re
import simplejson as json
import sys

json_encoder = json.JSONEncoder()

DEFAULT_PKGS = ['metrique.cubes']

SHA1_HEXDIGEST = lambda o: sha1(repr(o)).hexdigest()


def batch_gen(data, batch_size):
    '''
    Usage::
        for batch in batch_gen(iter, 100):
            do_something(batch)
    '''
    if not data:
        return

    if batch_size <= 0:
        # override: yield the whole list
        yield data

    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def clear_stale_pids(pids, pid_dir, prefix='metriqued'):
    'check for and remove any pids which have no corresponding process'
    procs = os.listdir('/proc')
    running = [pid for pid in pids if pid in procs]
    _running = []
    for pid in pids:
        if pid in running:
            _running.append(pid)
        else:
            pid_file = '%s.%s.pid' % (prefix, pid)
            path = os.path.join(pid_dir, pid_file)
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    # FIXME: LOG!
                    pass
    return _running


def csv2list(item):
    if isinstance(item, basestring):
        items = item.split(',')
    else:
        raise TypeError('Expected a csv string')
    items = [s.strip() for s in items]
    return items


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


def dt2ts(dt, drop_micro=False, strict=False):
    ''' convert datetime objects to timestamp seconds (float) '''
    # the equals check to 'NaT' is hack to avoid adding pandas as a dependency
    if repr(dt) == 'NaT' or not dt:
        if strict:
            raise ValueError("invalid datetime '%s'" % dt)
        else:
            return None
    elif isinstance(dt, (int, long, float)):  # its a ts already
        ts = dt
    elif isinstance(dt, basestring):  # convert to datetime first
        ts = dt2ts(dt_parse(dt))
    else:
        # FIXME: microseconds/milliseconds are being dropped!
        # see: http://stackoverflow.com/questions/7031031
        # for possible solution?
        ts = timegm(dt.timetuple())
    if drop_micro:
        return float(int(ts))
    else:
        return float(ts)


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


def get_pids(pid_dir, prefix='metriqued', clear_stale=True):
    pid_dir = os.path.expanduser(pid_dir)
    # eg, server.22325.pid, server.23526.pid
    pids = []
    for f in os.listdir(pid_dir):
        pid_re = re.search(r'%s.(\d+).pid' % prefix, f)
        if pid_re:
            pids.append(pid_re.groups()[0])
    if clear_stale:
        pids = clear_stale_pids(pids, pid_dir, prefix)
    return map(int, pids)


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


def jsonhash(obj, root=True, exclude=None, hash_func=None):
    '''
    calculate the objects hash based on all field values
    '''
    if not hash_func:
        hash_func = SHA1_HEXDIGEST
    if isinstance(obj, dict):
        obj = obj.copy()  # don't affect the ref'd obj passed in
        keys = set(obj.keys())
        if root and exclude:
            [obj.__delitem__(f) for f in exclude if f in keys]
        # frozenset's don't guarantee order; use sorted tuples
        # which means different python interpreters can return
        # back frozensets with different hash values even when
        # the content of the object is exactly the same
        result = sorted(
            (k, jsonhash(v, False)) for k, v in obj.items())
    elif isinstance(obj, list):
        # FIXME: should obj be sorted for consistent hashes?
        # when the object is the same, just different list order?
        result = tuple(jsonhash(e, False) for e in obj)
    else:
        result = obj
    if root:
        result = hash_func(repr(result))
    return result


def date_pql_string(date):
    '''
    Generate a new pql date query component that can be used to
    query for date (range) specific data in cubes.

    :param date: metrique date (range) to apply to pql query

    If date is None, the resulting query will be a current value
    only query (_end == None)

    The tilde '~' symbol is used as a date range separated.

    A tilde by itself will mean 'all dates ranges possible'
    and will therefore search all objects irrelevant of it's
    _end date timestamp.

    A date on the left with a tilde but no date on the right
    will generate a query where the date range starts
    at the date provide and ends 'today'.
    ie, from date -> now.

    A date on the right with a tilde but no date on the left
    will generate a query where the date range starts from
    the first date available in the past (oldest) and ends
    on the date provided.
    ie, from beginning of known time -> date.

    A date on both the left and right will be a simple date
    range query where the date range starts from the date
    on the left and ends on the date on the right.
    ie, from date to date.
    '''
    if date is None:
        return '_end == None'
    if date == '~':
        return ''

    before = lambda d: '_start <= %f' % dt2ts(d)
    after = lambda d: '(_end >= %f or _end == None)' % dt2ts(d)
    split = date.split('~')
    # replace all occurances of 'T' with ' '
    # this is used for when datetime is passed in
    # like YYYY-MM-DDTHH:MM:SS instead of
    #      YYYY-MM-DD HH:MM:SS as expected
    # and drop all occurances of 'timezone' like substring
    split = [re.sub('\+\d\d:\d\d', '', d.replace('T', ' ')) for d in split]
    if len(split) == 1:
        # 'dt'
        return '%s and %s' % (before(split[0]), after(split[0]))
    elif split[0] == '':
        # '~dt'
        return before(split[1])
    elif split[1] == '':
        # 'dt~'
        return after(split[0])
    else:
        # 'dt~dt'
        return '%s and %s' % (before(split[1]), after(split[0]))


def parse_pql_query(query, date=None):
    '''
    Given a pql based query string, parse it using
    pql.SchemaFreeParser and return the resulting
    pymongo 'spec' dictionary.

    :param query: pql query
    '''
    logger.debug('pql query: %s' % query)
    query = query_add_date(query, date)
    if not query:
        return {}
    if not isinstance(query, basestring):
        raise TypeError("query expected as a string")
    pql_parser = pql.SchemaFreeParser()
    try:
        spec = pql_parser.parse(query)
    except Exception as e:
        raise SyntaxError("Invalid Query (%s)" % str(e))
    #logger.debug('mongo spec: %s' % spec)
    return spec


def query_add_date(query, date):
    '''
    Take an existing pql query and append a date (range)
    limiter.

    :param query: pql query
    :param date: metrique date (range) to append
    '''
    date_pql = date_pql_string(date)
    if query and date_pql:
        return '%s and %s' % (query, date_pql)
    return query or date_pql


def to_encoding(ustring, encoding=None):
    encoding = encoding or locale.getpreferredencoding()
    if isinstance(ustring, basestring):
        if not isinstance(ustring, unicode):
            return unicode(ustring, encoding, 'replace')
        else:
            return ustring.encode(encoding, 'replace')
    else:
        raise ValueError('basestring type required')


def ts2dt(ts, milli=False, tz_aware=True):
    ''' convert timestamp int's (seconds) to datetime objects '''
    # anything already a datetime will still be returned
    # tz_aware, if set to true
    if not ts or ts != ts:
        return None  # its not a timestamp
    elif isinstance(ts, datetime):
        pass
    elif milli:
        ts = float(ts) / 1000.  # convert milli to seconds
    else:
        ts = float(ts)  # already in seconds
    if tz_aware:
        if isinstance(ts, datetime):
            ts.replace(tzinfo=pytz.utc)
            return ts
        else:
            return datetime.fromtimestamp(ts, tz=pytz.utc)
    else:
        if isinstance(ts, datetime):
            return ts
        else:
            return datetime.utcfromtimestamp(ts)


def utcnow(as_datetime=False, tz_aware=False, drop_micro=False):
    if tz_aware:
        now = datetime.now(pytz.UTC)
    else:
        now = datetime.utcnow()
    if drop_micro:
        now = now.replace(microsecond=0)
    if as_datetime:
        return now
    else:
        return dt2ts(now, drop_micro)
