#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.utils
~~~~~~~~~~~~~~~~~

This module contains utility functions shared between
metrique sub-modules
'''

from __future__ import unicode_literals

import logging
logger = logging.getLogger('metrique')

import anyconfig
anyconfig.set_loglevel(logging.WARN)  # too noisy...
from calendar import timegm
import collections
from copy import deepcopy
import cPickle
import cProfile as profiler
from datetime import datetime
from dateutil.parser import parse as dt_parse

try:
    from dulwich.repo import Repo
    HAS_DULWICH = True
except ImportError:
    HAS_DULWICH = False
    logger.warn('dulwich module is not installed!')

import gc
import glob
from hashlib import sha1
from inspect import isfunction
import itertools
import os
import pandas as pd
import pstats
import pytz
import re
import shelve
import shlex
import signal
import simplejson as json
import subprocess
import sys
import time
import urllib

json_encoder = json.JSONEncoder()

DEFAULT_PKGS = ['metrique.cubes']

SHA1_HEXDIGEST = lambda o: sha1(repr(o)).hexdigest()
UTC = pytz.utc

LOGS_DIR = os.environ.get('METRIQUE_LOGS')
CACHE_DIR = os.environ.get('METRIQUE_CACHE') or '/tmp'


def batch_gen(data, batch_size):
    '''
    Usage::
        for batch in batch_gen(iter, 100):
            do_something(batch)
    '''
    data = data or []
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def clear_stale_pids(pids, pid_dir='/tmp', prefix=''):
    'check for and remove any pids which have no corresponding process'
    pids = [unicode(pid) for pid in pids]
    procs = os.listdir('/proc')
    running = [pid for pid in pids if pid in procs]
    logger.warn(
        "Found %s pids running: %s" % (len(running),
                                       running))
    prefix = '%s.' % prefix if prefix else ''
    for pid in pids:
        # remove non-running procs
        if pid in running:
            continue
        pid_file = '%s%s.pid' % (prefix, pid)
        path = os.path.join(pid_dir, pid_file)
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError as e:
                logger.warn(e)
    return running


def configure(options=None, defaults=None, config_file=None,
              section_key=None, update=None, section_only=False):

    config = load_config(config_file)
    config = rupdate(config, deepcopy(update or {}))

    opts = deepcopy(options or {})
    defs = deepcopy(defaults or {})

    sk = section_key

    if not sk and (section_key or section_only):
        raise KeyError('section %s not set' % sk)

    # work only with the given section, if specified
    working_config = config[sk] if sk in config else config

    # if section key is already configured, ie, we initiated with
    # config set already, set options not set as None
    for k, v in opts.iteritems():
        if v is not None:
            working_config.update({k: v})
    # and update and defaults where current config
    # key doesn't exist yet or is set to None
    for k, v in defs.iteritems():
        if working_config.get(k) is None:
            working_config.update({k: v})

    if section_only:
        return working_config
    else:
        if sk:
            config[sk] = working_config
        else:
            config = working_config
        return config


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


def _debug_set_level(logger, level):
    # NOTE: int(0) == bool(False) is True
    if level in [-1, 0, False]:
        logger.setLevel(logging.WARN)
    elif level in [None]:
        logger.setLevel(logging.INFO)
    elif level is True:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(int(level))
    return logger


def debug_setup(logger=None, level=None, log2file=None,
                log_file=None, log_format=None, log_dir=None,
                log2stdout=None, truncate=False):
    '''
    Local object instance logger setup.

    Verbosity levels are determined as such::

        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)

    If (level == 2) `logging.DEBUG` will be set even for
    the "root logger".

    Configuration options available for customized logger behaivor:
        * debug (bool)
        * log2stdout (bool)
        * log2file (bool)
        * log_file (path)
    '''
    log2stdout = False if log2stdout is None else log2stdout
    if log_format and isinstance(log_format, basestring):
        log_format = logging.Formatter(log_format, "%Y%m%dT%H%M%S")
    _log_format = "%(name)s.%(process)s:%(asctime)s:%(message)s"
    _log_format = logging.Formatter(log_format, "%Y%m%dT%H%M%S")
    log_format = log_format or _log_format

    log2file = True if log2file is None else log2file
    log_file = log_file or 'metrique.log'
    log_dir = log_dir or LOGS_DIR or ''
    log_file = os.path.join(log_dir, log_file)

    logger = logger or 'metrique'
    if isinstance(logger, basestring):
        logger = logging.getLogger(logger)
    else:
        logger = logger or logging.getLogger(logger)
    logger.propagate = 0
    logger.handlers = []
    if log2file and log_file:
        if truncate:
            # clear the existing data before writing (truncate)
            open(log_file, 'w+').close()
        hdlr = logging.FileHandler(log_file)
        hdlr.setFormatter(log_format)
        logger.addHandler(hdlr)
    else:
        log2stdout = True
    if log2stdout:
        hdlr = logging.StreamHandler()
        hdlr.setFormatter(log_format)
        logger.addHandler(hdlr)
    logger = _debug_set_level(logger, level)
    return logger


def dt2ts(dt, drop_micro=False):
    ''' convert datetime objects to timestamp seconds (float) '''
    # the equals check to 'NaT' is hack to avoid adding pandas as a dependency
    if is_null(dt):
        return None
    elif isinstance(dt, (int, long, float)):  # its a ts already
        ts = dt
    elif isinstance(dt, basestring):  # convert to datetime first
        parsed_dt = dt_parse(dt)
        ts = dt2ts(parsed_dt)
    else:
        # FIXME: microseconds/milliseconds are being dropped!
        # see: http://stackoverflow.com/questions/7031031
        # for possible solution?
        ts = timegm(dt.timetuple())
    if drop_micro:
        return float(int(ts))
    else:
        return float(ts)


def get_cube(cube, init=False, pkgs=None, cube_paths=None, config=None,
             backends=None, **kwargs):
    '''
    Dynamically locate and load a metrique cube

    :param cube: name of the cube class to import from given module
    :param init: flag to request initialized instance or uninitialized class
    :param config: config dict to pass on initialization (implies init=True)
    :param pkgs: list of package names to search for the cubes in
    :param cube_path: additional paths to search for modules in (sys.path)
    :param kwargs: additional kwargs to pass to cube during initialization
    '''
    pkgs = pkgs or ['cubes']
    pkgs = [pkgs] if isinstance(pkgs, basestring) else pkgs
    # search in the given path too, if provided
    cube_paths = cube_paths or []
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
        logger.error(err)
        raise RuntimeError('"%s" not found! %s; %s \n%s)' % (
            cube, pkgs, cube_paths, sys.path))

    if init:
        _cube = _cube(config=config, **kwargs)
    return _cube


def get_pids(pid_dir, prefix='', clear_stale=True):
    pid_dir = os.path.expanduser(pid_dir)
    # eg, server.22325.pid, server.23526.pid
    pids = []
    prefix = '%s.' % prefix if prefix else ''
    for f in os.listdir(pid_dir):
        pid_re = re.search(r'%s(\d+).pid' % prefix, f)
        if pid_re:
            pids.append(pid_re.groups()[0])
    if clear_stale:
        pids = clear_stale_pids(pids, pid_dir, prefix)
    return map(int, pids)


def get_timezone_converter(from_timezone, tz_aware=False):
    '''
    return a function that converts a given
    datetime object from a timezone to utc

    :param from_timezone: timezone name as string
    '''
    from_tz = pytz.timezone(from_timezone)

    def timezone_converter(dt):
        if dt is None:
            return None
        elif isinstance(dt, basestring):
            dt = dt_parse(dt)
        if dt.tzinfo:
            # datetime instance already has tzinfo set
            # WARN if not dt.tzinfo == from_tz?
            try:
                dt = dt.astimezone(UTC)
            except ValueError:
                # date has invalid timezone; replace with expected
                dt = dt.replace(tzinfo=from_tz)
                dt = dt.astimezone(UTC)
        else:
            # set tzinfo as from_tz then convert to utc
            dt = from_tz.localize(dt).astimezone(UTC)
        if not tz_aware:
            dt = dt.replace(tzinfo=None)
        return dt
    return timezone_converter


def git_clone(uri, pull=True, reflect=False, cache_dir=None):
    '''
    Given a git repo, clone (cache) it locally.

    :param uri: git repo uri
    :param pull: whether to pull after cloning (or loading cache)
    '''
    cache_dir = cache_dir or CACHE_DIR
    # make the uri safe for filesystems
    repo_path = os.path.expanduser(os.path.join(cache_dir, safestr(uri)))
    if not os.path.exists(repo_path):
        from_cache = False
        logger.info(
            'Locally caching git repo [%s] to [%s]' % (uri, repo_path))
        cmd = 'git clone %s %s' % (uri, repo_path)
        sys_call(cmd)
    else:
        from_cache = True
        logger.info(
            'GIT repo loaded from local cache [%s])' % (repo_path))
    if pull and not from_cache:
        os.chdir(repo_path)
        cmd = 'git pull'
        sys_call(cmd)
    if reflect:
        if not HAS_DULWICH:
            raise RuntimeError("`pip install dulwich` required!")
        return Repo(repo_path)
    else:
        return repo_path


def is_null(value):
    if isinstance(value, basestring):
        value = value.strip()
    elif hasattr(value, 'empty'):
        # dataframes must check for .empty
        # since they don't define truth value attr
        # take the negative, since below we're
        # checking for cases where value 'is_null'
        value = not bool(value.empty)
    else:
        pass
    return bool(
        not value or
        value != value or
        repr(value) == 'NaT')


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
        keys = set(obj.iterkeys())
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
        result = unicode(hash_func(repr(result)))
    return result


def load_file(path, filetype=None, as_df=False, **kwargs):
    if not filetype:
        # try to get file extension
        filetype = path.split('.')[-1]
    if filetype in ['csv', 'txt']:
        result = load_csv(path, **kwargs)
    elif filetype in ['json']:
        result = load_json(path, **kwargs)
    elif filetype in ['pickle']:
        result = load_pickle(path, **kwargs)
    elif filetype in ['db']:
        result = load_shelve(path, **kwargs)
    else:
        raise TypeError("Invalid filetype: %s" % filetype)
    return _data_export(result, as_df=as_df)


def load_pickle(path, **kwargs):
    result = []
    with open(path) as f:
        while 1:
            # in case we have multiple pickles dumped
            try:
                result.append(cPickle.load(f))
            except EOFError:
                break
    return result


def load_csv(path, **kwargs):
    kwargs.setdefault('skipinitialspace', True)
    # load the file according to filetype
    return pd.read_csv(path, **kwargs)


def load_json(path, **kwargs):
    return pd.read_json(path, **kwargs)


def load_shelve(path, as_list=True, **kwargs):
    '''
    shelve expects each object to be indexed
    by one of it's column values (ie, _oid)
    where value is the entire object which maps
    to the given column value (ie, {_oid: {obj with _oid})
    '''
    kwargs.setdefault('flag', 'c')
    kwargs.setdefault('protocol', 2)
    if as_list:
        return [o for o in shelve.open(path, **kwargs).itervalues()]
    else:
        return shelve.open(path, **kwargs)


def _set_oid_func(_oid_func):
    k = itertools.count(1)

    def __oid_func(o):
        ''' default __oid generator '''
        o['_oid'] = o['_oid'] if '_oid' in o else k.next()
        return o

    if _oid_func:
        if _oid_func is True:
            _oid_func = __oid_func
        elif isfunction(_oid_func):
            pass
        else:
            raise TypeError("_oid must be a function!")
    else:
        _oid_func = None
    return _oid_func


def load(path, filetype=None, as_df=False, retries=None,
         _oid=None, quiet=False, **kwargs):
    '''Load multiple files from various file types automatically.

    Supports glob paths, eg::

        path = 'data/*.csv'

    Filetypes are autodetected by common extension strings.

    Currently supports loadings from:
        * csv (pd.read_csv)
        * json (pd.read_json)

    :param path: path to config json file
    :param filetype: override filetype autodetection
    :param kwargs: additional filetype loader method kwargs
    '''
    set_oid = _set_oid_func(_oid)

    # kwargs are for passing ftype load options (csv.delimiter, etc)
    # expect the use of globs; eg, file* might result in fileN (file1,
    # file2, file3), etc
    if not isinstance(path, basestring):
        # assume we're getting a raw dataframe
        objects = path
        if not isinstance(objects, pd.DataFrame):
            raise ValueError("loading raw values must be DataFrames")
    elif re.match('https?://', path):
        logger.debug('Saving %s to tmp file' % path)
        _path = urlretrieve(path, retries)
        logger.debug('%s saved to tmp file: %s' % (path, _path))
        try:
            objects = load_file(_path, filetype, **kwargs)
        finally:
            os.remove(_path)
    else:
        path = re.sub('^file://', '', path)
        path = os.path.expanduser(path)
        datasets = sorted(glob.glob(os.path.expanduser(path)))
        # buid up a single dataframe by concatting
        # all globbed files together
        objects = []
        [objects.extend(load_file(ds, filetype, **kwargs))
            for ds in datasets]

    if is_null(objects) and not quiet:
        raise ValueError("not objects extracted!")
    else:
        logger.debug("Data loaded successfully from %s" % path)

    if set_oid:
        # set _oids, if we have a _oid generator func defined
        objects = [set_oid(o) for o in objects]

    if as_df:
        return pd.DataFrame(objects)
    else:
        return objects


def _data_export(data, as_df=False):
    if as_df:
        if isinstance(data, pd.DataFrame):
            return data
        else:
            return pd.DataFrame(data)
    else:
        if isinstance(data, pd.DataFrame):
            return data.T.to_dict().values()
        else:
            return data


def _load_cube_pkg(pkg, cube):
    '''
    NOTE: all items in fromlist must be strings
    '''
    try:
        # First, assume the cube module is available
        # with the name exactly as written
        fromlist = map(str, [cube])
        mcubes = __import__(pkg, fromlist=fromlist)
        return getattr(mcubes, cube)
    except AttributeError:
        # if that fails, try to guess the cube module
        # based on cube 'standard naming convention'
        # ie, group_cube -> from group.cube import CubeClass
        _pkg, _mod, _cls = cube_pkg_mod_cls(cube)
        fromlist = map(str, [_cls])
        mcubes = __import__('%s.%s.%s' % (pkg, _pkg, _mod),
                            fromlist=fromlist)
        return getattr(mcubes, _cls)


def load_config(path):
    if not path:
        return {}
    else:
        config_file = os.path.expanduser(path)
        conf = anyconfig.load(config_file) or {}
        if conf:
            # convert mergeabledict (anyconfig) to dict of dicts
            return conf.convert_to(conf)
        else:
            raise IOError("Invalid config file: %s" % config_file)


def profile(fn, cache_dir=CACHE_DIR):
    cache_dir = cache_dir or CACHE_DIR
    # profile code snagged from http://stackoverflow.com/a/1175677/1289080

    def wrapper(*args, **kw):
        saveas = os.path.join(cache_dir, 'pyprofile.txt')
        elapsed, stat_loader, result = _profile(saveas, fn, *args, **kw)
        stats = stat_loader()
        stats.sort_stats('cumulative')
        stats.print_stats()
        # uncomment this to see who's calling what
        # stats.print_callers()
        try:
            os.remove(saveas)
        except Exception:
            pass
        return result
    return wrapper


def _profile(filename, fn, *args, **kw):
    load_stats = lambda: pstats.Stats(filename)
    gc.collect()

    began = time.time()
    profiler.runctx('result = fn(*args, **kw)', globals(), locals(),
                    filename=filename)
    ended = time.time()

    return ended - began, load_stats, locals()['result']


def rupdate(source, target):
    ''' recursively update nested dictionaries
        see: http://stackoverflow.com/a/3233356/1289080
    '''
    for k, v in target.iteritems():
        if isinstance(v, collections.Mapping):
            r = rupdate(source.get(k, {}), v)
            source[k] = r
        else:
            source[k] = target[k]
    return source


def safestr(str_):
    ''' get back an alphanumeric only version of source '''
    return "".join(x for x in str_ if x.isalnum())


def sys_call(cmd, sig=None, sig_func=None, quiet=True):
    if not quiet:
        logger.debug(cmd)
    if isinstance(cmd, basestring):
        cmd = re.sub('\s+', ' ', cmd)
        cmd = cmd.strip()
        cmd = shlex.split(cmd)
    if sig and sig_func:
        signal.signal(sig, sig_func)
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    if not quiet:
        logger.debug(output)
    return output.strip()


def to_encoding(ustring, encoding=None, errors='replace'):
    errors = errors or 'replace'
    encoding = encoding or 'utf-8'
    if isinstance(ustring, basestring):
        if not isinstance(ustring, unicode):
            return unicode(ustring, encoding, errors)
        else:
            return ustring.encode(encoding, errors).decode('utf8')
    else:
        raise ValueError('basestring type required')


def ts2dt(ts, milli=False, tz_aware=False):
    ''' convert timestamp int's (seconds) to datetime objects '''
    # anything already a datetime will still be returned
    # tz_aware, if set to true
    if is_null(ts):
        return None  # its not a timestamp
    elif isinstance(ts, datetime):
        pass
    elif isinstance(ts, basestring):
        try:
            ts = float(ts)
        except (TypeError, ValueError):
            # maybe we have a date like string already?
            try:
                ts = dt_parse(ts)
            except Exception:
                raise TypeError(
                    "unable to derive datetime from timestamp string: %s" % ts)
    elif milli:
        ts = float(ts) / 1000.  # convert milli to seconds
    else:
        ts = float(ts)  # already in seconds

    return _get_datetime(ts, tz_aware)


def _get_datetime(value, tz_aware=None):
    if tz_aware:
        if isinstance(value, datetime):
            return value.replace(tzinfo=UTC)
        else:
            return datetime.fromtimestamp(value, tz=UTC)
    else:
        if isinstance(value, datetime):
            if value.tzinfo:
                return value.astimezone(UTC).replace(tzinfo=None)
            else:
                return value
        else:
            return datetime.utcfromtimestamp(value)


def utcnow(as_datetime=True, tz_aware=False, drop_micro=False):
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


def urlretrieve(uri, saveas=None, retries=3, cache_dir=None):
    '''urllib.urlretrieve wrapper'''
    retries = int(retries) if retries else 3
    # FIXME: make random filename (saveas) in cache_dir...
    # cache_dir = cache_dir or CACHE_DIR
    while retries:
        try:
            _path, headers = urllib.urlretrieve(uri, saveas)
        except Exception as e:
            retries -= 1
            logger.warn(
                'Failed getting %s: %s (retry:%s in 1s)' % (
                    uri, e, retries))
            time.sleep(1)
            continue
        else:
            break
    return _path
