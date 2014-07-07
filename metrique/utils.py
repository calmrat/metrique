#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.utils
~~~~~~~~~~~~~~

This module contains utility functions shared between
metrique sub-modules.
'''

from __future__ import unicode_literals, absolute_import

import logging
logger = logging.getLogger('metrique')

try:
    import anyconfig
    anyconfig.set_loglevel(logging.WARN)  # too noisy...
    HAS_ANYCONFIG = True
except ImportError:
    HAS_ANYCONFIG = False
    logger.warn('anyconfig module is not installed!')

from calendar import timegm
from collections import defaultdict, Mapping, OrderedDict
from copy import copy
import cPickle
import cProfile as profiler
from datetime import datetime, date
try:
    from dateutil.parser import parse as dt_parse
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False
    logger.warn('dateutil module is not installed!')

try:
    from dulwich.repo import Repo
    HAS_DULWICH = True
except ImportError:
    HAS_DULWICH = False
    logger.warn('dulwich module is not installed!')

from functools import partial
import gc
from getpass import getuser
import glob
from hashlib import sha1
from inspect import isfunction
import itertools
import os
from pprint import pformat

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    logger.warn('pandas module is not installed!')

import pstats

try:
    import pytz
    HAS_PYTZ = True
except ImportError:
    HAS_PYTZ = False
    logger.warn('pytz module is not installed!')

import random
import re
import resource
import shlex
import shutil
import signal

try:
    import simplejson as json
except ImportError:
    logger.warn('simplejson module is not installed; fallback to json')
    import json

import string
import subprocess
import sys
import time
import urllib

env = os.environ
pjoin = os.path.join
NoneType = type(None)

json_encoder = json.JSONEncoder()

ZEROS = [0, 0.0, 0L]
DEFAULT_PKGS = ['metrique.cubes']

INVALID_USERNAME_RE = re.compile('[^a-zA-Z_]')

HOME_DIR = os.environ.get('METRIQUE_HOME')
PREFIX_DIR = os.environ.get('METRIQUE_PREFIX')
LOGS_DIR = os.environ.get('METRIQUE_LOGS')
CACHE_DIR = os.environ.get('METRIQUE_CACHE')
SRC_DIR = os.environ.get('METRIQUE_SRC')
BACKUP_DIR = env.get('METRIQUE_BACKUP')
STATIC_DIR = env.get('METRIQUE_STATIC')

# FIXME: add tests for local_tz, autoschema and more...


def active_virtualenv():
    return os.environ.get('VIRTUAL_ENV', '')


def autoschema(objects, fast=False, exclude_keys=None):
    logger.debug('autoschema generation started... Fast: %s' % fast)
    is_defined(objects, 'object samples can not be null')
    objects = tuple(objects) if is_array(objects, except_=False) else [objects]
    schema = defaultdict(dict)
    exclude_keys = exclude_keys or []
    for o in objects:
        logger.debug('autoschema model object: %s' % o)
        for k, v in o.iteritems():
            schema_type = schema.get(k, {}).get('type')
            if k in exclude_keys:
                continue
            elif schema_type not in [None, NoneType]:
                # we already have this type
                # FIXME: option to check rigerously all objects
                # consistency; raise exception if values are of
                # different type given same key, etc...
                continue
            else:
                _type = type(v)
                if is_array(_type, except_=False):
                    schema[k]['container'] = True
                    # FIXME: if the first object happens to be null
                    # we auto set to UnicodeText type...
                    # (default for type(None))
                    # but this isn't always going to be accurate...
                    if len(v) > 1:
                        _t = type(v[0])
                    else:
                        _t = NoneType
                    schema[k]['type'] = _t
                else:
                    schema[k]['type'] = _type
        if fast is True:  # finish after first sample
            break
    logger.debug(' ... schema generated: %s' % schema)
    return schema


def backup(paths, saveas=None, ext=None):
    paths = list2str(paths, delim=' ')
    saveas = saveas if saveas else 'out'
    saveas = re.sub('\.tar.*', '', saveas)

    gzip = sys_call('which gzip', ignore_errors=True)
    pigz = sys_call('which pigz', ignore_errors=True)
    ucp = '--use-compress-program=%s'
    if pigz:
        ucp = ucp % pigz
        ext = ext or 'tar.pigz'
    elif gzip:
        ucp = ucp % gzip
        ext = ext or 'tar.gz'
    else:
        raise RuntimeError('Install pigz or gzip!')

    saveas = '%s.%s' % (saveas, ext)
    if not os.path.isabs(saveas):
        saveas = os.path.join(CACHE_DIR, saveas)
    cmd = 'tar -c %s -f %s %s' % (ucp, saveas, paths)
    sys_call(cmd)
    assert os.path.exists(saveas)
    return saveas


def batch_gen(data, batch_size):
    '''
    Usage::
        for batch in batch_gen(iter, 100):
            do_something(batch)
    '''
    data = data or []
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def clear_stale_pids(pids, pid_dir='/tmp', prefix='', multi=False):
    'check for and remove any pids which have no corresponding process'
    if isinstance(pids, (int, float, long)):
        pids = [pids]
    pids = str2list(pids, map_=unicode)
    procs = map(unicode, os.listdir('/proc'))
    running = [pid for pid in pids if pid in procs]
    logger.warn(
        "Found %s pids running: %s" % (len(running),
                                       running))
    prefix = prefix.rstrip('.') if prefix else None
    for pid in pids:
        if prefix:
            _prefix = prefix
        else:
            _prefix = unicode(pid)
        # remove non-running procs
        if pid in running:
            continue
        if multi:
            pid_file = '%s%s.pid' % (_prefix, pid)
        else:
            pid_file = '%s.pid' % (_prefix)
        path = os.path.join(pid_dir, pid_file)
        if os.path.exists(path):
            logger.debug("Removing pidfile: %s" % path)
            try:
                remove_file(path)
            except OSError as e:
                logger.warn(e)
    return running


def configure(options=None, defaults=None, config_file=None,
              section_key=None, update=None, section_only=False):

    config = load_config(config_file)
    config = rupdate(config, copy(update or {}))

    opts = copy(options or {})
    defs = copy(defaults or {})

    sk = section_key

    if (section_key or section_only) and not (sk and sk in config):
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


def daemonize(pid_file=None, cwd=None):
    """
    Detach a process from the controlling terminal and run it in the
    background as a daemon.

    Modified version of:
        code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/

    author = "Chad J. Schroeder"
    copyright = "Copyright (C) 2005 Chad J. Schroeder"
    """
    cwd = cwd or '/'
    try:
        pid = os.fork()
    except OSError as e:
        raise Exception("%s [%d]" % (e.strerror, e.errno))

    if (pid == 0):   # The first child.
        os.setsid()
        try:
            pid = os.fork()    # Fork a second child.
        except OSError as e:
            raise Exception("%s [%d]" % (e.strerror, e.errno))
        if (pid == 0):    # The second child.
            os.chdir(cwd)
            os.umask(0)
        else:
            os._exit(0)    # Exit parent (the first child) of the second child.
    else:
        os._exit(0)   # Exit parent of the first child.

    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = 1024

    # Iterate through and close all file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:   # ERROR, fd wasn't open to begin with (ignored)
            pass

    os.open('/dev/null', os.O_RDWR)  # standard input (0)

    # Duplicate standard input to standard output and standard error.
    os.dup2(0, 1)            # standard output (1)
    os.dup2(0, 2)            # standard error (2)

    pid_file = pid_file or '%s.pid' % os.getpid()
    write_file(pid_file, os.getpid())
    return 0


def csv2list(item, delim=',', map_=None):
    return str2list(item, delim=delim, map_=None)


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
    _log_format = "%(levelname)s.%(name)s.%(process)s:%(asctime)s:%(message)s"
    log_format = log_format or _log_format
    if isinstance(log_format, basestring):
        log_format = logging.Formatter(log_format, "%Y%m%dT%H%M%S")

    log2file = True if log2file is None else log2file
    logger = logger or 'metrique'
    if isinstance(logger, basestring):
        logger = logging.getLogger(logger)
    else:
        logger = logger or logging.getLogger(logger)
    logger.propagate = 0
    logger.handlers = []
    if log2file:
        log_dir = log_dir or LOGS_DIR
        log_file = log_file or 'metrique'
        log_file = os.path.join(log_dir, log_file)
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
    is_true(HAS_DATEUTIL, "`pip install python_dateutil` required")
    if is_empty(dt, except_=False):
        ts = None
    elif isinstance(dt, (int, long, float)):  # its a ts already
        ts = float(dt)
    elif isinstance(dt, basestring):  # convert to datetime first
        try:
            parsed_dt = float(dt)
        except (TypeError, ValueError):
            parsed_dt = dt_parse(dt)
        ts = dt2ts(parsed_dt)
    else:
        assert isinstance(dt, (datetime, date))
        # keep micros; see: http://stackoverflow.com/questions/7031031
        ts = ((
            timegm(dt.timetuple()) * 1000.0) +
            (dt.microsecond / 1000.0)) / 1000.0
    if ts is None:
        pass
    elif drop_micro:
        ts = float(int(ts))
    else:
        ts = float(ts)
    return ts


def file_is_empty(path, remove=False, msg=None):
    path = to_encoding(path)
    is_true(os.path.isfile(path), '"%s" is not a file!' % path)
    if bool(os.stat(path).st_size == 0):
        logger.info("%s is empty" % path)
        if remove:
            logger.info("... %s removed" % path)
            remove_file(path)
        return True
    else:
        return False


def filename_append(orig_filename, append_str):
    is_defined(orig_filename, 'filename must be defined!')
    # make sure we don't duplicate the append str
    orig_filename = re.sub(append_str, '', orig_filename)
    name, ext = os.path.splitext(orig_filename)
    return '%s%s%s' % (name, append_str, ext)


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


def get_pid(pid_file=None):
    if not pid_file:
        return 0
    return int(''.join(open(pid_file).readlines()).strip())


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


def _get_timezone_converter(dt, from_tz, to_tz=None, tz_aware=False):
    if from_tz is None:
        raise TypeError("from_tz can not be null!")
    elif dt is None:
        return None
    else:
        to_tz = to_tz or pytz.UTC
        if isinstance(to_tz, basestring):
            to_tz = pytz.timezone(to_tz)
        dt = ts2dt(dt) if not isinstance(dt, datetime) else dt
        if dt.tzinfo:
            # datetime instance already has tzinfo set
            # WARN if not dt.tzinfo == from_tz?
            try:
                dt = dt.astimezone(to_tz)
            except ValueError:
                # date has invalid timezone; replace with expected
                dt = dt.replace(tzinfo=from_tz)
                dt = dt.astimezone(to_tz)
        else:
            # set tzinfo as from_tz then convert to utc
            dt = from_tz.localize(dt).astimezone(to_tz)
        if not tz_aware:
            dt = dt.replace(tzinfo=None)
        return dt


def get_timezone_converter(from_timezone, to_tz=None, tz_aware=False):
    '''
    return a function that converts a given
    datetime object from a timezone to utc

    :param from_timezone: timezone name as string
    '''
    if not from_timezone:
        return None
    is_true(HAS_DATEUTIL, "`pip install python_dateutil` required")
    is_true(HAS_PYTZ, "`pip install pytz` required")
    from_tz = pytz.timezone(from_timezone)
    return partial(_get_timezone_converter, from_tz=from_tz, to_tz=to_tz,
                   tz_aware=tz_aware)


def git_clone(uri, pull=True, reflect=False, cache_dir=None, chdir=True):
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
        sys_call(cmd, cwd=repo_path)
    if chdir:
        os.chdir(repo_path)
    if reflect:
        if not HAS_DULWICH:
            raise RuntimeError("`pip install dulwich` required!")
        return Repo(repo_path)
    else:
        return repo_path


def is_array(value, msg=None, except_=None, inc_set=False):
    check = (list, tuple, set) if inc_set else (list, tuple)
    result = isinstance(value, check)
    return is_true(result, msg=msg, except_=except_)


def is_defined(value, msg=None, except_=None):
    result = not is_empty(value, except_=False)
    return is_true(result, msg=msg, except_=except_)


def is_empty(value, msg=None, except_=None):
    '''
    is defined, but null or empty like value
    '''
    # 0, 0.0, 0L are also considered 'empty'
    if hasattr(value, 'empty'):
        # dataframes must check for .empty
        # since they don't define truth value attr
        # take the negative, since below we're
        # checking for cases where value 'is_null'
        value = not bool(value.empty)
    elif value in ZEROS:
        # will check for the negative below
        value = True
    else:
        pass
    _is_null = is_null(value, except_=False)
    result = bool(_is_null or not value)
    return is_true(result, msg=msg, except_=except_)


def is_false(value, msg=None, except_=None):
    result = is_true(value, msg=msg, except_=False) is False
    return result


def is_null(value, msg=None, except_=None):
    '''
    ie, "is not defined"
    '''
    # dataframes, even if empty, are not considered null
    value = False if hasattr(value, 'empty') else value
    result = bool(
        value is None or
        value != value or
        repr(value) == 'NaT')
    return is_true(result, msg=msg, except_=except_)


def is_string(value, msg=None, except_=None):
    result = isinstance(value, basestring)
    return is_true(result, msg=msg, except_=except_)


def is_true(value, msg=None, except_=None):
    # if msg is passed in, implied except_=True; otherwise,
    # respect what's passed
    except_ = bool(msg) if except_ is None else except_
    result = bool(value is True)
    if result:
        return result
    if except_:
        msg = msg or '(%s) is not True' % to_encoding(value)
        raise RuntimeError(msg)
    return result


def json_encode_default(obj):
    '''
    Convert datetime.datetime to timestamp

    :param obj: value to (possibly) convert
    '''
    if isinstance(obj, (datetime, date)):
        result = dt2ts(obj)
    else:
        result = json_encoder.default(obj)
    return to_encoding(result)


def jsonhash(obj, root=True, exclude=None, hash_func=None):
    '''
    calculate the objects hash based on all field values
    '''
    if not hash_func:
        hash_func = sha1_hexdigest
    if isinstance(obj, Mapping):
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


def list2str(items, delim=','):
    delim = delim or ','
    if is_array(items, except_=False):
        item = delim.join(map(unicode, tuple(items)))
    elif isinstance(items, basestring):
        # assume we already have a normalized delimited string
        item = items
    elif items is None:
        item = ''
    else:
        raise TypeError('expected a list')
    return item


def load_file(path, filetype=None, as_df=False, **kwargs):
    if not os.path.exists(path):
        raise IOError("%s does not exist" % path)
    if not filetype:
        # try to get file extension
        filetype = path.split('.')[-1]
    if filetype in ['csv', 'txt']:
        result = load_csv(path, **kwargs)
    elif filetype in ['json']:
        result = load_json(path, **kwargs)
    elif filetype in ['pickle']:
        result = load_pickle(path, **kwargs)
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
    is_true(HAS_PANDAS, "`pip install pandas` required")
    kwargs.setdefault('skipinitialspace', True)
    # load the file according to filetype
    return pd.read_csv(path, **kwargs)


def load_json(path, **kwargs):
    is_true(HAS_PANDAS, "`pip install pandas` required")
    return pd.read_json(path, **kwargs)


def local_tz():
    if time.daylight:
        offsetHour = time.altzone / 3600
    else:
        offsetHour = time.timezone / 3600
    return 'Etc/GMT%+d' % offsetHour


def local_tz_to_utc(dt):
    dt = ts2dt(dt)
    convert = get_timezone_converter(_local_tz)
    return convert(dt)


def set_oid_func(_oid_func):
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
    is_true(HAS_PANDAS, "`pip install pandas` required")
    set_oid = set_oid_func(_oid)

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
            remove_file(_path)
    else:
        path = re.sub('^file://', '', path)
        path = os.path.expanduser(path)
        files = sorted(glob.glob(os.path.expanduser(path)))
        if not files:
            raise IOError("failed to load: %s" % path)
        # buid up a single dataframe by concatting
        # all globbed files together
        objects = []
        [objects.extend(load_file(ds, filetype, **kwargs))
            for ds in files]

    if is_empty(objects, except_=False) and not quiet:
        raise RuntimeError("no objects extracted!")
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
    is_true(HAS_PANDAS, "`pip install pandas` required")
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

    config_file = os.path.expanduser(path)
    if HAS_ANYCONFIG:
        conf = anyconfig.load(config_file) or {}
        if conf:
            # convert mergeabledict (anyconfig) to dict of dicts
            return conf.convert_to(conf)
        else:
            raise IOError("Invalid config file: %s" % config_file)
    else:
        raise RuntimeError("`pip install anyconfig` required!")


def make_dirs(path, mode=0700, quiet=True):
    if not path.startswith('/'):
        raise OSError("requires absolute path! got %s" % path)
    if os.path.exists(path):
        if not quiet:
            logger.warn('Can not create %s; already exists!' % path)
    else:
        os.makedirs(path, mode)
    return path


def move(path, dest, quiet=False):
    if is_array(path, except_=False):
        return [move(p, dest) for p in tuple(path)]
    else:
        assert isinstance(path, basestring)
        if os.path.exists(path):
            return shutil.move(path, dest)
        elif not quiet:
            raise IOError('path not found: %s' % path)
        else:
            return []


def profile(fn, cache_dir=CACHE_DIR):
    cache_dir = cache_dir or CACHE_DIR
    # profile code snagged from http://stackoverflow.com/a/1175677/1289080

    def wrapper(*args, **kw):
        saveas = os.path.join(cache_dir, 'pyprofile.txt')
        elapsed, stat_loader, result = _profile(saveas, fn, *args, **kw)
        stat_loader.sort_stats('cumulative')
        stat_loader.print_stats()
        # uncomment this to see who's calling what
        # stats.print_callers()
        remove_file(saveas)
        return result
    return wrapper


def _profile(filename, fn, *args, **kw):
    gc.collect()

    began = time.time()
    profiler.runctx('result = fn(*args, **kw)', globals(), locals(),
                    filename=filename)
    ended = time.time()

    return ended - began, pstats.Stats(filename), locals()['result']


def rand_chars(size=6, chars=string.ascii_uppercase + string.digits,
               prefix=''):
    prefix = prefix or ''
    # see: http://stackoverflow.com/questions/2257441
    chars = ''.join(random.choice(chars) for x in range(size))
    chars = prefix + chars
    return chars


def read_file(rel_path, paths=None, raw=False, as_list=False, *args, **kwargs):
    '''
        find a file that lives somewhere within a set of paths and
        return its contents. Default paths include 'static_dir'
    '''
    if not rel_path:
        raise ValueError("rel_path can not be null!")
    paths = str2list(paths)
    # try looking the file up in a directory called static relative
    # to SRC_DIR, eg assuming metrique git repo is in ~/metrique
    # we'd look in ~/metrique/static
    paths.extend([STATIC_DIR, os.path.join(SRC_DIR, 'static')])
    paths = [os.path.expanduser(p) for p in set(paths)]
    for path in paths:
        path = os.path.join(path, rel_path)
        logger.debug("trying to read: %s " % path)
        if os.path.exists(path):
            break
    else:
        raise IOError("path %s does not exist!" % rel_path)
    fd = open(path, *args, **kwargs)
    if raw:
        return fd

    fd_lines = fd.readlines()
    if as_list:
        return fd_lines
    else:
        return ''.join(fd_lines)


def remove_file(path, force=False):
    logger.warn('Removing %s' % str(path))
    if not path:
        return []
    # create a list from glob search or expect a list
    path = glob.glob(path) if isinstance(path, basestring) else list(path)
    if is_array(path, except_=False):
        if len(path) == 1:
            path = path[0]
        else:
            return [remove_file(p, force=force) for p in tuple(path)]
    assert bool(path) is True
    assert isinstance(path, basestring)
    cwd = os.getcwd()
    is_true(os.path.isabs(path),
            'paths to remove must be absolute; got %s' % path)
    if os.path.exists(path):
        if os.path.isdir(path):
            if cwd == path:
                logger.warn('removing dir tree we are currently in. (%s) '
                            'chdir to metrique home: %s' % (cwd, PREFIX_DIR))
                assert os.path.exists(PREFIX_DIR)
                os.chdir(PREFIX_DIR)
            if force:
                shutil.rmtree(path)
            else:
                raise RuntimeError(
                    '%s is a directory; use force=True to remove!' % path)
        else:
            os.remove(path)
    else:
        logger.warn('%s not found' % path)
    return path


def rsync(targets=None, dest=None, compress=True,
          ssh_host=None, ssh_user=None):
    dest = dest or os.path.join(CACHE_DIR, 'metrique_rsync')
    _ = not (os.path.exists(dest) and not os.path.isdir(dest))
    is_true(_, msg="%s exists but is not a directory!" % dest)
    make_dirs(dest)
    compress = '-z' if compress else ''
    targets = str2list(targets, map_=unicode) or ['.']
    targets = list2str(targets, delim=' ')
    if ssh_host:
        ssh_user = ssh_user or getuser()
        sys_call('rsync -av %s -e ssh %s %s@%s:%s' % (
            compress, targets, ssh_user, ssh_host, dest))
    else:
        dest = pjoin(BACKUP_DIR, dest)
        sys_call('rsync -av %s %s %s' % (compress, targets, dest))
    return True


def rupdate(source, target):
    ''' recursively update nested dictionaries
        see: http://stackoverflow.com/a/3233356/1289080
    '''
    for k, v in target.iteritems():
        if isinstance(v, Mapping):
            r = rupdate(source.get(k, {}), v)
            source[k] = r
        else:
            source[k] = target[k]
    return source


def safestr(str_):
    ''' get back an alphanumeric only version of source '''
    str_ = str_ or ""
    return "".join(x for x in str_ if x.isalnum())


def sha1_hexdigest(o):
    return sha1(repr(o)).hexdigest()


def str2list(item, delim=',', map_=None):
    if isinstance(item, basestring):
        items = item.split(delim)
    elif is_array(item, except_=False):
        items = map(unicode, tuple(item))
    elif item is None:
        items = []
    else:
        raise TypeError('Expected a csv string (or existing list)')
    items = [s.strip() for s in items]
    items = map(map_, items) if map_ else items
    return items


def _sys_call(cmd, shell=True, quiet=False, bg=False):
    if not quiet:
        logger.info('Running: `%s`' % cmd)

    if isinstance(cmd, basestring):
        cmd = re.sub('\s+', ' ', cmd)
        cmd = cmd.strip()
        cmd = shlex.split(cmd)
    else:
        cmd = [s.strip() for s in list(cmd)]

    if bg:
        p = subprocess.Popen(cmd)
        if not p:
            raise RuntimeError("Failed to start '%s'" % cmd)
        return p
    try:
        cmd = ' '.join(s for s in cmd)
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT,
                                         shell=shell)
    except subprocess.CalledProcessError as e:
        output = to_encoding(e.output)
        raise RuntimeError(
            "Command: %s\n\tExit status: %s.\n\tOutput:\n%s" % (
                e.cmd, e.returncode, output))
    else:
        if not quiet:
            logger.debug(output)
    return output.strip()


def sys_call(cmd, sig=None, sig_func=None, shell=True, cwd=None, quiet=False,
             fork=False, pid_file=None, ignore_errors=False, bg=False):
    _path = os.getcwd()
    cwd = cwd or _path
    try:
        if fork:
            logger.warn('*' * 50 + 'FORKING' + '*' * 50)
            bg = True
            pid = os.fork()
            if pid == 0:  # child
                try:
                    p = _sys_call(cmd, shell=shell, quiet=quiet, bg=bg)
                finally:
                    os.chdir(_path)

                with open(pid_file, 'w') as f:
                    f.write(str(p.pid))
                p.wait()
                del p
                os._exit(0)
            else:
                return pid_file
        else:
            try:
                output = _sys_call(cmd, shell=shell, quiet=quiet, bg=bg)
            finally:
                os.chdir(_path)
    except Exception as e:
        logger.warn('Error: %s' % e)
        if ignore_errors:
            return None
        else:
            raise
    return output


def terminate(pid, sig=signal.SIGTERM):
    pid_file = None
    if isinstance(pid, basestring):
        # we have a path to pidfile...
        pid_file = pid
        pid = get_pid(pid_file)
    if pid <= 0:
        logger.warn('no pid to kill found at %s' % pid)
    else:
        try:
            logger.debug('killing %s with %s' % (pid, sig))
            result = os.kill(pid, sig)
            logger.debug(result)
        except OSError:
            logger.debug("%s not found" % pid)
        else:
            logger.debug("%s killed" % pid)
    if pid_file:
        remove_file(pid_file)
    return


def timediff(t0, msg=' ... done'):
    return '   %s in \033[92m%.2f s\033[0m' % (msg, time() - t0)


def to_encoding(str_, encoding=None, errors='replace'):
    errors = errors or 'replace'
    encoding = encoding or 'utf-8'
    if str_ is None:
        return None
    elif not isinstance(str_, unicode):
        result = unicode(str(str_), encoding, errors)
    else:
        result = str_.encode(encoding, errors).decode('utf8')
    return result


def ts2dt(ts, milli=False, tz_aware=False):
    ''' convert timestamp int's (seconds) to datetime objects '''
    # anything already a datetime will still be returned
    # tz_aware, if set to true
    is_true(HAS_DATEUTIL, "`pip install python_dateutil` required")
    if isinstance(ts, datetime):
        pass
    elif is_empty(ts, except_=False):
        return None  # its not a timestamp
    elif isinstance(ts, (int, float, long)) and ts < 0:
        return None
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
    is_true(HAS_PYTZ, "`pip install pytz` required")
    if tz_aware:
        if isinstance(value, datetime):
            return value.replace(tzinfo=pytz.UTC)
        else:
            value = datetime.fromtimestamp(value, tz=pytz.UTC)
            value.replace(tzinfo=pytz.UTC)
            return value
    else:
        if isinstance(value, datetime):
            if value.tzinfo:
                return value.astimezone(pytz.UTC).replace(tzinfo=None)
            else:
                return value
        else:
            return datetime.utcfromtimestamp(value)


def utcnow(as_datetime=False, tz_aware=False, drop_micro=False):
    is_true(HAS_PYTZ, "`pip install pytz` required")
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


def utc_to_local_tz(dt):
    dt = ts2dt(dt)
    convert = get_timezone_converter('UTC', _local_tz)
    return convert(dt)


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
                'Failed getting uri "%s": %s (retry:%s in 1s)' % (
                    uri, e, retries))
            time.sleep(.2)
            continue
        else:
            break
    else:
        raise RuntimeError("Failed to retrieve uri: %s" % uri)
    return _path


def validate_password(password):
    char_8_plus = password and len(password) >= 8
    ok = all((is_string(password, except_=False), char_8_plus))
    if not ok:
        raise ValueError("Invalid password; must be len(string) >= 8")
    return password


def validate_roles(roles, valid_roles):
    roles = set(str2list(roles, map_=unicode))
    if not roles <= set(valid_roles):
        raise ValueError("invalid roles %s, try: %s" % (roles, valid_roles))
    return sorted(roles)


def validate_username(username, restricted_names=None):
    if not isinstance(username, basestring):
        raise TypeError("username must be a string")
    elif INVALID_USERNAME_RE.search(username):
        raise ValueError(
            "Invalid username '%s'; "
            "lowercase, ascii alpha [a-z_] characters only!" % username)
    else:
        username = username.lower()
    if restricted_names and username in restricted_names:
        raise ValueError(
            "username '%s' is not permitted" % username)
    return username


def virtualenv_deactivate():
    virtenv = active_virtualenv()
    result = None
    if virtenv:
        to_remove = [p for p in sys.path if p.startswith(virtenv)]
        if to_remove:
            sys.path = [p for p in sys.path if p not in to_remove]
            logger.debug(' ... paths cleared: %s' % sorted(to_remove))
        env['VIRTUAL_ENV'] = ''
        logger.debug('Virtual Env (%s): Deactivated' % virtenv)
        result = True
    else:
        logger.debug('Deactivate: Virtual Env not detected')
    return result


def virtualenv_activate(virtenv=None):
    virtenv = virtenv or active_virtualenv()
    if not virtenv:
        logger.info('Activate: No virtenv defined')
        return  # nothing to activate
    elif virtenv == active_virtualenv():
        logger.debug('Virtual Env already active')
        return  # nothing to activate
    else:
        virtualenv_deactivate()  # deactive active virtual first

    activate_this = pjoin(virtenv, 'bin', 'activate_this.py')
    if os.path.exists(activate_this):
        execfile(activate_this, dict(__file__=activate_this))
        env['VIRTUAL_ENV'] = virtenv
        logger.info('Virtual Env (%s): Activated' % active_virtualenv())
    else:
        raise OSError("Invalid virtual env; %s not found" % activate_this)


def write_file(path, value, mode='w', force=False):
    if os.path.exists(path) and mode == 'w' and not force:
        raise RuntimeError('file exists, use different mode or force=True')
    with open(path, mode) as f:
        f.write(unicode(str(value)))


class DictDiffer(object):
    """
    # snagged from http://stackoverflow.com/a/1165552/1289080
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, dicts, added=True, removed=True,
                 changed=False, unchanged=False, diff=True,
                 exclude=None, include=None):
        is_array(dicts, 'dicts must be a list of dicts; got %s' % type(dicts))
        is_false(exclude and include, 'set include or exclude, not both')
        self._exclude = set(str2list(exclude) or [])
        self._include = set(str2list(include) or [])
        self._added = added
        self._removed = removed
        self._changed = changed
        self._unchanged = unchanged
        self._diff = diff

        od = OrderedDict
        s = sorted

        def skey(t):
            return t[0]

        self.dicts = [od(s(d.iteritems(), key=skey)) for d in dicts]

    def __getitem__(self, value):
        is_true(isinstance(value, slice), 'expected slice')
        dicts = self.dicts[value]
        past_dict = dicts.pop(0)
        _diffs = []
        for current_dict in dicts:
            current = set(self._include or current_dict.keys())
            past = set(self._include or past_dict.keys())
            intersect = current.intersection(past)
            if self._exclude:
                current = current - self._exclude
                past = past - self._exclude
                intersect = intersect - self._exclude

            _diff = {}
            if self._added:
                added = self.added(current, intersect)
                _diff['added'] = added
            if self._removed:
                removed = self.removed(past, intersect)
                _diff['removed'] = removed
            if self._changed:
                changed = self.changed(past_dict, current_dict, intersect)
                _diff['changed'] = changed
            if self._unchanged:
                unchanged = self.unchanged(past_dict, current_dict, intersect)
                _diff['unchanged'] = unchanged
            if self._diff:
                diff = self.diff(past_dict, current_dict, intersect)
                _diff['diff'] = diff
            _diffs.append(_diff)
            past_dict = current_dict
        return _diffs

    def added(self, current, intersect):
        return current - intersect

    def removed(self, past, intersect):
        return past - intersect

    def changed(self, past_dict, current_dict, intersect):
        return set(o for o in intersect if past_dict[o] != current_dict[o])

    def unchanged(self, past_dict, current_dict, intersect):
        return set(o for o in intersect if past_dict[o] == current_dict[o])

    def diff(self, past_dict, current_dict, intersect):
        _dict = {}
        for o in intersect:
            was = past_dict[o]
            now = current_dict[o]
            if was != now:
                _dict[o] = 'from %s to %s' % (repr(was), repr(now))
        return _dict

    def __str__(self):
        return pformat(self[:], indent=2)

    def __repr__(self):
        return self.__str__()


_local_tz = local_tz()
