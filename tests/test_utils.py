#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from __future__ import unicode_literals

import calendar
from copy import copy
from datetime import datetime
import os
import pytz
import simplejson as json
import shutil
from time import time

from .utils import is_in, set_env, qremove

env = set_env()

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')
fixtures = os.path.join(testroot, 'fixtures')
etc = os.path.join(testroot, 'etc')
cache_dir = env['METRIQUE_CACHE']
log_dir = env['METRIQUE_LOGS']


def test_batch_gen():
    '''

    '''
    from metrique.utils import batch_gen

    try:
        next(batch_gen(None, 1))
    except StopIteration:
        pass

    assert len(next(batch_gen([1], 1))) == 1

    assert len(next(batch_gen([1, 2], 1))) == 1
    assert len(next(batch_gen([1, 2], 2))) == 2

    assert len(tuple(batch_gen([1, 2], 1))) == 2
    assert len(tuple(batch_gen([1, 2], 2))) == 1

    assert len(tuple(batch_gen(range(50), 2))) == 25
    assert len(tuple(batch_gen(range(50), 5))) == 10

    assert len(tuple(batch_gen(range(100000), 1))) == 100000


def test_clear_stale_pids():
    from metrique.utils import clear_stale_pids
    pid_dir = '/tmp'
    # we assume we'll have a pid 1, but never -1
    pids = map(unicode, [1, -1])
    pid_files = [os.path.join(pid_dir, '%s.pid' % pid) for pid in pids]
    prefix = ''
    for pid, pid_file in zip(pids, pid_files):
        with open(pid_file, 'w') as f:
            f.write(str(pid))
    running = clear_stale_pids(pids, pid_dir, prefix)
    assert '1' in running
    assert '-1' not in running
    assert os.path.exists(pid_files[0])
    assert not os.path.exists(pid_files[1])
    os.remove(pid_files[0])


def test_configure():
    from metrique.utils import configure

    assert configure() == {}

    config = dict(
        debug=100,
        OK='OK')

    defaults = dict(
        debug=False,
        log2file=False)

    options = dict(
        debug=20,
        log2file=None)  # when None, should be ignored

    config_file = os.path.join(etc, 'test_conf.json')
    # contents:
        #{   "file": true
        #    "debug": true,
        #    "log2file": true   }

    # first, only defaults
    x = configure(defaults=defaults)
    assert is_in(x, 'debug', False)
    assert is_in(x, 'log2file', False)

    # then, where opt is not None, override
    x = configure(defaults=defaults, options=options)
    assert is_in(x, 'debug', 20)
    assert is_in(x, 'log2file', False)  # ignored options:None value

    # update acts as 'template config' in place of {}
    # but options will override values set already...
    # so, except that we have a new key, this should
    # be same as the one above
    x = configure(update=config, defaults=defaults,
                  options=options)

    assert is_in(x, 'debug', 20)
    assert is_in(x, 'log2file', False)  # ignored options:None value
    assert is_in(x, 'OK', 'OK')  # only in the template config

    # first thing loaded is values from disk, then updated
    # with 'update' config template
    # since log2file is set in config_file to True, it will
    # take that value
    x = configure(config_file=config_file, update=config,
                  defaults=defaults, options=options)
    assert is_in(x, 'debug', 20)
    assert is_in(x, 'log2file', True)  # ignored options:None value
    assert is_in(x, 'OK', 'OK')  # only in the template config
    assert is_in(x, 'file', True)  # only in the config_file config

    # cf is loaded first and update config template applied on top
    x = configure(config_file=config_file, update=config)
    assert is_in(x, 'debug', 100)
    assert is_in(x, 'log2file', True)  # ignored options:None value
    assert is_in(x, 'OK', 'OK')  # only in the template config
    assert is_in(x, 'file', True)  # only in the config_file config

    # cf is loaded first and update config template applied on top
    x = configure(config_file=config_file, options=options)
    assert is_in(x, 'debug', 20)
    assert is_in(x, 'log2file', True)  # ignored options:None value
    assert is_in(x, 'file', True)  # only in the config_file config

    # cf is loaded first and where key:values aren't set or set to
    # None defaults will be applied
    x = configure(config_file=config_file, defaults=defaults)
    assert is_in(x, 'debug', True)
    assert is_in(x, 'log2file', True)  # ignored options:None value
    assert is_in(x, 'file', True)  # only in the config_file config

    config_file = os.path.join(etc, 'test_conf_nested.json')
    # Contents are same, but one level nested under key 'metrique'
    x = configure(config_file=config_file, defaults=defaults,
                  section_key='metrique', section_only=True)
    assert is_in(x, 'debug', True)
    assert is_in(x, 'log2file', True)  # ignored options:None value
    assert is_in(x, 'file', True)  # only in the config_file config

    _x = x.copy()
    config_file = os.path.join(etc, 'test_conf_nested.json')
    # Contents are same, but one level nested under key 'metrique'
    x = configure(config_file=config_file, defaults=defaults,
                  section_key='metrique')
    assert is_in(x, 'metrique', _x)

    try:  # should fail
        x = configure(config_file='I_DO_NOT_EXIST')
    except IOError:
        pass

    for arg in ('update', 'options', 'defaults'):
        try:
            x = configure(**{arg: 'I_SHOULD_BE_A_DICT'})
        except AttributeError:
            pass


def test_csv2list():
    ' args: item '
    from metrique.utils import csv2list

    a_lst = ['a', 'b', 'c', 'd', 'e']
    a_str = 'a, b,     c,    d , e'
    assert csv2list(a_str) == a_lst

    for i in [None, a_lst, {}, 1, 1.0]:
        # try some non-string input values
        try:
            csv2list(i)
        except TypeError:
            pass


def test_cube_pkg_mod_cls():
    ''' ie, group_cube -> from group.cube import CubeClass '''
    from metrique.utils import cube_pkg_mod_cls

    pkg, mod, _cls = 'testcube', 'csvfile', 'Csvfile'
    cube = 'testcube_csvfile'
    assert cube_pkg_mod_cls(cube) == (pkg, mod, _cls)


def test__debug_set_level():
    from metrique.utils import _debug_set_level

    import logging
    logger = logging.getLogger('__test__')

    warn = ([0, -1, False], logging.WARN)
    info = ([None], logging.INFO)
    debug = ([True], logging.DEBUG)
    _int = ([10, 10.0], logging.DEBUG)

    for lvls, log_lvl in [warn, info, debug, _int]:
        for _lvl in lvls:
            logger = _debug_set_level(logger, _lvl)
            assert logger.level == log_lvl


def test_debug_setup(capsys):
    from metrique.utils import debug_setup

    import logging
    #logging.basicConfig(level=logging.DEBUG)

    log_file = '__test_log.log'

    # by default, logging -> file, not stdout
    _l = debug_setup()
    assert _l
    assert _l.level == logging.INFO
    assert _l.name == 'metrique'
    assert len(_l.handlers) == 1
    assert isinstance(_l.handlers[0], logging.FileHandler)

    logger_test = logging.getLogger('test')
    _l = debug_setup(logger=logger_test)
    assert _l is logger_test
    assert _l.name == 'test'

    _l = debug_setup(logger=logger_test,
                     log2file=False, log2stdout=True)
    _l.info('*')
    out, err = [x.strip() for x in capsys.readouterr()]
    #assert out == ''
    assert err == '*'

    # no output should seen for info(), since we set level
    # to warn, but issue an info call
    _l = debug_setup(logger=logger_test,
                     log2file=False, log2stdout=True,
                     level=logging.WARN)
    _l.info('*')
    out, err = [x.strip() for x in capsys.readouterr()]
    #assert out == ''
    assert err == ''
    _l.warn('*')
    out, err = [x.strip() for x in capsys.readouterr()]
    #assert out == ''
    assert err == '*'

    try:
        # output should be redirected to disk
        # reduce output to only include the message
        _l = debug_setup(logger=logger_test, truncate=True,
                         log_dir=log_dir, log_file=log_file,
                         log_format='%(message)s')
        _l.info('*')
        _lf = os.path.join(log_dir, log_file)
        lf = open(_lf).readlines()
        text = ''.join(lf).strip()
        assert text == '*'
    finally:
        qremove(_lf)


def test_dt2ts():
    '''  '''
    from metrique.utils import dt2ts

    # FIXME: millisecond precision, better?
    now_time = int(time())
    now_date = datetime.utcfromtimestamp(now_time)
    now_date_iso = now_date.isoformat()

    assert dt2ts(None) is None
    assert dt2ts(now_time) == now_time
    assert dt2ts(now_date) == now_time
    assert dt2ts(now_date_iso) == now_time


def test_get_cube():
    ' args: cube, path '
    from metrique.utils import get_cube

    # expected to be available (built-ins)
    get_cube('csvdata_rows')
    get_cube('sqldata_generic')

    # test pulling from arbitrary path/pkg
    paths = [os.path.dirname(os.path.abspath(__file__))]
    cube = 'csvcube_local'
    pkgs = ['testcubes']
    get_cube(cube=cube, pkgs=pkgs, cube_paths=paths)


def test_get_pids():
    from metrique.utils import get_pids as _

    pid_dir = os.path.expanduser('~/.metrique/trash')
    if not os.path.exists(pid_dir):
        os.makedirs(pid_dir)

    # any pid files w/out a /proc/PID process mapped are removed
    assert _(pid_dir, clear_stale=True) == []

    fake_pid = 11111
    assert fake_pid not in _(pid_dir, clear_stale=False)

    pid = 99999
    path = os.path.join(pid_dir, '%s.pid' % pid)
    with open(path, 'w') as f:
        f.write(str(pid))

    # don't clear the fake pidfile... useful for testing only
    pids = _(pid_dir, clear_stale=False)
    assert pid in pids
    assert all([True if isinstance(x, int) else False for x in pids])

    # clear it now and it should not show up in the results
    assert pid not in _(pid_dir, clear_stale=True)


def test_get_timezone_converter():
    ' args: from_timezone '
    ' convert is always TO utc '
    from metrique.utils import utcnow, get_timezone_converter

    # note: caching timezones always takes a few seconds
    good = 'US/Eastern'
    good_tz = pytz.timezone(good)

    now_utc = utcnow(tz_aware=True)

    now_est = copy(now_utc)
    now_est = now_est.astimezone(good_tz)
    now_est = now_est.replace(tzinfo=None)

    c = get_timezone_converter(good)
    assert c(now_est) == now_utc.replace(tzinfo=None)


def test_git_clone():
    from metrique.utils import git_clone, safestr
    uri = 'https://github.com/kejbaly2/tornadohttp.git'
    local_path = os.path.join(cache_dir, safestr(uri))
    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    _t = time()
    repo = git_clone(uri, pull=False, reflect=False, cache_dir=cache_dir)
    assert repo == local_path
    not_cached = time() - _t

    _t = time()
    repo = git_clone(uri, pull=False, reflect=True, cache_dir=cache_dir)
    cached = time() - _t

    assert repo.path == local_path
    assert cached < not_cached


def test_is_null():
    from metrique.utils import is_null
    nulls = ['', '  \t\n\t  ', 0, None, {}, []]
    not_nulls = ['hello', -1, 1, {'key': 'value'}, [1]]
    try:
        import pandas
    except ImportError:
        nulls += [pandas.NaT, pandas.NaN, pandas.DataFrame()]
    for x in nulls:
        null = is_null(x)
        print '%s is null? %s' % (repr(x), null)
        assert null is True
    for x in not_nulls:
        null = is_null(x)
        print '%s is null? %s' % (repr(x), null)
        assert null is False


def test_json_encode():
    ' args: obj '
    from metrique.utils import json_encode

    now = datetime.utcnow()

    dct = {"a": now}

    _dct = json.loads(json.dumps(dct, default=json_encode))
    assert isinstance(_dct["a"], float)


def test_jsonhash():
    from metrique.utils import jsonhash

    dct = {'a': [3, 2, 1],
           'z': ['a', 'c', 'b', 1],
           'b': {1: [], 3: {}},
           'partner': [],
           'pm_score': None,
           'priority': 'insignificant',
           'product': 'thisorthat',
           'qa_contact': None,
           'qa_whiteboard': None,
           'qe_cond_nak': None,
           'reporter': 'test@test.com',
           'resolution': None,
           'severity': 'low',
           'short_desc': 'blabla',
           'status': 'CLOSED',
           'target_milestone': '---',
           'target_release': ['---'],
           'verified': [],
           'version': '2.1r'}

    dct_sorted_z = copy(dct)
    dct_sorted_z['z'] = sorted(dct_sorted_z['z'])

    dct_diff = copy(dct)
    del dct_diff['z']

    DCT = '26a7e42282b97a7c6ee8ecafd035cd5c72706398'
    DCT_SORTED_Z = '49848f0184d83be0287b16c55076f558d849bb9f'
    DCT_DIFF = 'ba439203e121d8a761d14bcbe67e49ed023c07bf'

    assert dct != dct_sorted_z

    assert jsonhash(dct) == DCT
    assert jsonhash(dct_sorted_z) == DCT_SORTED_Z
    assert jsonhash(dct_diff) == DCT_DIFF

    ' list sort order is an identifier of a unique object '
    assert jsonhash(dct) != jsonhash(dct_sorted_z)


def test_load_file():
    # also tests utils.{load_pickle, load_csv, load_json, load_shelve}
    from metrique.utils import load_file
    files = ['test.csv', 'test.json', 'test.pickle', 'test.db']
    for f in files:
        print 'Loading %s' % f
        path = os.path.join(fixtures, f)
        objects = load_file(path)
        print '... got %s' % objects
        assert len(objects) == 1
        assert map(unicode, sorted(objects[0].keys())) == ['col_1', 'col_2']


def test_load():
    from metrique.utils import load
    path_glob = os.path.join(fixtures, 'test*.csv')

    x = load(path_glob)
    assert len(x) == 2
    assert 'col_1' in x[0].keys()
    assert 1 in x[0].values()
    assert 100 in x[1].values()

    x = load(path_glob, _oid=True)
    assert '_oid' in x[0].keys()
    assert x[0]['_oid'] == 1
    assert x[1]['_oid'] == 2

    set_oid_func = lambda o: dict(_oid=42, **o)
    x = load(path_glob, _oid=set_oid_func)
    assert x[0]['_oid'] == 42
    assert x[1]['_oid'] == 42

    # check that we can get a dataframe
    x = load(path_glob, as_df=True)
    assert hasattr(x, 'ix')

    # passing in a dataframe should return back the same dataframe...
    _x = load(x)
    assert _x is x

    # check that we can grab data from the web
    uri = 'https://mysafeinfo.com/api/data?list=days&format=csv'
    x = load(uri, filetype='csv')
    assert len(x) == 7
    x = load(path_glob)


def test_load_config():
    from metrique.utils import load_config

    try:
        x = load_config()
    except TypeError:
        pass

    x = load_config(path=None)

    config_file = os.path.join(etc, 'test_conf.json')
    x = load_config(path=config_file)
    assert x
    assert x['file'] is True

    try:
        x = load_config(path='BAD_PATH')
    except IOError:
        pass


def test_profile(capsys):
    from metrique.utils import profile

    @profile
    def test():
        return

    test()
    out, err = [x.strip() for x in capsys.readouterr()]
    assert out  # we should have some output printed to stdout


def test_rupdate():
    from metrique.utils import rupdate
    source = {'toplevel': {'nested': 1, 'hidden': 1}}
    target = {'toplevel': {'nested': 2}}

    updated = rupdate(source, target)
    assert 'toplevel' in updated
    assert 'nested' in updated['toplevel']
    assert 'hidden' in updated['toplevel']
    assert updated['toplevel']['nested'] == 2


def test_safestr():
    from metrique.utils import safestr
    str_ = '     abc123:;"/\\.,\n\t'
    assert safestr(str_) == 'abc123'


def test_sys_call():
    from metrique.utils import sys_call

    try:
        sys_call('ls FILE_THAT_DOES_NOT_EXIST')
    except Exception:
        pass

    csv_path = os.path.join(fixtures, 'test.csv')
    out = sys_call('ls %s' % csv_path)
    assert out == csv_path


def test_to_encoding():
    from metrique.utils import to_encoding
    str_utf8 = unicode('--台北--')
    str_ = str('hello')

    assert to_encoding(str_utf8, 'utf-8')
    assert to_encoding(str_, 'utf-8')

    assert to_encoding(str_utf8, 'ascii')
    assert to_encoding(str_, 'ascii')

    try:
        to_encoding(str_, 'ascii', errors='strict')
    except UnicodeEncodeError:
        pass


def test_ts2dt():
    ''' args: ts, milli=False, tz_aware=True '''
    from metrique.utils import ts2dt

    # FIXME: millisecond precision, better?
    now_time = int(time())
    now_time_milli = int(time()) * 1000
    now_date = datetime.utcfromtimestamp(now_time)
    now_date_iso = now_date.isoformat()

    ' datetime already, return it back'
    assert ts2dt(now_date) == now_date

    ' tz_aware defaults to false '
    try:
        ' cant compare offset-naive and offset-aware datetimes '
        assert ts2dt(now_time) == now_date
    except TypeError:
        pass

    assert ts2dt(now_date, tz_aware=False) == now_date

    assert ts2dt(now_time_milli, milli=True, tz_aware=False) == now_date

    try:
        ' string variants not accepted "nvalid literal for float()"'
        ts2dt(now_date_iso) == now_date
    except ValueError:
        pass


def test_urlretrieve():
    from metrique.utils import urlretrieve
    uri = 'https://mysafeinfo.com/api/data?list=days&format=csv'
    saveas = os.path.join(cache_dir, 'test_download.csv')

    qremove(saveas)
    _path = urlretrieve(uri, saveas=saveas, cache_dir=cache_dir)
    assert _path == saveas
    assert os.path.exists(_path)
    assert os.stat(_path).st_size > 0
    qremove(_path)


def test_utcnow():
    ' args: as_datetime=False, tz_aware=False '
    from metrique.utils import utcnow

    now_date = datetime.utcnow().replace(microsecond=0)
    now_date_utc = datetime.now(pytz.utc).replace(microsecond=0)
    now_time = int(calendar.timegm(now_date.utctimetuple()))

    # FIXME: millisecond resolution?
    assert utcnow(as_datetime=False, drop_micro=True) == now_time
    assert utcnow(as_datetime=True, drop_micro=True) == now_date
    _ = utcnow(as_datetime=True, tz_aware=True, drop_micro=True)
    assert _ == now_date_utc
    assert utcnow(as_datetime=False,
                  tz_aware=True, drop_micro=True) == now_time
