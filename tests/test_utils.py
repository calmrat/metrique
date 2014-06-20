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
import string
from time import time, sleep

from utils import is_in, set_env

env = set_env()
exists = os.path.exists

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')
fixtures = os.path.join(testroot, 'fixtures')
etc = os.path.join(testroot, 'etc')
cache_dir = env['METRIQUE_CACHE']
tmp_dir = env['METRIQUE_TMP']
log_dir = env['METRIQUE_LOGS']


def test_backup():
    from metrique.utils import backup, rand_chars, remove_file
    f1 = os.path.join(cache_dir, '%s' % rand_chars(prefix='backup'))
    f2 = os.path.join(cache_dir, '%s' % rand_chars(prefix='backup'))
    open(f1, 'w').close()
    open(f2, 'w').close()
    assert [exists(f) for f in (f1, f2)]
    saveas = backup('%s %s' % (f1, f2))
    assert exists(saveas)
    remove_file(saveas)
    saveas = backup((f1, f2))
    assert exists(saveas)
    remove_file((saveas, f1, f2))


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
    from metrique.utils import clear_stale_pids, remove_file
    pid_dir = '/tmp'
    # we assume we'll have a pid 1, but never -1
    pids = map(unicode, [1, -1])
    pids_str = '1, -1'
    pid_files = [os.path.join(pid_dir, '%s.pid' % pid) for pid in pids]
    prefix = ''
    for pid, pid_file in zip(pids, pid_files):
        with open(pid_file, 'w') as f:
            f.write(str(pid))
    running = clear_stale_pids(pids, pid_dir, prefix)
    assert '1' in running
    assert '-1' not in running
    assert exists(pid_files[0])
    assert not exists(pid_files[1])
    running = clear_stale_pids(pids_str, pid_dir, prefix)
    assert '1' in running
    assert '-1' not in running
    assert exists(pid_files[0])
    assert not exists(pid_files[1])
    remove_file(pid_files[0])


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

    try:  # should fail
        x = configure(config_file=config_file, section_key='I_DO_NOT_EXIST')
    except KeyError:
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


def test__data_export():
    from metrique.utils import _data_export
    import pandas
    data = [{'a': 'a'}]
    data_df = pandas.DataFrame(data)
    for v in (data, data_df):
        assert _data_export(v, as_df=True).to_dict() == data_df.to_dict()
        assert _data_export(v, as_df=False) == data


def test_debug_setup(capsys):
    from metrique.utils import debug_setup, remove_file

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
    log_format = '%(message)s'
    _l = debug_setup(logger=logger_test, log_format=log_format)
    assert _l is logger_test
    assert _l.name == 'test'

    _l = debug_setup(logger=logger_test, log_format=log_format,
                     log2file=False, log2stdout=True)
    _l.info('*')
    out, err = [x.strip() for x in capsys.readouterr()]
    #assert out == ''
    assert err == '*'

    # no output should seen for info(), since we set level
    # to warn, but issue an info call
    _l = debug_setup(logger=logger_test, log_format=log_format,
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
        remove_file(_lf)


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


def test_file_is_empty():
    from metrique.utils import file_is_empty, write_file, rand_chars
    from metrique.utils import remove_file

    f1 = os.path.join(cache_dir, rand_chars(prefix='empty_test_1'))
    f2 = os.path.join(cache_dir, rand_chars(prefix='not_empty_test_2'))

    write_file(f1, '')
    write_file(f2, 'not empty')

    assert file_is_empty(f1)
    assert exists(f1)
    assert file_is_empty(f1, remove=True)
    assert not exists(f1)

    assert not file_is_empty(f2)

    try:
        # not a valid path
        file_is_empty('DOES_NOT_EXIST')
    except RuntimeError:
        pass

    try:
        # not a valid path
        file_is_empty(True)
    except RuntimeError:
        pass

    remove_file(f2)
    assert not exists(f2)


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

    get_cube(cube=cube, pkgs=pkgs, cube_paths=paths, init=True)

    try:
        get_cube(cube='DOES_NOT_EXIST')
    except RuntimeError:
        pass


def test__get_datetime():
    from metrique.utils import _get_datetime, utcnow, dt2ts

    now_tz = utcnow(tz_aware=True, as_datetime=True)
    now = now_tz.replace(tzinfo=None)
    try:
        now_tz == now  # can't compare tz_aware <> naive
    except TypeError:
        pass
    # default is tz_aware=False
    assert _get_datetime(now_tz) == now
    assert _get_datetime(now) == now
    assert _get_datetime(now_tz, tz_aware=True) == now_tz
    assert _get_datetime(now, tz_aware=True) == now_tz
    assert _get_datetime(dt2ts(now), tz_aware=True) == now_tz


def test_get_pid():
    from metrique.utils import get_pid, rand_chars, remove_file

    assert get_pid() == 0
    assert get_pid(None) == 0

    path = os.path.join(cache_dir, '%s.pid' % rand_chars(prefix='get_pid'))

    try:
        get_pid(path)
    except IOError:
        pass

    with open(path, 'w') as f:
        f.write("1")
    assert exists(path)
    assert get_pid(path) == 1
    remove_file(path)

    with open(path, 'w') as f:
        f.write("a")
    try:
        get_pid(path)
    except ValueError:
        pass
    remove_file(path)


def test_get_pids():
    from metrique.utils import get_pids as _

    # any pid files w/out a /proc/PID process mapped are removed
    assert _(cache_dir, clear_stale=True) == []

    fake_pid = 11111
    assert fake_pid not in _(cache_dir, clear_stale=False)

    pid = 99999
    path = os.path.join(cache_dir, '%s.pid' % pid)
    with open(path, 'w') as f:
        f.write(str(pid))

    # don't clear the fake pid_file... useful for testing only
    pids = _(cache_dir, clear_stale=False)
    assert pid in pids
    assert all([True if isinstance(x, int) else False for x in pids])

    # clear it now and it should not show up in the results
    assert pid not in _(cache_dir, clear_stale=True)


def test_get_timezone_converter():
    ' args: from_timezone '
    ' convert is always TO utc '
    from metrique.utils import utcnow, get_timezone_converter

    # note: caching timezones always takes a few seconds
    est = 'US/Eastern'
    EST = pytz.timezone(est)

    now_utc_tz = utcnow(tz_aware=True, as_datetime=True)
    now_utc = now_utc_tz.replace(tzinfo=None)

    now_est = copy(now_utc_tz)
    now_est_tz = now_est.astimezone(EST)
    now_est = now_est_tz.replace(tzinfo=None)

    assert get_timezone_converter(None) is None

    c = get_timezone_converter(est)
    assert c(None) is None
    assert c(now_est) == now_utc
    assert c(now_est_tz) == now_utc
    assert c(now_est_tz) == c(now_est)

    c = get_timezone_converter(est, tz_aware=True)
    assert c(now_est) == now_utc_tz
    assert c(now_est_tz) == c(now_est)
    assert c(now_est_tz) == now_utc_tz


def test_git_clone():
    # FIXME: THIS ONE IS CAUSING SOME INTERESTING PROBLEMS?
    from metrique.utils import git_clone, safestr, remove_file
    uri = 'https://github.com/kejbaly2/tornadohttp.git'
    local_path = os.path.join(cache_dir, safestr(uri))
    remove_file(local_path, force=True)

    _t = time()
    repo = git_clone(uri, pull=False, reflect=False, cache_dir=cache_dir)
    assert repo == local_path
    not_cached = time() - _t

    _t = time()
    repo = git_clone(uri, pull=False, reflect=True, cache_dir=cache_dir)
    cached = time() - _t

    assert repo.path == local_path
    assert cached < not_cached

    git_clone(uri, pull=True, reflect=False, cache_dir=cache_dir)
    remove_file(local_path, force=True)


def test_is_empty():
    # 0 is not considered NULL, as 0 is a legit number too...
    from metrique.utils import is_empty
    import pandas
    import numpy
    empties = [None, '', 0.0, 0, 0L, {}, [], pandas.DataFrame()]
    not_empties = ['hello', '  \t\n\t  ',
                   -1, 1,
                   {'key': 'value'}, [1]]
    empties += [pandas.NaT, numpy.NaN]
    for x in empties:
        empty = is_empty(x, except_=False)
        print '%s is empty? %s' % (repr(x), empty)
        assert empty is True
        try:
            empty = is_empty(x, except_=True)
        except RuntimeError:
            pass
    for x in not_empties:
        empty = is_empty(x, except_=False)
        print '%s is empty? %s' % (repr(x), empty)
        assert empty is False


def test_is_null():
    # 0 is not considered NULL, as 0 is a legit number too...
    # and causes issues with datetimes, since epoch(0)->1970, 1, 1
    # same with other 'empty' values like '', [], etc.
    from metrique.utils import is_null
    import pandas
    import numpy
    nulls = [None]
    not_nulls = ['hello', -1, 1, 0.0, 0, 0L, '', '  \t\n\t  ',
                 {'key': 'value'}, [1], {}, [], pandas.DataFrame()]
    nulls += [pandas.NaT, numpy.NaN]
    for x in nulls:
        null = is_null(x, except_=False)
        print '%s is null? %s' % (repr(x), null)
        assert null is True
        try:
            null = is_null(x, except_=True)
        except RuntimeError:
            pass
    for x in not_nulls:
        null = is_null(x, except_=False)
        print '%s is null? %s' % (repr(x), null)
        assert null is False


def test_is_true():
    from metrique.utils import is_true
    assert is_true(True) is True
    assert is_true(None, except_=False) is False
    try:
        is_true('')
    except RuntimeError:
        pass


def test_is_defined():
    from metrique.utils import is_defined
    defined = [-1, 1, 0.1, True, 'h']
    not_defined = ['', 0, None, False, [], {}]
    for x in defined:
        true = is_defined(x, except_=False)
        print '%s is defined? %s' % (repr(x), true)
        assert true is True
        try:
            true = is_defined(x, except_=True)
        except RuntimeError:
            pass
    for x in not_defined:
        true = is_defined(x, except_=False)
        print '%s is defined? %s' % (repr(x), true)
        assert true is False


def test_json_encode_default():
    ' args: obj '
    from metrique.utils import json_encode_default

    now = datetime.utcnow()

    dct = {"a": now, "b": "1"}

    _dct = json.loads(json.dumps(dct, default=json_encode_default))
    assert isinstance(_dct["a"], str)
    assert float(_dct["a"])

    dct = {"a": json_encode_default}
    try:
        _dct = json.dumps(dct, default=json_encode_default)
    except TypeError:
        pass


def test_jsonhash():
    from metrique.utils import jsonhash

    dct = {'a': [3, 2, 1],
           'z': ['a', 'c', 'b', 1],
           'b': {1: [], 3: {}},
           'product': 'thisorthat',
           'qe_cond_nak': None,
           'target_release': ['---'],
           'verified': [1],
           'version': '2.1r'}

    dct_sorted_z = copy(dct)
    dct_sorted_z['z'] = sorted(dct_sorted_z['z'])

    dct_diff = copy(dct)
    del dct_diff['z']

    DCT = '6951abb765573ee052402c53dd7c0a5a09fc870b'
    DCT_SORTED_Z = 'ab3bd17ed90c5051205b2b1df9741f52c6a6755b'
    DCT_DIFF = 'ac12039e077bd03879d7481f299678b73b6216e5'

    assert dct != dct_sorted_z

    assert jsonhash(dct) == DCT
    assert jsonhash(dct_sorted_z) == DCT_SORTED_Z
    assert jsonhash(dct_diff) == DCT_DIFF

    ' list sort order is an identifier of a unique object '
    assert jsonhash(dct) != jsonhash(dct_sorted_z)

    # pop off 'product'
    ex_dct = {'a': [3, 2, 1],
              'z': ['a', 'c', 'b', 1],
              'b': {1: [], 3: {}},
              'qe_cond_nak': None,
              'target_release': ['---'],
              'verified': [1],
              'version': '2.1r'}

    EX = 'c6d26b63a50ce402ca15eb79383b5dbe91e304e9'
    # jsonhashing ex_dct (without product) should be
    # equal to jsonhashing dct with exclude 'product'
    assert jsonhash(ex_dct) == EX
    assert jsonhash(dct, exclude=['product']) == EX


def test_list2str():
    from metrique.utils import list2str
    l = [1, 1.1, '1', None, 0]
    ok = '1,1.1,1,None,0'
    assert list2str(l) == ok
    ok = '1, 1.1, 1, None, 0'
    assert list2str(l, delim=', ') == ok
    ok = '1 1.1 1 None 0'
    assert list2str(l, delim=' ') == ok
    assert list2str(ok) == ok
    assert list2str(None) == ''
    try:
        list2str(list2str)
    except TypeError:
        pass


def test_load_file():
    # also tests utils.{load_pickle, load_csv, load_json, load_shelve}
    from metrique.utils import load_file
    files = ['test.csv', 'test.json', 'test.pickle']
    for f in files:
        print 'Loading %s' % f
        path = os.path.join(fixtures, f)
        objects = load_file(path)
        print '... got %s' % objects
        assert len(objects) == 1
        assert map(unicode, sorted(objects[0].keys())) == ['col_1', 'col_2']

    try:
        load_file('DOES_NOT_EXIST')
    except IOError:
        pass

    try:
        load_file(os.path.join(fixtures, 'file_with_unknown.extension'))
    except TypeError:
        pass


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

    try:
        set_oid_func = 'i am a string, not a func'
        x = load(path_glob, _oid=set_oid_func)
    except TypeError:
        pass

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

    try:  # can load only files or dataframes
        load(1)
    except ValueError:
        pass

    empty = os.path.join(fixtures, 'empty.csv')
    try:
        load(empty, header=None)
    except ValueError:
        pass

    header = os.path.join(fixtures, 'header_only.csv')
    try:
        load(header)
    except RuntimeError:
        pass

    try:
        load('DOES_NOT_EXIST')
    except IOError:
        pass

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


def test_make_dirs():
    from metrique.utils import make_dirs, rand_chars, remove_file
    d_1 = rand_chars(prefix='make_dirs')
    d_2 = rand_chars()
    base = os.path.join(tmp_dir, d_1)
    rand_dirs = os.path.join(base, d_2)
    path = os.path.join(tmp_dir, rand_dirs)
    assert make_dirs(path) == path
    assert exists(path)
    remove_file(base, force=True)
    assert not exists(base)
    for _ in ['', 'relative/dir']:
        # requires absolute path!
        try:
            make_dirs(_)
        except OSError:
            pass


def test_move():
    from metrique.utils import move, rand_chars, remove_file
    dest = tmp_dir
    rel_path_1 = rand_chars(prefix='move')
    path_1 = os.path.join(cache_dir, rel_path_1)
    _path_1 = os.path.join(dest, rel_path_1)
    open(path_1, 'a').close()

    rel_path_2 = rand_chars(prefix='move')
    path_2 = os.path.join(cache_dir, rel_path_2)
    open(path_2, 'a').close()

    paths = (path_1, path_2)

    assert exists(path_1)
    move(path_1, dest)
    assert not exists(path_1)
    move(_path_1, cache_dir)

    assert exists(path_2)
    move(paths, dest)
    assert not any((exists(path_1), exists(path_2)))
    remove_file(paths, force=True)
    remove_file(dest, force=True)
    remove_file(tmp_dir, force=True)

    try:
        move('DOES_NOT_EXST', 'SOMEWHERE')
    except IOError:
        pass
    assert move('DOES_NOT_EXST', 'SOMEWHERE', quiet=True) == []


def test_profile(capsys):
    from metrique.utils import profile

    @profile
    def test():
        return

    test()
    out, err = [x.strip() for x in capsys.readouterr()]
    assert out  # we should have some output printed to stdout


def test_rand_chars():
    from metrique.utils import rand_chars
    str_ = rand_chars()
    assert isinstance(str_, basestring)
    assert isinstance(str_, unicode)
    assert len(str_) == 6
    OK = set(string.ascii_uppercase + string.digits)
    assert all(c in OK for c in str_)
    assert rand_chars() != rand_chars()
    assert rand_chars(prefix='rand_chars')[0:10] == 'rand_chars'


def test_read_file():
    from metrique.utils import read_file

    paths = fixtures
    try:
        read_file('')
    except ValueError:
        pass

    content = 'col_1, col_2'
    f = read_file('header_only.csv', paths=paths).strip()
    assert f == content
    assert read_file('templates/etc/metrique.json', paths=paths)

    fd = read_file('header_only.csv', paths=paths, raw=True)
    assert isinstance(fd, file)

    lst = read_file('header_only.csv', paths=paths, as_list=True)
    assert isinstance(lst, list)
    assert len(lst) == 1

    try:
        read_file('DOES_NOT_EXIST')
    except IOError:
        pass


def test_remove_file():
    from metrique.utils import remove_file, rand_chars, make_dirs
    assert remove_file(None) == []
    assert remove_file('') == []
    assert remove_file('DOES_NOT_EXIST') == []
    path = os.path.join(cache_dir, rand_chars())
    assert not exists(path)
    open(path, 'w').close()
    assert exists(path)
    assert remove_file(path) == path
    assert not exists(path)
    open(path, 'w').close()
    assert remove_file(path) == path
    assert not exists(path)
    assert remove_file('DOES_NOT_EXIST') == []
    # build a simple nested directory tree
    path = os.path.join(cache_dir, rand_chars())
    assert make_dirs(path) == path
    try:
        remove_file(path)
    except RuntimeError:
        pass
    assert remove_file(path, force=True) == path


def test_rsync():
    from metrique.utils import rsync, sys_call, rand_chars, remove_file
    from metrique.utils import read_file
    #remove_file(f_1)
    #remove_file(dest, force=True)
    if not sys_call('which rsync'):
        return   # skip this test if rsync isn't available
    fname = rand_chars(prefix='rsync')
    path = os.path.join(cache_dir, fname)
    with open(path, 'w') as f:
        f.write('test')
    dest = os.path.join(tmp_dir, 'rsync')
    rsync(targets=path, dest=dest)
    assert read_file(os.path.join(dest, fname)) == 'test'
    with open(path, 'w') as f:
        f.write('test 2')
    rsync(targets=path, dest=dest)
    assert read_file(os.path.join(dest, fname)) == 'test 2'
    remove_file(path, force=True)
    remove_file(dest, force=True)


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
    assert safestr(None) == ''


def test_str2list():
    from metrique.utils import str2list

    assert str2list(None) == []

    a_lst = ['a', 'b', 'c', 'd', 'e']
    a_str = 'a, b,     c,    d , e'
    assert str2list(a_str) == a_lst

    b_str = '1, 2,     3,    4 , 5'
    b_lst = [1., 2., 3., 4., 5.]
    assert str2list(b_str, map_=float) == b_lst

    for i in [None, a_lst, {}, 1, 1.0]:
        # try some non-string input values
        try:
            str2list(i)
        except TypeError:
            pass


def test_sys_call():
    from metrique.utils import sys_call

    try:
        sys_call('ls FILE_THAT_DOES_NOT_EXIST')
    except Exception:
        pass

    assert sys_call('ls FILE_THAT_DOES_NOT_EXIST', ignore_errors=True) is None

    csv_path = os.path.join(fixtures, 'test.csv')
    out = sys_call('ls %s' % csv_path)
    assert out == csv_path

    # should work if passed in as a list of args too
    out = sys_call(['ls', csv_path])
    assert out == csv_path


def test_terminate():
    from metrique.utils import terminate, sys_call, get_pid, clear_stale_pids
    import signal

    pid_file = os.path.join(cache_dir, 'test.pid')
    sys_call('sleep 30', fork=True, pid_file=pid_file, shell=False)
    sleep(1)
    pid = get_pid(pid_file)
    running = clear_stale_pids(pid)
    assert running
    terminate(pid, sig=signal.SIGTERM)
    sleep(1)
    running = clear_stale_pids(pid)
    assert not running
    # since we didn't tell it where to find the pid_file
    # it won't be cleaned up
    assert exists(pid_file)
    # this time we point to a pid_file and it gets cleaned up
    terminate(pid_file, sig=signal.SIGTERM)
    assert not exists(pid_file)
    # invalid pids are ignored
    assert terminate(-1) is None


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

    assert ts2dt(now_date_iso) == now_date
    try:
        ts2dt('not a valid datetime str') == now_date
    except TypeError:
        pass


def test_urlretrieve():
    from metrique.utils import urlretrieve, remove_file
    uri = 'https://mysafeinfo.com/api/data?list=days&format=csv'
    saveas = os.path.join(cache_dir, 'test_download.csv')

    remove_file(saveas)
    _path = urlretrieve(uri, saveas=saveas, cache_dir=cache_dir)
    assert _path == saveas
    assert exists(_path)
    assert os.stat(_path).st_size > 0
    remove_file(_path)

    try:
        urlretrieve('does not exist')
    except RuntimeError:
        pass


def test_utcnow():
    ' args: as_datetime=False, tz_aware=False '
    from metrique.utils import utcnow

    # default behaivor is as_datetime == False, which return epoch/float
    assert isinstance(utcnow(), float)

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


def test_validate_password():
    from metrique.utils import validate_password

    OK = 'helloworld42'
    BAD1 = 'short'
    BAD2 = None

    assert validate_password(OK) == OK

    for bad in (BAD1, BAD2):
        try:
            validate_password(bad)
        except ValueError:
            pass


def test_validate_roles():
    from metrique.utils import validate_roles

    valid_roles = ['SELECT', 'ADMIN', 'WRITE']
    OK = ['SELECT', 'ADMIN']
    BAD = ['BLOWUP', 'ADMIN']

    assert validate_roles(OK, valid_roles) == sorted(OK)
    try:
        validate_roles(OK, valid_roles) == sorted(BAD)
    except ValueError:
        pass


def test_validate_username():
    from metrique.utils import validate_username

    restricted = ['admin']
    ok = 'helloworld'
    OK = 'HELLOWORLD'
    BAD1 = '1'
    BAD2 = None
    BAD3 = 'admin'

    assert validate_username(ok) == ok
    assert validate_username(OK) == ok

    for bad in (BAD1, BAD2, BAD3):
        try:
            validate_username(bad, restricted_names=restricted)
        except (ValueError, TypeError):
            pass


def test_virtualenv():
    from metrique.utils import virtualenv_activate, virtualenv_deactivate
    from metrique.utils import active_virtualenv
    av = active_virtualenv
    orig_venv = av()
    if not orig_venv:
        # this test will only work if we START in a virtenv
        # otherwise, we don't know of a virtenv we can test against
        return
    assert av() == os.environ['VIRTUAL_ENV']
    assert virtualenv_deactivate() is True
    assert virtualenv_deactivate() is None
    assert av() == ''
    assert '' == os.environ['VIRTUAL_ENV']
    virtualenv_activate(orig_venv)
    assert av() == os.environ['VIRTUAL_ENV']
    assert av() == orig_venv

    assert virtualenv_activate() is None
    assert virtualenv_activate(orig_venv) is None

    try:
        virtualenv_activate('virtenv_that_doesnt_exist')
    except OSError:
        pass


def test_write_file():
    from metrique.utils import write_file, rand_chars, read_file
    from metrique.utils import remove_file

    f1 = os.path.join(cache_dir, rand_chars())
    write_file(f1, 'hello world')
    assert exists(f1)
    assert read_file(f1) == 'hello world'

    # can't overwrite files with default settings
    try:
        write_file(f1, 'hello world')
    except RuntimeError:
        pass

    write_file(f1, 'hello world', force=True)
    assert exists(f1)
    assert read_file(f1) == 'hello world'

    write_file(f1, 'hello world', mode='a')
    assert exists(f1)
    assert read_file(f1) == 'hello worldhello world'

    remove_file(f1)
