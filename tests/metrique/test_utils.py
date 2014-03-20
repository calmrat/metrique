#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import calendar
from copy import copy
from datetime import datetime
import os
import pytz
import simplejson as json
from time import time

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')


def test_batch_gen():
    '''

    '''
    from metrique.utils import batch_gen

    # test arg signature, aka API

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
    get_cube('sqldata_teiid')

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
    path = os.path.join(pid_dir, 'metriqued.%s.pid' % pid)
    with open(path, 'w') as f:
        f.write(str(pid))

    # don't clear the fake pidfile... useful for testing only
    pids = _(pid_dir, clear_stale=False)
    assert pid in pids
    assert all([True if isinstance(x, int) else False for x in pids])

    # clear it now and it should not show up in the results
    assert pid not in _(pid_dir, clear_stale=True)


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

    DCT = 'a1ddd970e7d0a6a38e2799c503da41cc85f79a8b'
    DCT_SORTED_Z = '8683dda62f0986d570713be610e967b706d6a161'
    DCT_DIFF = '4610d85fbd7f6cdf72ab6bf4db4d05cf893eb407'

    assert dct != dct_sorted_z

    assert jsonhash(dct) == DCT
    assert jsonhash(dct_sorted_z) == DCT_SORTED_Z
    assert jsonhash(dct_diff) == DCT_DIFF

    ' list sort order is an identifier of a unique object '
    assert jsonhash(dct) != jsonhash(dct_sorted_z)


def test_set_default():
    ''' args: key, default, null_ok=False, err_msg=None '''
    from metrique.utils import set_default

    k = None  # key
    d = None  # default
    n = True  # null_ok
    e = None  # err_msg

    assert set_default(k, d, n, e) is None

    k = []
    assert set_default(k, d, n, e) == []

    d = list
    assert set_default(k, d, n, e) == []

    d = 'hello'
    assert set_default(k, d, n, e) != d
    assert set_default(k, d, n, e) != 42

    n = False
    try:
        set_default(k, d, n, e)
    except RuntimeError:
        pass

    e = 'oops'
    try:
        set_default(k, d, n, e)
    except RuntimeError as e:
        assert e == 'oops'


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

    ' tz_aware defaults to true '
    try:
        ' cant compare offset-naive and offset-aware datetimes '
        assert ts2dt(now_time) != now_date
    except TypeError:
        pass

    assert ts2dt(now_date, tz_aware=False) == now_date

    assert ts2dt(now_time_milli, milli=True, tz_aware=False) == now_date

    try:
        ' string variants not accepted "nvalid literal for float()"'
        ts2dt(now_date_iso) == now_date
    except ValueError:
        pass


def test_utcnow():
    ' args: as_datetime=False, tz_aware=False '
    from metrique.utils import utcnow

    now_date = datetime.utcnow().replace(microsecond=0)
    now_date_utc = datetime.now(pytz.utc).replace(microsecond=0)
    now_time = int(calendar.timegm(now_date.utctimetuple()))

    # FIXME: millisecond resolution?
    assert utcnow(drop_micro=True) == now_time
    assert utcnow(as_datetime=True, drop_micro=True) == now_date
    _ = utcnow(as_datetime=True, tz_aware=True, drop_micro=True)
    assert _ == now_date_utc
    assert utcnow(tz_aware=True, drop_micro=True) == now_time


# FIXME: THIS IS REALLY SLOW... reenable by adding test_ prefix
def get_timezone_converter():
    ' args: from_timezone '
    ' convert is always TO utc '
    from metrique.utils import get_timezone_converter

    # note: caching timezones always takes a few seconds
    good = 'US/Eastern'
    good_tz = pytz.timezone(good)

    now_utc = datetime.now(pytz.utc)

    now_est = copy(now_utc)
    now_est = now_est.astimezone(good_tz)
    now_est = now_est.replace(tzinfo=None)

    c = get_timezone_converter(good)
    assert c(None, now_est) == now_utc
