#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import calendar
from copy import copy
from datetime import datetime as dt
from pytz import utc
from time import time


def test_batch_gen():
    '''

    '''
    from metriqueu.utils import batch_gen

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


def test_dt2ts():
    '''  '''
    from metriqueu.utils import dt2ts

    # FIXME: millisecond precision, better?
    now_time = int(time())
    now_date = dt.utcfromtimestamp(now_time)
    now_date_iso = now_date.isoformat()

    assert dt2ts(now_time) == now_time
    assert dt2ts(now_date) == now_time
    assert dt2ts(now_date_iso) == now_time


def test_jsonhash():
    from metriqueu.utils import jsonhash

    dct = {'a': [3, 2, 1], 'z': ['a', 'c', 'b', 1], 'b': {1: [], 3: {}}}

    dct_sorted_z = copy(dct)
    dct_sorted_z['z'] = sorted(dct_sorted_z['z'])

    dct_diff = copy(dct)
    del dct_diff['z']

    DCT = '541d0fa961265d976d9a27e8632787875dc58406'
    DCT_SORTED_Z = 'ca4631674276933bd251bd4bc86372138a841a4b'
    DCT_DIFF = '07d6c518867fb6b6c77c0ec1d835fb800419fc24'

    assert dct != dct_sorted_z

    assert jsonhash(dct) == DCT
    assert jsonhash(dct_sorted_z) == DCT_SORTED_Z
    assert jsonhash(dct_diff) == DCT_DIFF

    ' list sort order is an identifier of a unique object '
    assert jsonhash(dct) != jsonhash(dct_sorted_z)


def test_milli2sec():
    '''
    args: ts
    '''
    from metriqueu.utils import milli2sec

    now_time = time()
    now_time_milli = now_time * 1000

    assert milli2sec(now_time_milli) == now_time


def test_new_oid():
    '''
    '''
    from metriqueu.utils import new_oid
    assert isinstance(new_oid(), basestring)
    assert len(new_oid()) == 24


def test_set_default():
    ''' args: key, default, null_ok=False, err_msg=None '''
    from metriqueu.utils import set_default

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
    from metriqueu.utils import ts2dt

    # FIXME: millisecond precision, better?
    now_time = int(time())
    now_time_milli = int(time()) * 1000
    now_date = dt.utcfromtimestamp(now_time)
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
    from metriqueu.utils import utcnow

    now_date = dt.utcnow().replace(microsecond=0)
    now_date_utc = dt.now(utc).replace(microsecond=0)
    now_time = lambda x: int(calendar.timegm(x.utctimetuple()))

    # FIXME: millisecond resolution?
    assert utcnow(drop_micro=True) == now_time(now_date)
    assert utcnow(as_datetime=True, drop_micro=True) == now_date
    assert utcnow(tz_aware=True, drop_micro=True) == now_date_utc


def test_strip_split():
    ' args: item '
    from metriqueu.utils import strip_split

    a_lst = ['a', 'b', 'c', 'd', 'e']
    a_str = 'a, b,     c,    d , e'
    assert strip_split(a_str) == a_lst
    assert strip_split(None) == []
    assert strip_split(a_lst) == a_lst

    try:
        strip_split({})
    except TypeError:
        pass
