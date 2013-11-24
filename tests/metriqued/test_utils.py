#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from copy import copy
from datetime import datetime
import os
import pytz

from metriqueu.utils import dt2ts


def test_date_pql_string():
    from metriqued.utils import date_pql_string as _

    assert _(None) == '_end == None'
    assert _('~') == ''

    d1 = datetime(2000, 1, 1, 0, 0, 0)
    d1_ts = dt2ts(d1)

    ba = '_start <= %f and (_end >= %f or _end == None)' % (d1_ts, d1_ts)
    d1_str = str(d1)  # test no T b/w date/time
    # test passing only a date (no ~ separator)
    assert _(d1_str) == ba

    d1_iso = d1.isoformat()  # test with T b/w date/time
    # test 'before()'
    assert _('~%s' % d1_iso) == '_start <= %f' % d1_ts

    # test 'after()'
    d1_tz = d1.replace(tzinfo=pytz.UTC).isoformat()  # test with timezone
    assert _('%s~' % d1_tz) == '(_end >= %f or _end == None)' % d1_ts

    d1_date = '2000-01-01'  # without time
    assert _('~%s' % d1_date) == '_start <= %f' % d1_ts

    # test 'date~date' date range, passing in raw datetime objects
    d1 = datetime(2000, 1, 1, 0, 0, 0)
    d1_ts = dt2ts(d1)
    d2 = datetime(2000, 1, 2, 0, 0, 0)
    d2_ts = dt2ts(d2)
    ba = '_start <= %f and (_end >= %f or _end == None)' % (d2_ts, d1_ts)
    assert _('%s~%s' % (d1, d2)) == ba


def test_get_pids():
    from metriqued.utils import get_pids as _

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


def test_jsonhash():
    from metriqued.utils import jsonhash

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


def parse_pql_query():
    from metriqued.utils import parse_pql_query as _

    assert _(None) == {}
    try:
        assert _(True)
    except TypeError:
        pass
    assert 'i_heart == "metrique"' == {'i_heart': 'metrique'}


def test_query_add_date():
    from metriqued.utils import query_add_date as _
    d1 = datetime(2000, 1, 1, 0, 0, 0)
    d1_ts = dt2ts(d1)

    q = 'i_heart == "metrique"'
    _pql = '_start <= %f' % d1_ts
    assert _(q, '~') == q
    assert _(q, None) == '%s and _end == None' % q
    assert _(q, '~%s' % d1) == '%s and %s' % (q, _pql)
