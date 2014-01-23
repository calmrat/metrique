#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from datetime import datetime
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
