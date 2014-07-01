#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


def test_init():
    from metrique.result import Result
    from metrique.utils import utcnow

    try:
        Result()
    except ValueError:
        pass
    else:
        assert False, "Can't init with null data"

    try:
        Result({})
    except ValueError:
        pass
    else:
        assert False, "Can't init with empty data"

    try:
        data = [{'a': 1, 'b': 2}]
        Result(data)
    except ValueError:
        pass
    else:
        assert False, "_start and _end must be defined..."

    data = [{'_start': utcnow(), '_end': None, 'a': 1, 'b': 2}]
    Result(data)


def get_result_object():
    from time import time
    from random import randint
    from metrique.result import Result
    t = int(time())
    data = []
    for oid in range(1, 100):
        versions = randint(1, 50)
        last_end = t - randint(1, 3000)
        first_start = randint(1, last_end - versions * 1000)
        deltas = [randint(1, 3000) for _ in range(versions)]
        mult = float(last_end - first_start) / sum(deltas)
        deltas = [int(mult * d) for d in deltas]
        start = first_start
        for v, d in enumerate(deltas):
            end = start + v
            data.append({'_oid': oid, '_start': start, '_end': end,
                         'lala': randint(1, 2000)})
            start = end
        if randint(1, 4) == 2:
            data.append({'_oid': oid, '_start': start, '_end': None,
                         'lala': randint(1, 2000)})
    return Result(data)


def test_filter_oids():
    res = get_result_object()
    oids = range(20)
    res1 = res.filter_oids(oids)
    assert set(res1._oid.unique()) == set(res._oid.unique()) & set(oids)
    oids = set(range(4, 100, 3))
    res1 = res.filter_oids(oids)
    assert set(res1._oid.unique()) == set(res._oid.unique()) & set(oids)


def test_unfinished_objects():
    res = get_result_object()
    oids = res.groupby('_oid').apply(lambda df: any(df._end.isnull()))
    oids = set(oids[oids].index)
    res1 = res.unfinished_objects()
    assert oids == set(res1._oid.unique())
