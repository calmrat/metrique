#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
import pytest
from time import time

from utils import set_env

env = set_env()
exists = os.path.exists

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')
fixtures = os.path.join(testroot, 'fixtures')
cache_dir = env['METRIQUE_CACHE']


def test_api():
    from metrique import MetriqueObject
    from metrique.utils import utcnow, dt2ts
    from metrique._version import __version__

    now = utcnow()
    a = {'col_1': 1, 'col_2': now}

    # _oid expected to be defined
    try:
        MetriqueObject()
    except TypeError:
        pass
    try:
        MetriqueObject(**a)
    except TypeError:
        pass

    # _oid can't be null either
    a['_oid'] = None
    try:
        MetriqueObject(**a)
    except RuntimeError:
        pass

    a['_oid'] = 1
    o = MetriqueObject(**a)
    assert o
    assert o['_start'] < utcnow()

    # all objects get the metrique version used to
    # build them applied
    assert o['__v__'] == __version__

    # keys are all changed to lowercase
    assert o['_START'] < utcnow()

    expected_keys = sorted(
        ['_hash', '_v', '__v__', '_e', '_oid', '_id',
         '_start', '_end', 'col_1', 'col_2'])

    assert sorted(o.keys()) == expected_keys

    # hash should be constant if values don't change
    _hash = o['_hash']
    assert _hash == MetriqueObject(**a).get('_hash')

    a['col_1'] = 2
    assert _hash != MetriqueObject(**a).get('_hash')
    a['col_1'] = 3
    assert _hash != MetriqueObject(**a).get('_hash')

    # _start should not get updated if passed in
    a['_start'] = now
    assert MetriqueObject(**a).get('_start') == now

    # _id should be ignored if passed in; a unique _id will be generated
    # based on obj content (in this case, string of _oid
    a['_id'] = 'blabla'
    assert MetriqueObject(**a).get('_id') != 'blabla'
    assert MetriqueObject(**a).get('_id') == '1'

    a['_end'] = now
    o = MetriqueObject(**a)
    assert o['_start'] == o['_end']

    # _end must come on/after _start
    try:
        a['_end'] = now
        a['_start'] = utcnow()
        o = MetriqueObject(**a)
    except ValueError:
        pass

    # _start, if null, will be set to utcnow()
    a['_start'] = None
    a['_end'] = None
    assert MetriqueObject(**a).get('_start') is not None

    # setting items will update the _hash
    # setting _end will update _id
    o = MetriqueObject(**a)
    _hash = o['_hash']
    _id = o['_id']
    o['col_1'] = 'yipee'
    new_hash = o['_hash']
    assert new_hash != _hash
    assert o['_id'] == _id
    _now = utcnow()
    o['_end'] = _now
    assert o['_id'] != _id
    assert o['_id'] == '%s:%s' % (o['_oid'], dt2ts(o['_start']))
    o['_end'] = None
    assert o['_id'] == _id

    # popping objects updates _hash too
    assert o['_hash'] == new_hash
    assert o.pop('col_1') == 'yipee'
    assert o['_hash'] != new_hash

    o['_end'] = utcnow()
    o.pop('_end')
    assert o

    # keys are normalized too
    o['ONE   -;;/two'] = 1
    assert o['one_two']

    assert 'col_3' not in o
    _hash = o['_hash']
    o.update(dict(col_3=5))
    assert o['_hash'] != _hash
    assert o['col_3']

    # make dates (_start/_end) as epoch
    o = MetriqueObject(_as_datetime=False, **a)
    assert isinstance(o['_start'], float)

    # object version can be set at the module level
    # which then gets applied to all future objects
    # created, which does affect the _hash result
    o = MetriqueObject(**a)
    assert o._VERSION == 0
    _hash = o['_hash']
    MetriqueObject._VERSION = 1
    o = MetriqueObject(**a)
    assert o._VERSION == 1
    assert _hash != o['_hash']
    # put things back how we found them...
    MetriqueObject._VERSION = 0


@pytest.mark.perf
def test_performance():
    from metrique import MetriqueObject
    _o = dict(_oid=1, COL_1=1, col_2=2, _id='bla')
    expected = 0.001
    s = time()
    MetriqueObject(**_o)
    e = time()
    diff = e - s
    print 'Creating 1 objects vs expected: %s:%s' % (diff, expected)
    assert diff <= expected

    k = 10000
    expected = 4.0
    s = time()
    [MetriqueObject(**_o) for i in xrange(1, k)]
    e = time()
    diff = e - s
    print 'Creating %s objects vs expected: %s:%s' % (k, diff, expected)
    psec = diff / k
    print '... %s per second' % psec
    assert diff <= 4.0
