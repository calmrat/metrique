#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


import os

from utils import set_env

from metrique.utils import debug_setup

logger = debug_setup('metrique', level=10, log2stdout=True, log2file=False)

env = set_env()
exists = os.path.exists

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')
fixtures = os.path.join(testroot, 'fixtures')
cache_dir = env['METRIQUE_CACHE']


def test_generic():
    from metrique import MongoDBProxy
    try:
        # test that db kwarg required
        MongoDBProxy()
    except TypeError:
        pass


def test_mongodb():
    from metrique import MongoDBProxy
    from metrique.utils import utcnow, ts2dt

    _start = ts2dt("2001-01-01")
    _end = ts2dt("2001-01-02")
    _before = ts2dt("2000-12-31")
    _after = ts2dt("2001-01-03")
    CUBE = 'bla'

    p = MongoDBProxy(CUBE)
    assert p.proxy.alive()

    p.drop_db()

    assert sorted(p.proxy.database_names()) == ['local']

    assert p.proxy.tz_aware == p.config.get('tz_aware')
    # as we initiate with a db, it should already be created
    assert sorted(p.proxy.database_names()) == ['local']

    # Clear out ALL tables in the database!
    p.drop_tables(True)

    bla_cubes = sorted(p.proxy[CUBE].collection_names())
    assert bla_cubes == []
    assert p.ls() == []

    obj = {'col_1': 1, 'col_3': utcnow()}

    table = p.ensure_table(name=CUBE)
    assert table is not None

    assert p.count(CUBE) == 0

    expected_fields = ['__v__', '_e', '_end', '_hash', '_id',
                       '_oid', '_start', '_v']

    _exp = expected_fields + obj.keys()
    # no docs inserted yet, so we have no columns available
    assert p.columns(CUBE) == []

    try:
        # we don't have _oids set in each object
        p.insert(CUBE, obj)
    except TypeError:
        pass

    obj.update({'_oid': 1})
    print 'Inserting %s' % obj
    p.insert(CUBE, obj)

    # we have a document now, so columns should be populated
    assert p.columns(CUBE) == sorted(_exp)

    assert p.count(CUBE) == 1
    assert p.find(CUBE, '_oid == 1', raw=True, date=None)
    # should be one object with col_1 == 1 (_oids: 1, 2)
    assert p.count(CUBE, 'col_1 == 1', date='~') == 1

    obj.update({'_oid': 2, '_start': _start, '_end': _end})
    print 'Inserting %s' % obj
    p.insert(CUBE, obj)
    assert p.count(CUBE, '_oid == 2', date=None) == 0
    assert p.count(CUBE, '_oid == 2', date='%s~' % _start) == 1
    assert p.count(CUBE, '_oid == 2', date='~%s' % _start) == 1
    assert p.count(CUBE, '_oid == 2', date='~') == 1
    assert p.count(CUBE, '_oid == 2', date='~%s' % _before) == 0
    assert p.count(CUBE, '_oid == 2', date='%s~' % _after) == 0
    # should be two objects with col_1 == 1 (_oids: 1, 2)
    assert p.count(CUBE, 'col_1 == 1', date='~') == 2

    assert p.distinct(CUBE, '_oid') == [1, 2]

    # insert new obj, then update col_3's values
    obj = {'_oid': 3, '_start': utcnow(), '_end': None, 'col_1': 1}
    print 'Inserting %s' % obj
    p.insert(CUBE, obj)
    assert p.count(CUBE, '_oid == 3', date='~') == 1

    obj['col_1'] = 42
    p.upsert(CUBE, obj)
    # should be two versions of _oid:3
    assert p.count(CUBE, '_oid == 3', date='~') == 2
    # should be three objects with col_1 == 1 (_oids: 1, 2, 3)
    assert p.count(CUBE, 'col_1 == 1', date='~') == 3
    assert p.count(CUBE, 'col_1 == 42', date='~') == 1

    # should be four object versions in total at this point
    assert p.count(CUBE, date='~') == 4

    # last _oid should be 3
    assert p.get_last_field(CUBE, '_oid') == 3
    obj.update({'_oid': 0})
    p.insert(CUBE, obj)
    assert p.get_last_field(CUBE, '_oid') == 3
    obj.update({'_oid': 42})
    p.insert(CUBE, obj)
    assert p.get_last_field(CUBE, '_oid') == 42

    assert p.ls() == [CUBE]

    # Indexes
    ix = p.index_list(CUBE).keys()
    assert 'col_1_1' not in ix
    p.index(CUBE, 'col_1')
    print p.index_list(CUBE)
    ix = p.index_list(CUBE).keys()
    assert 'col_1_1' in ix
