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
    from metrique.mongodb import MongoDBProxy
    try:
        # test that db kwarg required
        MongoDBProxy()
    except TypeError:
        pass


def test_mongodb():
    from metrique.mongodb import MongoDBProxy
    from metrique import MetriqueObject
    from metrique.utils import ts2dt

    O = MetriqueObject

    _start = ts2dt("2001-01-01")
    _end = ts2dt("2001-01-02")
    _before = ts2dt("2000-12-31")
    _after = ts2dt("2001-01-03")
    _date = ts2dt("2014-01-01 00:00:00")
    DB = 'test'
    TABLE = 'bla'

    p = MongoDBProxy(db=DB, table=TABLE)
    assert p.proxy.alive()

    # drop all dbs
    [p.drop_db(_) for _ in p.proxy.database_names() if _ not in ['admin',
                                                                 'local']]

    dbs = sorted(p.proxy.database_names())
    try:
        assert dbs == ['local']
    except:
        # travis environment has admin db present
        assert dbs == ['admin', 'local']

    assert p.proxy.tz_aware == p.config.get('tz_aware')

    # Clear out ALL tables in the database!
    p.drop(True)

    bla_cubes = sorted(p.proxy[TABLE].collection_names())
    assert bla_cubes == []
    assert p.ls() == []

    table = p.autotable(name=TABLE)
    assert table is not None

    assert p.count() == 0

    expected_fields = ['__v__', '_e', '_end', '_hash', '_id',
                       '_start', '_v']

    _obj_1 = {'_oid': 1, 'col_1': 1, 'col_3': _date}
    obj_1 = O(**_obj_1)

    _exp = expected_fields + _obj_1.keys()
    # no docs inserted yet, so we have no columns available
    assert p.columns() == []

    # should not be possible to update _oid
    assert obj_1['_oid'] == 1
    obj_1.update({'_oid': 42})
    assert obj_1['_oid'] == 1

    print 'Inserting %s' % obj_1
    p.insert([obj_1])

    # we have a document now, so columns should be populated
    assert p.columns() == sorted(_exp)

    assert p.count() == 1
    assert p.find('_oid == 1', raw=True, date=None)
    # should be one object with col_1 == 1 (_oids: 1, 2)
    assert p.count('col_1 == 1', date='~') == 1

    _obj_2 = {'_oid': 2, 'col_1': 1, '_start': _start, '_end': _end}
    obj_2 = O(**_obj_2)
    print 'Inserting %s' % obj_2
    p.insert([obj_2])
    assert p.count('_oid == 2', date=None) == 0
    assert p.count('_oid == 2', date='%s~' % _start) == 1
    # update to and including date...
    assert p.count('_oid == 2', date='~%s' % _start) == 1
    assert p.count('_oid == 2', date='~') == 1
    assert p.count('_oid == 2', date='~%s' % _before) == 0
    assert p.count('_oid == 2', date='%s~' % _after) == 0
    # should be two objects with col_1 == 1 (_oids: 1, 2)
    assert p.count('col_1 == 1', date='~') == 2

    assert p.distinct('_oid') == [1, 2]

    # insert new obj, then update col_3's values
    _obj_3 = {'_oid': 3, 'col_1': 1, '_start': _date, '_end': None, 'col_1': 1}
    obj_3 = O(**_obj_3)
    print 'Inserting %s' % obj_3
    p.insert([obj_3])
    assert p.count('_oid == 3', date='~') == 1

    obj_3['col_1'] = 42
    p.upsert([obj_3])
    # should be two versions of _oid:3
    assert p.count('_oid == 3', date='~') == 2
    # should be three objects with col_1 == 1 (_oids: 1, 2, 3)
    assert p.count('col_1 == 1', date='~') == 3
    assert p.count('col_1 == 42', date='~') == 1

    # should be four object versions in total at this point
    assert p.count(date='~') == 4

    # last _oid should be 3
    assert p.get_last_field('_oid') == 3
    _obj_4 = {'_oid': -1}
    obj_4 = O(**_obj_4)
    p.insert([obj_4])
    # should still have 3 as highest _oid
    assert p.get_last_field('_oid') == 3

    _obj_5 = {'_oid': 42}
    obj_5 = O(**_obj_5)
    p.insert([obj_5])
    # now, 42 should be highest
    assert p.get_last_field('_oid') == 42

    assert p.ls() == [TABLE]

    # Indexes
    ix = p.index_list().keys()
    assert 'col_1_1' not in ix
    p.index('col_1')
    print p.index_list()
    ix = p.index_list().keys()
    assert 'col_1_1' in ix
