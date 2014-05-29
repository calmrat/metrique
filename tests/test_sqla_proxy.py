#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


from datetime import datetime
import os
from sqlalchemy.exc import OperationalError, ProgrammingError

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
    from metrique import SQLAlchemyProxy
    try:
        # test that db kwarg required
        SQLAlchemyProxy()
    except TypeError:
        pass


def db_tester(proxy):
    from metrique.utils import utcnow

    now = utcnow()
    TABLE = 'bla'
    p = proxy

    # Clear out ALL tables in the database!
    p.drop_tables(True)

    assert p.ls() == []

    obj = {'col_1': 1, 'col_3': now}

    schema = {
        'col_1': {'type': int},
        'col_3': {'type': datetime},
    }

    autoschema = p.autoschema(obj)
    assert dict(autoschema) == dict(schema)

    table = p.ensure_table(name=TABLE, schema=schema)
    assert table is not None

    assert p.count(TABLE) == 0

    expected_fields = ['__v__', '_e', '_end', '_hash', '_id',
                       '_oid', '_start', '_v', 'id']

    _exp = expected_fields + obj.keys()
    assert sorted(p.columns(TABLE)) == sorted(_exp)

    try:
        # we don't have _oids set in each object
        p.insert(TABLE, obj)
    except TypeError:
        pass

    obj.update({'_oid': 1})
    p.insert(TABLE, obj)

    assert p.count(TABLE) == 1
    assert p.find(TABLE, '_oid == 1', raw=True, date=None)

    obj.update({'_oid': 2, '_start': now, '_end': utcnow()})
    p.insert(TABLE, obj)
    assert p.count(TABLE, '_oid == 2', date='%s~' % now) == 1
    assert p.count(TABLE, '_oid == 2', date='~%s' % now) == 0

    assert p.distinct(TABLE, '_oid') == [1, 2]

    # insert new obj, then update col_3's values
    obj = {'_oid': 3, '_start': utcnow(), '_end': None, 'col_1': 1}
    p.insert(TABLE, obj)
    assert p.count(TABLE, '_oid == 3', date='~') == 1
    obj['col_1'] = 42
    p.upsert(TABLE, obj)
    # should be two versions of _oid:3
    assert p.count(TABLE, '_oid == 3', date='~') == 2
    assert p.count(TABLE, 'col_1 == 1', date=None) == 1
    assert p.count(TABLE, 'col_1 == 42', date='~') == 1

    # should be four object versions in total at this point
    assert p.count(TABLE, date='~') == 4

    # last _oid should be 3
    assert p.get_last_field(TABLE, '_oid') == 3
    obj.update({'_oid': 0})
    p.insert(TABLE, obj)
    assert p.get_last_field(TABLE, '_oid') == 3
    obj.update({'_oid': 42})
    p.insert(TABLE, obj)
    assert p.get_last_field(TABLE, '_oid') == 42

    assert p.ls() == ['bla']

    p.drop(TABLE)
    try:
        assert p.count(TABLE) == 0
    except (OperationalError, ProgrammingError):
        pass

    assert p.ls() == []


def test_sqlite3():
    from metrique.utils import remove_file
    from metrique import SQLAlchemyProxy

    _expected_db_path = os.path.join(cache_dir, 'test.sqlite')
    remove_file(_expected_db_path)

    DB = 'test'
    p = SQLAlchemyProxy(db=DB)
    assert p.get_engine_uri() == 'sqlite:///%s' % _expected_db_path

    db_tester(p)

    remove_file(_expected_db_path)


# test container type!
#schema.update({'col_2': {'type': unicode, 'container': True}})

def test_postgresql():
    from metrique import SQLAlchemyProxy

    DB = 'test'
    p = SQLAlchemyProxy(dialect='postgresql', db=DB)
    _u = p.config.get('username')
    _p = p.config.get('password')
    _po = p.config.get('port')
    _expected_engine = 'postgresql://%s:%s@127.0.0.1:%s/%s' % (_u, _p, _po, DB)
    assert p.get_engine_uri() == _expected_engine

    db_tester(p)
