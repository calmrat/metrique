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


def db_tester(proxy):
    from metrique.utils import utcnow, ts2dt

    _start = ts2dt("2001-01-01 00:00:00")
    _end = ts2dt("2001-01-02 00:00:00")
    _before = ts2dt("2000-12-31 00:00:00")
    _after = ts2dt("2001-01-03 00:00:00")
    CUBE = 'bla'
    p = proxy

    # Clear out ALL tables in the database!
    p.drop_tables(True)

    assert p.ls() == []

    obj = {'col_1': 1, 'col_3': utcnow()}

    schema = {
        'col_1': {'type': int},
        'col_3': {'type': datetime},
    }

    autoschema = p.autoschema(obj)
    assert dict(autoschema) == dict(schema)

    table = p.ensure_table(name=CUBE, schema=schema)
    assert table is not None

    assert p.count(CUBE) == 0

    expected_fields = ['__v__', '_e', '_end', '_hash', '_id',
                       '_oid', '_start', '_v', 'id']

    _exp = expected_fields + obj.keys()
    assert sorted(p.columns(CUBE)) == sorted(_exp)

    try:
        # we don't have _oids set in each object
        p.insert(CUBE, obj)
    except TypeError:
        pass

    obj.update({'_oid': 1})
    print 'Inserting %s' % obj
    p.insert(CUBE, obj)

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
    ix = [i['name'] for i in p.index_list().get(CUBE)]
    assert 'ix_col_1' not in ix
    p.index(CUBE, 'col_1')
    ix = [i['name'] for i in p.index_list().get(CUBE)]
    assert 'ix_col_1' in ix


def test_sqlite3():
    from metrique.utils import remove_file
    from metrique import SQLAlchemyProxy

    _expected_db_path = os.path.join(cache_dir, 'test.sqlite')
    remove_file(_expected_db_path)

    DB = 'test'
    p = SQLAlchemyProxy(db=DB)
    assert p.get_engine_uri() == 'sqlite:///%s' % _expected_db_path

    db_tester(p)

    CUBE = 'bla'

    p.drop(CUBE)
    try:
        assert p.count(CUBE) == 0
    except (OperationalError, ProgrammingError):
        pass

    assert p.ls() == []

    remove_file(_expected_db_path)


# test container type!
#schema.update({'col_2': {'type': unicode, 'container': True}})

def test_postgresql():
    from metrique import SQLAlchemyProxy
    from metrique.utils import rand_chars

    DB = 'admin'
    p = SQLAlchemyProxy(dialect='postgresql', db=DB)
    _u = p.config.get('username')
    _p = p.config.get('password')
    _po = p.config.get('port')
    _expected_engine = 'postgresql://%s:%s@127.0.0.1:%s/%s' % (_u, _p, _po, DB)
    assert p.get_engine_uri() == _expected_engine

    # FIXME: DROP ALL

    db_tester(p)

    CUBE = 'bla'
    schema = {
        'col_1': {'type': int},
        'col_3': {'type': datetime},
    }

    # FIXME: remove user! before test completes

    # new user
    new_u = rand_chars(chars='asdfghjkl')
    new_p = rand_chars(8)
    p.user_register(username=new_u, password=new_p)

    # Sharing
    p.share(CUBE, new_u)

    # switch to the new users db
    p.config['username'] = new_u
    p.config['password'] = new_p
    p.config['db'] = new_u
    p.initialize()
    assert p.ls() == []

    NEW_CUBE = 'blabla'
    p.ensure_table(name=NEW_CUBE, schema=schema)
    assert p.ls() == [NEW_CUBE]

    # switch to admin's db
    p.config['username'] = _u
    p.config['password'] = _p
    p.config['db'] = _u
    p.initialize()
    assert p.ls() == [CUBE]

    p.drop(CUBE)
    try:
        assert p.count(CUBE) == 0
    except (OperationalError, ProgrammingError):
        pass

    assert p.ls() == []
