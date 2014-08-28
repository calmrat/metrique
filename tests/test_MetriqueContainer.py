#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from __future__ import unicode_literals, absolute_import

from copy import deepcopy
import os

from .utils import set_env
from metrique.utils import debug_setup

logger = debug_setup('metrique', level=10, log2stdout=True, log2file=False)

env = set_env()
exists = os.path.exists

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')
fixtures = os.path.join(testroot, 'fixtures')
cache_dir = env['METRIQUE_CACHE']


def test_datatypes():
    from metrique import MetriqueContainer
    from metrique.utils import utcnow, remove_file

    o = {"_oid": 1,
         "date": utcnow(),
         "dict_null": {},
         "dict": {'hello': 'world'},
         "bool": True,
         "null": None,
         "list_null": [],
         "list": [1, 2, 3]}
    db = 'admin'
    table = 'test'
    c = MetriqueContainer(name=table, db=db)

    c.drop()
    remove_file(c._proxy._sqlite_path)

    c.add(o)
    c.upsert()

    c.drop()
    remove_file(c._proxy._sqlite_path)


def test_api():
    from metrique import MetriqueContainer, metrique_object
    from metrique.utils import utcnow, remove_file, ts2dt

    _start = ts2dt('2001-01-01')
    _end = ts2dt('2001-01-02')
    a = {'_oid': 1, 'col_1': 1, 'col_2': utcnow(), '_start': _start}
    b = {'_oid': 2, 'col_1': 2, 'col_2': utcnow(), '_start': _start}
    ma = metrique_object(**a)
    mb = metrique_object(**b)
    objs_list = [a, b]
    r_objs_list = [ma, mb]

    c = MetriqueContainer()
    assert not c.name
    assert not c._proxy

    MetriqueContainer()

    # check various forms of passing in objects results in expected
    # container contents

    assert c.objects() == []
    assert MetriqueContainer(objects=c).objects() == []
    assert MetriqueContainer(objects=objs_list).objects() == r_objs_list
    mc = MetriqueContainer(objects=objs_list)
    assert MetriqueContainer(objects=mc).objects() == r_objs_list

    mc.add({'_oid': 5})

    # should have 3 objects, first two, plus the last one
    assert len(mc) == 3
    assert len(mc.values()) == 3

    assert sorted(mc._oids) == [1, 2, 5]
    try:
        mc.ls()
    except NotImplementedError:
        pass
    else:
        assert False

    mc.extend([{'_oid': 6}, {'_oid': 7}])
    assert sorted(mc._oids) == [1, 2, 5, 6, 7]

    mc.add({'_oid': 8, '_start': _start, '_end': _end, 'col_1': True})
    mc.add({'_oid': 8, '_end': None, 'col_1': False})
    assert sorted(mc._oids) == [1, 2, 5, 6, 7, 8]

    r = [o for o in mc.objects() if o['_oid'] == 8]
    assert len(r) == 2
    assert sorted(mc._oids) == [1, 2, 5, 6, 7, 8]

    mc.clear()
    assert mc.objects() == []

    db = 'admin'
    name = 'container_test'
    c = MetriqueContainer(name=name, db=db)

    _expected_db_path = os.path.join(cache_dir, 'admin.sqlite')
    # test drop
    c.drop(True)
    assert c.proxy._sqlite_path == _expected_db_path
    # make sure we're working with a clean db
    remove_file(_expected_db_path)

    mc = MetriqueContainer(name=name, db=db, objects=objs_list)
    assert mc.df() is not None
    assert mc.df().empty is False

    # local persistence; filter method queries .objects buffer
    # .upsert dumps data to proxy db; but leaves the data in the buffer
    # .flush dumps data and removes all objects dumped
    # count queries proxy db
    mc = MetriqueContainer(name=name, db=db, objects=objs_list)
    _store = deepcopy(mc.store)

    assert len([o for o in mc.objects() if o['col_1'] == 1]) == 1
    mc.upsert()
    assert mc.store == _store
    assert len([o for o in mc.objects() if o['col_1'] == 1]) == 1
    assert mc.count('col_1 == 1') == 1
    assert mc.count() == 2

    # persisting again shouldn't result in new rows
    mc.upsert()
    assert mc.store == _store
    assert len([o for o in mc.objects() if o['col_1'] == 1]) == 1
    assert mc.count('col_1 == 1') == 1
    assert mc.count() == 2

    # flushing now shouldn't result in new rows; but store should be empty
    mc.flush()
    assert mc.store == []
    assert len([o for o in mc.objects() if o['col_1'] == 1]) == 0
    assert mc.count('col_1 == 1') == 1
    assert mc.count() == 2

    # adding the same object shouldn't result in new rows
    a.update({'col_1': 42})
    mc.add(a)
    assert len([o for o in mc.objects() if o['col_1'] == 1]) == 0
    assert len([o for o in mc.objects() if o['col_1'] == 42]) == 1
    mc.flush()
    assert mc.count(date='~') == 3
    assert mc.count(date=None) == 2
    assert mc.count('col_1 == 1', date=None) == 0
    assert mc.count('col_1 == 1', date='~') == 1
    assert mc.count('col_1 == 42') == 1
    assert mc.count('col_1 == 42', date='~') == 1

    # remove the db
    remove_file(_expected_db_path)
