#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from copy import deepcopy
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


def test_api():
    from metrique import MetriqueContainer, MetriqueObject
    from metrique.utils import utcnow, remove_file, dt2ts, ts2dt

    _start = ts2dt('2001-01-01')
    _end = ts2dt('2001-01-02')
    a = {'_oid': 1, 'col_1': 1, 'col_2': utcnow(), '_start': _start}
    b = {'_oid': 2, 'col_1': 2, 'col_2': utcnow(), '_start': _start}
    ma = MetriqueObject(**a)
    mb = MetriqueObject(**b)
    objs_list = [a, b]
    objs_dict = {'1': a, '2': b}
    r_objs_dict = {'1': ma, '2': mb}

    c = MetriqueContainer()
    assert not c.name
    assert not c._proxy

    MetriqueContainer()

    # check various forms of passing in objects results in expected
    # container contents

    assert c == {}
    assert MetriqueContainer(objects=c) == {}
    assert MetriqueContainer(objects=objs_list) == r_objs_dict
    assert MetriqueContainer(objects=objs_dict) == r_objs_dict
    mc = MetriqueContainer(objects=objs_list)
    assert MetriqueContainer(objects=mc) == r_objs_dict

    # setting version should result in all objects added having that version
    # note: version -> _v in MetriqueObject
    assert mc.version == 0
    assert mc['1']['_v'] == 0
    mc = MetriqueContainer(objects=objs_list, version=3)
    assert mc.version == 3
    assert mc['1']['_v'] == 3

    # setting converts key to _id of value after being passed
    # through MetriqueObject(); notice key int(5) -> str('5')
    mc[5] = {'_oid': 5}
    assert mc['5']['_oid'] == 5

    # should have 3 objects, first two, plus the last one
    assert len(mc) == 3
    assert len(mc.values()) == 3
    assert sorted(mc._ids) == ['1', '2', '5']

    assert sorted(mc._oids) == [1, 2, 5]
    try:
        mc.ls()
    except NotImplementedError:
        pass

    mc.extend([{'_oid': 6}, {'_oid': 7}])
    assert sorted(mc._oids) == [1, 2, 5, 6, 7]

    mc.add({'_oid': 8, '_start': _start, '_end': _end, 'col_1': True})
    mc.add({'_oid': 8, '_end': None, 'col_1': False})
    assert sorted(mc._oids) == [1, 2, 5, 6, 7, 8]

    r = mc.filter(where={'_oid': 8})
    assert len(r) == 2
    assert sorted(mc._oids) == [1, 2, 5, 6, 7, 8]

    assert sorted(mc._oids) == [1, 2, 5, 6, 7, 8]
    mc.pop('7')
    assert sorted(mc._oids) == [1, 2, 5, 6, 8]
    mc.pop(6)
    assert sorted(mc._oids) == [1, 2, 5, 8]
    del mc[5]
    assert sorted(mc._oids) == [1, 2, 8]

    assert '1' in mc

    mc.clear()
    assert mc == {}

    db = 'admin'
    name = 'container_test'
    c = MetriqueContainer(name=name, db=db)

    _expected_db_path = os.path.join(cache_dir, 'admin.sqlite')
    # test drop
    c.drop(True)
    assert c._persist_path == _expected_db_path
    # make sure we're working with a clean db
    remove_file(_expected_db_path)

    mc = MetriqueContainer(name=name, db=db, objects=objs_list)
    assert mc.df() is not None
    assert mc.df().empty is False

    # local persistence; filter method queries .objects buffer
    # .persist dumps data to proxy db; but leaves the data in the buffer
    # .flush dumps data and removes all objects dumped
    # count queries proxy db
    mc = MetriqueContainer(name=name, db=db, objects=objs_list)
    _store = deepcopy(mc.store)

    assert len(mc.filter({'col_1': 1})) == 1
    _ids = mc.persist()
    assert _ids == ['1', '2']
    assert mc.store == _store
    assert len(mc.filter({'col_1': 1})) == 1
    assert mc.count('col_1 == 1') == 1
    assert mc.count() == 2

    # persisting again shouldn't result in new rows
    _ids = mc.persist()
    assert _ids == ['1', '2']
    assert mc.store == _store
    assert len(mc.filter({'col_1': 1})) == 1
    assert mc.count('col_1 == 1') == 1
    assert mc.count() == 2

    # flushing now shouldn't result in new rows; but store should be empty
    _ids = mc.flush()
    assert _ids == ['1', '2']
    assert mc.store == {}
    assert len(mc.filter({'col_1': 1})) == 0
    assert mc.count('col_1 == 1') == 1
    assert mc.count() == 2

    # adding the same object shouldn't result in new rows
    a.update({'col_1': 42})
    mc.add(a)
    assert len(mc.filter({'col_1': 1})) == 0
    assert len(mc.filter({'col_1': 42})) == 1
    _ids = mc.flush()
    assert mc.count(date='~') == 3
    assert mc.count(date=None) == 2
    assert mc.count('col_1 == 1', date=None) == 0
    assert mc.count('col_1 == 1', date='~') == 1
    assert mc.count('col_1 == 42') == 1
    assert mc.count('col_1 == 42', date='~') == 1
    # adjust for local time...
    #_ts = dt2ts(convert(_start))
    _ts = dt2ts(_start)
    assert _ids == ['1', '1:%s' % _ts]

    # remove the db
    remove_file(_expected_db_path)
