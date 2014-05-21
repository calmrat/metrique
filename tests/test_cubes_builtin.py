#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from __future__ import unicode_literals

import os
import shelve

from .utils import set_env, qremove

env = set_env()

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')
fixtures = os.path.join(testroot, 'fixtures')
etc = os.path.join(testroot, 'etc')
cache_dir = env['METRIQUE_CACHE']
log_dir = env['METRIQUE_LOGS']


def test_csvdata():
    '''

    '''
    from metrique import pyclient
    name = 'us_idx_eod'
    db_file = os.path.join(cache_dir, '%s.db' % name)
    qremove(db_file)
    m = pyclient(cube='csvdata_rows', name=name)

    uri = os.path.join(fixtures, 'us-idx-eod.csv')
    m.get_objects(uri=uri)

    assert m.objects
    assert len(m.objects) == 14
    assert m.objects.fields == ['__v__', '_e', '_end', '_hash', '_id',
                                '_oid', '_start', '_v', 'close', 'date',
                                'open', 'symbol']

    _ids = m.objects._ids
    _hash = 'bd31daf9425203d5f2574516e8ab212cf3405ce7'
    _filtered = m.objects.filter(where={'_hash': _hash})
    assert len(_filtered) == 1
    assert m.objects['11']['_hash'] == _hash  # check _hash is as expected
    assert m.objects['11']['symbol'] == '$AJT'
    assert m.objects.persist() == _ids
    # still there...
    assert m.objects['11']['symbol'] == '$AJT'

    qremove(db_file)

    # persist and remove from container
    assert m.objects.flush() == _ids
    assert m.objects == {}

    cube = shelve.open(db_file)
    # bssdb requires keys to be strings! no unicode
    assert cube[str('11')] == _filtered[0]


def test_load_json():
    '''

    '''
    from metrique import pyclient
    from metrique.utils import load

    name = 'meps'
    db_file = os.path.join(cache_dir, '%s.db' % name)
    qremove(db_file)

    def _oid_func(o):
        o['_oid'] = o['id']
        return o

    m = pyclient(name=name)
    path = os.path.join(fixtures, 'meps.json')
    objects = load(path, _oid=_oid_func, orient='index')

    assert len(objects) == 736

    m.objects.extend(objects)

    assert len(m.objects)

    _ids = m.objects.flush()

    assert sorted(_ids) == sorted(map(unicode, [o['_oid'] for o in objects]))
    assert m.objects == {}

    qremove(db_file)
