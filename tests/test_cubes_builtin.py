#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from __future__ import unicode_literals

import os

from utils import set_env
from metrique.utils import remove_file, debug_setup

logger = debug_setup('metrique', level=10, log2stdout=True, log2file=False)

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
    db_file = os.path.join(cache_dir, '%s.sqlite' % name)
    remove_file(db_file)
    m = pyclient(cube='csvdata_rows', name=name)

    m.objects.drop()

    uri = os.path.join(fixtures, 'us-idx-eod.csv')
    m.get_objects(uri=uri)

    assert m.objects
    assert len(m.objects) == 14
    assert m.objects.fields == ['__v__', '_e', '_end', '_hash', '_id',
                                '_oid', '_start', '_v', 'close', 'date',
                                'open', 'symbol']

    # {u'symbol': u'$AJT', u'date': u'09/08/2008', u'close': 18.15, u'_start':
    # datetime.datetime(2014, 5, 28, 14, 9, 22, 999145), u'open': 17.84,
    # u'_oid': 11, u'_v': 0, u'_end': None, u'_hash':
    # u'5a6d18a9c654886926e5f769d4bf4808df6cba39', u'__v__': u'0.3.1-1a',
    # u'_e': {}, u'_id': u'11'}
    _ids = m.objects._ids
    _hash = '5a388d116969aafcf6e9a74f0298a4728a510705'
    _oid = 11
    _filtered = m.objects.filter(where={'_oid': _oid})
    print 'Object: %s' % _filtered
    assert len(_filtered) == 1
    assert m.objects['11']['_hash'] == _hash  # check _hash is as expected
    assert m.objects['11']['symbol'] == '$AJT'
    assert m.objects.persist() == _ids
    # still there...
    assert m.objects['11']['symbol'] == '$AJT'

    # persist and remove from container
    assert m.objects.flush() == _ids
    assert m.objects == {}

    objs = m.objects.find('_oid == %s' % _oid, one=True, raw=True)
    o = {k: v for k, v in objs.items() if k != 'id'}
    _o = dict(_filtered[0])
    # we can't assure float precision is exact as it goes in/out
    # but it should be close...
    assert o['_start'] - _o['_start'] <= .1
    # FIXME: ideally, _e would come back out as it went in!
    # not going in as {} but come out as None
    for k in ['_start', '_e']:
        del o[k]
        del _o[k]
    assert o == _o

    remove_file(db_file)


def test_load_json():
    '''

    '''
    from metrique import pyclient
    from metrique.utils import load

    name = 'meps'
    db_file = os.path.join(cache_dir, '%s.sqlite' % name)
    remove_file(db_file)

    def _oid_func(o):
        o['_oid'] = o['id']
        return o

    m = pyclient(name=name)
    path = os.path.join(fixtures, 'meps.json')
    objects = load(path, _oid=_oid_func, orient='index')

    assert len(objects) == 736

    m.objects.extend(objects)

    assert len(m.objects)

    # {u'phone_stb': u'+33 (0)3 88 1 75224', u'comms': None, u'country':
    # u'Latvia', u'_start': datetime.datetime(2014, 5, 29, 15, 0, 33, 935124),
    # u'_oid': 28615, u'name': u'Roberts Z\u012aLE', u'url':
    # u'http://www.europarl.euro...rs/expert/committees/view.do?id=28615',
    # u'_v': 0, u'phone_bxl': u'+32 (0)2 28 45224', u'_end': None, u'_hash':
    # u'823c4c5b9f7f7750f3d20a247d2d23540e7936b3', u'__v__': u'0.3.1-1a',
    # u'party': u'European Conservatives and Reformists', u'_e': {}, u'_id':
    # u'28615', u'email': None}
    _hash = '2908df4739650439444dfc19c0b0e023739c1ebd'
    _filtered = m.objects.filter(where={'_oid': 28615})
    assert len(_filtered) == 1
    print 'Object: %s' % _filtered
    assert _filtered[0]['_hash'] == _hash

    _ids = m.objects.flush()

    assert sorted(_ids) == sorted(map(unicode, [o['_oid'] for o in objects]))
    assert m.objects == {}

    remove_file(db_file)


def test_gitdata_commit():
    from metrique import pyclient
    from metrique.utils import remove_file

    name = 'gitdata_commit'
    db_file = os.path.join(cache_dir, '%s.sqlite' % name)
    remove_file(db_file)

    uri_1 = 'https://github.com/kejbaly2/tornadohttp.git'
    uri_2 = 'https://github.com/kejbaly2/metrique.git'
    m = pyclient(cube=name)

    m.get_objects(uri=uri_1)
    k = len(m.objects)
    assert k > 0
    m.get_objects(uri=uri_1, pull=True)
    assert k == len(m.objects)

    # {u'files': {u'setup.py': {u'removed': 0, u'added': 3},
    # u'tornadohttp/tornadohttp.py': {u'removed': 7, u'added': 10},
    # u'tornadohttp/__init__.py': {u'removed': 0, u'added': 7},
    # u'tornadohttp/_version.py': {u'removed': 0, u'added': 9}}, u'committer':
    # u'Chris Ward <cward@redhat.com>', u'added': 29, u'extra': None,
    # u'author_time': 1396355424, u'related': None, u'repo_uri':
    # u'https://github.com/kejbaly2/tornadohttp.git', u'acked_by': None,
    # u'resolves': None, u'message': u'version bump; logdir and other configs
    # renamed\n', u'_start': datetime.datetime(2014, 4, 1, 12, 30, 24),
    # u'_oid': u'99dc1e5c4e3ab2c8ab5510e50a3edf64f9fcc705', u'removed': 7,
    # u'mergetag': None, u'author': u'Chris Ward <cward@redhat.com>', u'_v': 0,
    # u'tree': u'66406ded27ba129ad1639928b079b821ab416fed', u'_end': None,
    # u'signed_off_by': None, u'parents':
    # ['78b311d90e35eb36016a7f41e75657754dbe0784'], u'_hash':
    # u'2b408e995e0fed13833c36a61efd3622bad05ddc', u'__v__': u'0.3.1-1a',
    # u'_e': {}, u'_id': u'99dc1e5c4e3ab2c8ab5510e50a3edf64f9fcc705'}
    _hash = '2b408e995e0fed13833c36a61efd3622bad05ddc'
    _oid = '99dc1e5c4e3ab2c8ab5510e50a3edf64f9fcc705'
    _filtered = m.objects.filter(where={'_oid': _oid})
    assert len(_filtered) == 1
    print 'Object: %s' % _filtered
    assert _filtered[0]['_hash'] == _hash

    _ids = m.objects.flush()
    assert len(_ids) == k

    # load a second repo
    # make sure our sessions are working as expected and
    # a second call works as expected; eg, in the past
    # there was a bug where we didn't load the table into
    # metadata if the table wasn't being created for the
    # first time and so non-standard types weren't
    # defined in the session...
    m.get_objects(uri=uri_2, flush=True)

    remove_file(m.repo.path, force=True)
    remove_file(db_file)


def test_osinfo_rpm():
    from metrique import pyclient
    from metrique.utils import sys_call

    if sys_call('which rpm', ignore_errors=True) is None:
        # skip these tests, since we don't have rpm installed
        return

    name = 'osinfo_rpm'
    db_file = os.path.join(cache_dir, '%s.sqlite' % name)
    remove_file(db_file)
    m = pyclient(cube=name)

    m.get_objects()
    k = len(m.objects)
    assert k > 0

    name = 'bash'
    _filtered = m.objects.filter(where={'name': name})
    assert len(_filtered) == 1
    print 'Object: %s' % _filtered

    _ids = m.objects.flush()
    assert len(_ids) == k

    remove_file(db_file)
