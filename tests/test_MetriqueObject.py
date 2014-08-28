#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os

from utils import set_env

env = set_env()
exists = os.path.exists

testroot = os.path.dirname(os.path.abspath(__file__))
cubes = os.path.join(testroot, 'cubes')
fixtures = os.path.join(testroot, 'fixtures')
cache_dir = env['METRIQUE_CACHE']

# FIXME: test passing object with 'id' key produces a warning
# see https://docs.python.org/2/library/warnings.html


def test_func():
    from metrique.core_api import metrique_object
    from metrique.utils import utcnow
    from metrique._version import __version__

    now = utcnow()
    a = {'col_1': 1, 'col_2': now}

    # _oid must be passed in (as arg or kwarg, doesn't matter)
    try:
        metrique_object()
    except TypeError:
        pass
    else:
        assert False

    # same here; _oid still not being passed in
    try:
        metrique_object(**a)
    except TypeError:
        pass
    else:
        assert False

    # _oid can't be null either
    a['_oid'] = None
    try:
        metrique_object(**a)
    except ValueError:
        pass
    else:
        assert False

    a['_oid'] = 1
    o = metrique_object(**a)
    assert o
    assert o['_start'] < utcnow()

    expected_keys = sorted(
        ['_e', '_oid', '_start', '_end', 'col_1', 'col_2'])

    assert sorted(o.keys()) == expected_keys

    a['_start'] = now
    a['_end'] = now
    o = metrique_object(**a)
    assert o['_start'] == o['_end']

    # _end must come on/after _start
    try:
        a['_end'] = now - 1
        a['_start'] = now
        o = metrique_object(**a)
    except AssertionError:
        pass
    else:
        assert False, '_end was able to be smaller than _start!'

    # _start, if null, will be set to utcnow(); _end if null, stays null
    a['_start'] = None
    a['_end'] = None

    assert metrique_object(**a).get('_start') is not None
    assert metrique_object(**a).get('_end') is None

    # dates (_start/_end) are epoch
    a['_end'] = int(utcnow() + 100)  # +100 to ensure _end >= _start
    o = metrique_object(**a)
    assert isinstance(o['_start'], float)
    assert isinstance(o['_end'], float)

    a['_end'] = None
    o = metrique_object(**a)
