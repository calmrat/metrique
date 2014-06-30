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


def test_parse_fields():
    from metrique.parse import parse_fields

    OK_list = ['a', 'b', 'c']
    OK_dct = {'a': 1, 'b': 1, 'c': 1}
    all_ = '~'
    fields_str = 'c  , b  , a  '
    fields_list = ['a  ', 'c ', '  b']
    fields_dct = {' a': 1, 'b ': 1, 'c  ': 1}
    fields_dct_BAD = {' a': None, 'b ': 1, 'c  ': 1}

    assert parse_fields(fields_list) == OK_list
    assert parse_fields(fields_str) == OK_list
    assert parse_fields(fields_str, as_dict=True) == OK_dct
    assert parse_fields(fields_dct, as_dict=True) == OK_dct

    try:
        # we expect values to be int!
        parse_fields(fields_dct_BAD, as_dict=True)
    except TypeError:
        pass
    else:
        assert False

    assert parse_fields(None) == []
    assert parse_fields(False) == []
    assert parse_fields(all_) == []
    assert parse_fields(None, as_dict=True) == {}
    assert parse_fields(False, as_dict=True) == {}
    assert parse_fields(all_, as_dict=True) == {}


def test_date_range():
    from metrique.parse import date_range, ts2dt

    all_ = '~'
    after = '2014-01-01~'
    before = '~2014-01-01'
    after_before = '2014-01-01~2014-01-01'

    dt = ts2dt('2014-01-01')

    _end_null = '_end == None'
    _all = ''
    _after = '(_end >= date("%s") or %s)' % (dt, _end_null)
    _before = '_start <= date("%s")' % dt
    _after_before = '%s and %s' % (_before, _after)

    assert date_range(None) == _end_null
    assert date_range(all_) == _all
    assert date_range(after) == _after
    assert date_range(before) == _before
    assert date_range(after_before) == _after_before
