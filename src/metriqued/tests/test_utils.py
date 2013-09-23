#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from copy import copy


def test_jsonhash():
    from metriqued.utils import jsonhash

    dct = {'a': [3, 2, 1], 'z': ['a', 'c', 'b', 1], 'b': {1: [], 3: {}}}

    dct_sorted_z = copy(dct)
    dct_sorted_z['z'] = sorted(dct_sorted_z['z'])

    dct_diff = copy(dct)
    del dct_diff['z']

    DCT = '541d0fa961265d976d9a27e8632787875dc58406'
    DCT_SORTED_Z = 'ca4631674276933bd251bd4bc86372138a841a4b'
    DCT_DIFF = '07d6c518867fb6b6c77c0ec1d835fb800419fc24'

    assert dct != dct_sorted_z

    assert jsonhash(dct) == DCT
    assert jsonhash(dct_sorted_z) == DCT_SORTED_Z
    assert jsonhash(dct_diff) == DCT_DIFF

    ' list sort order is an identifier of a unique object '
    assert jsonhash(dct) != jsonhash(dct_sorted_z)
