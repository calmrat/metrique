#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


def test_csv2list():
    ' args: csv, delimiter=",") '
    ' always expect output of a list of strings '

    from metrique.utils import csv2list

    d = ','

    l = ['1', '2', '3']
    t = ('1', '2', '3')
    s = set(['1', '2', '3'])
    _s = '1,2,      3'

    assert csv2list(l, d) == l
    assert csv2list(t, d) == l
    assert csv2list(s, d) == l

    assert csv2list(_s, d) == l

    assert csv2list(None, d) == []

    try:
        csv2list(True, d)
    except TypeError:
        pass
