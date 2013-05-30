#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


def doublequote(item):
    return '"%s"' % item


def list2csv(_list, quote=False):
    if quote:
        _list = map(doublequote, _list)
    return ','.join(map(str, _list))
