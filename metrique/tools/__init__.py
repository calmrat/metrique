#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import hashlib
import uuid


def doublequote(item):
    ''' convert a given obj to string, double-quoted'''
    return '"%s"' % item


def list2csv(_list, quote=False):
    ''' convert a list of objects into a csv string '''
    if quote:
        _list = map(doublequote, _list)
    return ','.join(map(str, _list))


def hash_password(password, salt=None):
    ''' salt sha512 hexdigest a password '''
    if not salt:
        salt = uuid.uuid4().hex
    return salt, hashlib.sha512(password + salt).hexdigest()
