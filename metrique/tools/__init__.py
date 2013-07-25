#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import hashlib
import pytz
import uuid

# FIXME: OBSOLETE? WHAT IN HERE IS NOT USED ANYMORE? REMOVE IT


def doublequote(item):
    ''' convert a given obj to string, double-quoted'''
    return '"%s"' % item


def csv2list(csv, delimiter=','):
    ''' convert a str(csv,csv) into a list of strings '''
    if type(csv) in [list, tuple, set]:
        return list(csv)
    elif csv:
        return [s.strip() for s in csv.split(delimiter)]
    else:
        return None


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


def get_timezone_converter(from_timezone):
    utc = pytz.utc
    from_tz = pytz.timezone(from_timezone)

    def timezone_converter(self, dt):
        try:
            return from_tz.localize(dt).astimezone(utc)
        except Exception:
            return None
    return timezone_converter


def perc(numerator, denominator):
    return (float(numerator) / denominator) * 100
