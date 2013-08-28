#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from bson.objectid import ObjectId
from calendar import timegm
from datetime import datetime
from dateutil.parser import parse as dt_parse
import pytz


def perc(numerator, denominator):
    return (float(numerator) / denominator) * 100


def doublequote(item):
    ''' convert a given obj to string, double-quoted'''
    return '"%s"' % item


def list2csv(_list, quote=False):
    ''' convert a list of objects into a csv string '''
    if quote:
        _list = map(doublequote, _list)
    return ','.join(map(str, _list))


def csv2list(csv, delimiter=','):
    ''' convert a str(csv,csv) into a list of strings '''
    if type(csv) in [list, tuple, set]:
        return list(csv)
    elif csv:
        return [s.strip() for s in csv.split(delimiter)]
    elif csv is None:
        return []
    else:
        raise ValueError(
            "Failed to convert csv string to list; got %s" % csv)


def get_timezone_converter(from_timezone):
    utc = pytz.utc
    from_tz = pytz.timezone(from_timezone)

    def timezone_converter(self, dt):
        try:
            return from_tz.localize(dt).astimezone(utc)
        except Exception:
            return None
    return timezone_converter


def milli2sec(ts):
    ''' normalize timestamps to timestamp int's (seconds) '''
    if not ts:
        return ts
    return float(float(ts) / 1000.)  # convert milli to seconds


def batch_gen(data, batch_size):
    '''
    Usage::
        for batch in batch_gen(iter, 100):
            do_something(batch)
    '''
    if not data:
        return

    if batch_size == -1:
        # override: yield the whole list
        yield data

    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def ts2dt(ts, milli=False, tz_aware=True):
    ''' convert timestamp int's (seconds) to datetime objects '''
    if not ts:
        return ts
    elif isinstance(ts, datetime):  # its a dt already
        return ts
    # ts must be float and in seconds
    elif milli:
        ts = float(ts) / 1000.  # convert milli to seconds
    else:
        ts = float(ts)  # already in seconds
    if tz_aware:
        return datetime.fromtimestamp(ts, tz=pytz.utc)
    else:
        return datetime.utcfromtimestamp(ts)


def dt2ts(dt):
    ''' convert datetime objects to timestamp int's (seconds) '''
    if isinstance(dt, (int, long, float, complex)):  # its a ts already
        return dt
    elif isinstance(dt, basestring):  # convert to datetime first
        return dt2ts(dt_parse(dt))
    else:
        return timegm(dt.timetuple())


def new_oid():
    '''
    Creates a new ObjectId and casts it to string,
    so it's easily serializable
    '''
    return str(ObjectId())
