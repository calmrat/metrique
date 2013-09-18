#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from bson.objectid import ObjectId
from calendar import timegm
from datetime import datetime as dt
from dateutil.parser import parse as dt_parse
from hashlib import sha1
import pytz


def batch_gen(data, batch_size):
    '''
    Usage::
        for batch in batch_gen(iter, 100):
            do_something(batch)
    '''
    if not data:
        return

    if batch_size <= 0:
        # override: yield the whole list
        yield data

    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def dt2ts(dt):
    ''' convert datetime objects to timestamp int's (seconds) '''
    if isinstance(dt, (int, long, float, complex)):  # its a ts already
        return dt
    elif isinstance(dt, basestring):  # convert to datetime first
        return dt2ts(dt_parse(dt))
    else:
        return timegm(dt.timetuple())


def jsonhash(obj, root=True):
    '''
    calculate the objects hash based on all field values
    '''
    # FIXME: check if 'mutable mapping'
    if isinstance(obj, dict):
        result = frozenset(
            [(jsonhash(k),
              jsonhash(v, False)) for k, v in obj.items()])
    # FIXME: check if 'iterable'
    elif isinstance(obj, (list, tuple, set)):
        result = tuple(sorted(jsonhash(e, False) for e in obj))
    else:
        result = repr(obj)
    return sha1(repr(result)).hexdigest() if root else result


def milli2sec(ts):
    ''' normalize timestamps to timestamp int's (seconds) '''
    if not ts:
        return ts
    return float(float(ts) / 1000.)  # convert milli to seconds


def new_oid():
    '''
    Creates a new ObjectId and casts it to string,
    so it's easily serializable
    '''
    return str(ObjectId())


def set_default(key, default, null_ok=False, err_msg=None):
    if not err_msg:
        err_msg = "non-null value required for %s" % key
    if not null_ok and key is None and default is None:
            raise RuntimeError(err_msg)
    try:
        # if we get 'type' obj, eg `list`
        result = key if key is not None else default()
    except (TypeError, AttributeError):
        result = key if key is not None else default
    return result


def ts2dt(ts, milli=False, tz_aware=True):
    ''' convert timestamp int's (seconds) to datetime objects '''
    if not ts or isinstance(ts, dt):
        # its not a timestamp or is already a datetime
        return ts
    # ts must be float and in seconds
    elif milli:
        ts = float(ts) / 1000.  # convert milli to seconds
    else:
        ts = float(ts)  # already in seconds
    if tz_aware:
        return dt.fromtimestamp(ts, tz=pytz.utc)
    else:
        return dt.utcfromtimestamp(ts)


def utcnow(dt=False, tz_aware=False):
    now = dt.utcnow()
    if tz_aware:
        return pytz.UTC.localize(now)
    elif dt:
        return now
    else:
        return dt2ts(now)
