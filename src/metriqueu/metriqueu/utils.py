#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from bson.objectid import ObjectId
from calendar import timegm
from datetime import datetime as dt
from decorator import decorator
from dateutil.parser import parse as dt_parse
import hashlib
import pytz

sha1 = hashlib.sha1


def _memo(func, *args, **kw):
    # sort and convert list items to tuple for hashability
    if type(kw) is list:
        kw = frozenset(kw)
    args = list(args)
    for k, arg in enumerate(args):
        if type(arg) is list:
            args[k] = frozenset(arg)
    # frozenset is used to ensure hashability
    key = frozenset(args), frozenset(kw.iteritems())
    cache = func.cache  # attributed added by memoize
    if key in cache:
        return cache[key]
    else:
        cache[key] = result = func(*args, **kw)
    return result


def memo(f):
    ''' memoize function output '''
    f.cache = {}
    return decorator(_memo, f)


def new_oid():
    '''
    Creates a new ObjectId and casts it to string,
    so it's easily serializable
    '''
    return str(ObjectId())


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


def milli2sec(ts):
    ''' normalize timestamps to timestamp int's (seconds) '''
    if not ts:
        return ts
    return float(float(ts) / 1000.)  # convert milli to seconds


def ts2dt(ts, milli=False, tz_aware=True):
    ''' convert timestamp int's (seconds) to datetime objects '''
    if not ts:
        return ts
    elif isinstance(ts, dt):  # its a dt already
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


def dt2ts(dt):
    ''' convert datetime objects to timestamp int's (seconds) '''
    if isinstance(dt, (int, long, float, complex)):  # its a ts already
        return dt
    elif isinstance(dt, basestring):  # convert to datetime first
        return dt2ts(dt_parse(dt))
    else:
        return timegm(dt.timetuple())


def jsonhash(obj):
    '''
    calculate the objects hash based on all field values
    '''
    if isinstance(obj, dict):
        return sha1(
            repr(frozenset(
                dict(
                    [(jsonhash(k), jsonhash(v)) for k, v in obj.items()]
                ).items()
            ))
        ).hexdigest()
    elif isinstance(obj, (list, tuple, set)):
        return sha1(
            repr(tuple(
                sorted(
                    [jsonhash(e) for e in list(obj)]
                )
            ))
        ).hexdigest()
    else:
        return repr(obj)


def utcnow(dt=False, tz_aware=False):
    now = dt.utcnow()
    if tz_aware:
        return pytz.UTC.localize(now)
    elif dt:
        return now
    else:
        return dt2ts(now)


def set_default(key, default, null_ok=False, err_msg=None):
    if not err_msg:
        err_msg = "non-null value required for %s" % key
    if not null_ok and key is None and default is None:
            raise RuntimeError(err_msg)
    try:
        return key or default()  # if we get 'type' obj, eg `list`
    except (TypeError, AttributeError):
        return key or default
