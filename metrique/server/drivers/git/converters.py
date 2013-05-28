#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from datetime import datetime
from dateutil.parser import parse as dt_parse


def ts(dt):
    if isinstance(dt, datetime):
        return dt
    ts, tz = dt.split(' ')
    _dt = datetime.fromtimestamp(float(ts))
    _dt_str = '%s %s' % (_dt.isoformat(), tz)
    dt_tz = dt_parse(_dt_str)
    return dt_tz
