#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>
# Contributor: "Juraj Niznan" <jniznan@redhat.com>

from datetime import datetime
from dateutil.parser import parse as dt_parse
import pytz


def get_timezone_converter(from_timezone):
    utc = pytz.utc
    from_tz = pytz.timezone(from_timezone)

    def timezone_converter(self, dt):
        try:
            return from_tz.localize(dt).astimezone(utc)
        except Exception:
            return None
    return timezone_converter


def ts_tz2dt_tz(ts_str):
    ts, tz = ts_str.split(' ')
    _dt = datetime.fromtimestamp(float(ts))
    _dt_str = '%s %s' % (_dt.isoformat(), tz)
    dt_tz = dt_parse(_dt_str)
    return dt_tz
