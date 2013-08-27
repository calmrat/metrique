#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import simplejson as json

from datetime import datetime as dt

from metrique.utils import dt2ts

json_encoder = json.JSONEncoder()


def json_encode(obj):
    '''
    Convert datetime.datetime to timestamp
    '''
    if isinstance(obj, dt):
        return dt2ts(obj)
    else:
        return json_encoder.default(obj)
