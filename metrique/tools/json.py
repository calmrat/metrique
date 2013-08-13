#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import simplejson as json

from datetime import datetime as dt
from dateutil.tz import tzutc
from calendar import timegm

from bson.objectid import ObjectId
from pymongo.cursor import Cursor

json_encoder = json.JSONEncoder()


def json_encode(obj):
    '''
    Convert datetime.datetime to timestamp
    '''
    if isinstance(obj, dt):
        return {'$date': timegm(obj.timetuple())}
    elif isinstance(obj, ObjectId):
        return unicode(obj)
    elif isinstance(obj, Cursor):
        return tuple(obj)
    else:
        return json_encoder.default(obj)


def decoder(dct):
    '''
    Convert datetime dicts to datetime objects
    '''
    if len(dct) == 1 and '$date' in dct:
        return dt.fromtimestamp(dct['$date'], tz=tzutc())
    return dct
