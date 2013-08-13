#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import simplejson as json

from datetime import datetime as dt
from dateutil.tz import tzutc
from time import mktime

from bson.objectid import ObjectId

class Encoder(json.JSONEncoder):
    '''
    Convert datetime.datetime to dict
    '''
    def default(self, obj):
        if isinstance(obj, dt):
            return {'$date': mktime(obj.timetuple())}
        elif isinstance(obj, ObjectId):
            return unicode(obj)
        else:
            return json.JSONEncoder.default(self, obj)


def decoder(dct):
    '''
    Convert datetime dicts to datetime objects
    '''
    if len(dct) == 1 and '$date' in dct:
        return dt.fromtimestamp(dct['$date'], tz=tzutc())
    return dct
