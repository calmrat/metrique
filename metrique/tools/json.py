#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import datetime
from dateutil.parser import parse as dt_parse
import simplejson as json
from bson.objectid import ObjectId

from metrique.tools.constants import RE_DATE_DATETIME


class Encoder(json.JSONEncoder):
    '''
        Convert
        * datetime.datetime and .date -> date.isoformat
        * set -> list
        * re_type -> sre_pattern_id (see constant)
        # DEFAULT: unicode string representation of the object
    '''
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            encoded = obj.isoformat()
        elif isinstance(obj, datetime.date):
            encoded = obj.isoformat()
        elif isinstance(obj, set):
            encoded = list(obj)
        else:
            encoded = unicode(obj)
        return encoded


def _id_convert(dct):
    if '_id' in dct:
        try:
            dct['_id'] = ObjectId(dct['_id'])
        except:
            pass
    return dct


def decoder(dct):
    '''
    Convert
     * object ids back to mongo ObjectId() objects
     * datetime strings to datetime objects
    '''
    for k, v in dct.items():
        if isinstance(v, basestring) and RE_DATE_DATETIME.match(v):
            try:
                dct[k] = dt_parse(v)
            except:
                pass
        elif type(v) is list:
            dct[k] = tuple(v)
        else:
            pass
    dct = _id_convert(dct)
    return dct
