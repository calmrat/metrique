#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import simplejson as json

from datetime import datetime as dt
from dateutil.tz import tzutc
from time import mktime


class Encoder(json.JSONEncoder):
    '''
    Convert datetime.datetime to dict
    '''
    def default(self, obj):
        if isinstance(obj, dt):
            return {'$date': mktime(obj.timetuple())}
        else:
            return obj 


def decoder(dct):
    ''' 
    Convert datetime dicts to datetime objects
    '''
    return dt.fromtimestamp(dct['$date'], tz=tzutc()) if len(dct) == 1 and '$date' in dct else dct 

