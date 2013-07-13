#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import datetime
from dateutil.parser import parse as dt_parse
import simplejson as json
import re
from bson.objectid import ObjectId

from metrique.tools.constants import RE_TYPE, RE_DATE_DATETIME

# FIXME: IS THIS EVER EXPECTED TO BE PASSED BY CLIENT ANYMORE?
# It was used... to handle when clients used a compiled regex
# in their queries... but clients should be discouraged
# from this... they can used regex("...")
SRE_PATTERN_ID = '_sre.SRE_Pattern'
HAS_SRE_OBJ = re.compile('(<%s (.+)>)' % SRE_PATTERN_ID)


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
        elif type(obj) is RE_TYPE:
            encoded = '<%s %s>' % (SRE_PATTERN_ID, obj.pattern)
        else:
            encoded = unicode(obj)
        return encoded


def decoder(dct):
    '''
    Convert
     * object ids back to mongo ObjectId() objects
     * datetime strings to datetime objects
    '''
    for k, v in dct.items():
        if isinstance(v, basestring) and RE_DATE_DATETIME.match(v):
            # FIXME: SHOULD THIS BE pandas Time instead of dt_parse?
            # to handle timestamps as well as dt strings?
            # Are we correctly handling TZones? UTC? test....
            try:
                dct[k] = dt_parse(v)
            except:
                pass
    if '_id' in dct:
        try:
            dct['_id'] = ObjectId(dct['_id'])
        except:
            pass
    return dct
