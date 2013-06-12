#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import datetime
import simplejson as json
import re

from metrique.tools.constants import RE_TYPE

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
