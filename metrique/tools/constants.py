#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import re
from datetime import datetime, date
from datetime import time as dt_time
from pytz import timezone


UTC = timezone("UTC")
EST = timezone("EST")
CEST = timezone("CET")

# Types... for type checking
NONE_TYPE = type(None)
DATETIME_TYPE = type(datetime(1, 1, 1))
DATE_TYPE = type(date(1, 1, 1))
RE_TYPE = type(re.compile(''))
TUPLE_TYPE = type(tuple())
LIST_TYPE = type(list())
DICT_TYPE = type(dict())
STR_TYPE = type(str())
UNICODE_TYPE = type(unicode())
FLOAT_TYPE = type(float())
INT_TYPE = type(int())
LONG_TYPE = type(long())
BOOL_TYPE = type(bool())

LIST_TYPES = (LIST_TYPE, TUPLE_TYPE)
STR_TYPES = (STR_TYPE, UNICODE_TYPE)
NOMINAL_TYPES = (INT_TYPE, FLOAT_TYPE, LONG_TYPE)

NONE_VALUES = (None, '---', '', 'None')

# 00:00:00; used to combine with date() to form datetime() at midnight
NULL_TIME = dt_time()

# Convenience constants; these are exipration times in seconds
# console escapes for colored output
WHITE = '\033[97m'
BLUE = '\033[96m'
YELLOW = '\033[93m'
GREEN = '\033[92m'
RED = '\033[91m'
ENDC = '\033[0m'

# date only
RE_DATE = re.compile('(\d\d\d\d)-(\d\d)-(\d\d)')
# datetime only
RE_DATETIME = re.compile('(\d\d\d\d)-(\d\d)-(\d\d)(T| )?(\d\d):(\d\d):(\d\d)Z?')
# either date or datetime
RE_DATE_DATETIME = re.compile('(\d\d\d\d)-(\d\d)-(\d\d)((T| )?(\d\d):(\d\d):(\d\d)Z?)?')

RE_DRIVER_CUBE = re.compile('^([^_]+)_(.+)$', re.U)

# FIXME: Can these be removed??
SRE_PATTERN_ID = '_sre\.SRE_Pattern'
HAS_SRE_PATTERN = re.compile('<%s (.+)>' % SRE_PATTERN_ID)
