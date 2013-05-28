#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from datetime import datetime
from dateutil.parser import parse as dt_parse
from metrique.tools.constants import UTC, NONE_VALUES
from metrique.tools.constants import UNICODE_TYPE
from metrique.tools.constants import STR_TYPE, FLOAT_TYPE, DATETIME_TYPE
from metrique.tools.constants import INT_TYPE, BOOL_TYPE, NONE_TYPE
from metrique.tools.constants import LONG_TYPE, STR_TYPES
from metrique.tools.constants import NOMINAL_TYPES, LIST_TYPES

from logging import getLogger
logger = getLogger(__name__)

# These are constants to load at module init time
# type_cast() will be called many thousands of times so
# no reason to re-evaluate these lists every time...
VALID_RAW_TYPES = (NONE_TYPE, FLOAT_TYPE, INT_TYPE, LONG_TYPE, BOOL_TYPE,
                   DATETIME_TYPE, UNICODE_TYPE, STR_TYPE)
VALID_CAST_TYPES = (FLOAT_TYPE, DATETIME_TYPE, UNICODE_TYPE)


def type_cast(raw, cast_type=None):
    '''
    Convert a token from one supported type to another.
    Default cast type is unicode string

    Input types supported: none, bool, int, float, long, datetime, unicode, str
    Output types supported: float, datetime, unicode
    '''
    if not cast_type:
        cast_type = unicode
    elif cast_type not in VALID_CAST_TYPES:
        raise TypeError("Unsupported cast type: %s" % cast_type)

    if type(raw) in LIST_TYPES:
        raw_items = raw  # we already have a list...
    else:
        raw_items = [raw]  # put it in a list so we can iterate it in a moment

    tokens = []
    for item in raw_items:
        item_type = type(item)
        if item_type not in VALID_RAW_TYPES:
            raise TypeError("Unsupported input token type: %s" % item_type)

        elif item in NONE_VALUES:
            token = None  # normalize nulls to None object

        elif isinstance(item, cast_type):
            if cast_type is STR_TYPE:
                # force str into unicode
                token = unicode(item, 'UTF-8', errors='ignore')
            elif item_type is DATETIME_TYPE:
                # update the datetime object so it's tz aware - UTC
                token = item.replace(tzinfo=UTC)
            else:  # raw type is the type we need it to be in already; move on...
                token = item

        elif cast_type is datetime:
            # FIXME: we should allow drivers set parser format?
            if item_type in STR_TYPES:
                try:
                    token = dt_parse(item).replace(tzinfo=UTC)
                except Exception:
                    try:
                        ts, tz = item.split(' ')
                        dt = datetime.fromtimestamp(float(ts))
                        token = dt_parse('%s %s' % (dt, tz))
                    except Exception:
                        raise TypeError("Invalid format for date parse (%s)" % item)
            elif item_type in NOMINAL_TYPES:
                try:
                    token = datetime.fromtimestamp(item, UTC)
                except:
                    raise TypeError("Failed to convert (%s) as timestamp to date" % item)
            else:
                raise TypeError("Unsupported type for type_cast to datetime")

        elif cast_type is not unicode:
            # we have work to do... normalize to the type expected
            try:
                token = cast_type(item)
            except:
                raise TypeError("Unsupported cast type: (%s) %s" % (cast_type, item))

        else:
            # unicode's first argument must be a str, so, first convert
            # whatever object is to it's string equivalent then
            # we can convert to unicode
            try:
                token = unicode(str(item), 'utf8')
            except UnicodeDecodeError:
                token = unicode(str(item), 'latin-1')

        tokens.append(token)

    if len(tokens) == 1:
        return tokens[0]
    else:
        return tokens
