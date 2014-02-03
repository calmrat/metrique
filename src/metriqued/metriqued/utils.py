#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriqued.uitls
~~~~~~~~~~~~~~~

This module contains various shared utils used by metriqued and friends.
'''

import pql
from bson.timestamp import Timestamp
import re
import simplejson as json

from metriqueu.utils import dt2ts

json_encoder = json.JSONEncoder()

OBJECTS_MAX_BYTES = 16777216
EXISTS_SPEC = {'$exists': 1}


def date_pql_string(date):
    '''
    Generate a new pql date query component that can be used to
    query for date (range) specific data in cubes.

    :param date: metrique date (range) to apply to pql query

    If date is None, the resulting query will be a current value
    only query (_end == None)

    The tilde '~' symbol is used as a date range separated.

    A tilde by itself will mean 'all dates ranges possible'
    and will therefore search all objects irrelevant of it's
    _end date timestamp.

    A date on the left with a tilde but no date on the right
    will generate a query where the date range starts
    at the date provide and ends 'today'.
    ie, from date -> now.

    A date on the right with a tilde but no date on the left
    will generate a query where the date range starts from
    the first date available in the past (oldest) and ends
    on the date provided.
    ie, from beginning of known time -> date.

    A date on both the left and right will be a simple date
    range query where the date range starts from the date
    on the left and ends on the date on the right.
    ie, from date to date.
    '''
    if date is None:
        return '_end == None'
    if date == '~':
        return ''

    before = lambda d: '_start <= %f' % dt2ts(d)
    after = lambda d: '(_end >= %f or _end == None)' % dt2ts(d)
    split = date.split('~')
    # replace all occurances of 'T' with ' '
    # this is used for when datetime is passed in
    # like YYYY-MM-DDTHH:MM:SS instead of
    #      YYYY-MM-DD HH:MM:SS as expected
    # and drop all occurances of 'timezone' like substring
    split = [re.sub('\+\d\d:\d\d', '', d.replace('T', ' ')) for d in split]
    if len(split) == 1:
        # 'dt'
        return '%s and %s' % (before(split[0]), after(split[0]))
    elif split[0] == '':
        # '~dt'
        return before(split[1])
    elif split[1] == '':
        # 'dt~'
        return after(split[0])
    else:
        # 'dt~dt'
        return '%s and %s' % (before(split[1]), after(split[0]))


def query_add_date(query, date):
    '''
    Take an existing pql query and append a date (range)
    limiter.

    :param query: pql query
    :param date: metrique date (range) to append
    '''
    date_pql = date_pql_string(date)
    if query and date_pql:
        return '%s and %s' % (query, date_pql)
    return query or date_pql


def parse_pql_query(query):
    '''
    Given a pql based query string, parse it using
    pql.SchemaFreeParser and return the resulting
    pymongo 'spec' dictionary.

    :param query: pql query
    '''
    if not query:
        return {}
    if not isinstance(query, basestring):
        raise TypeError("query expected as a string")
    pql_parser = pql.SchemaFreeParser()
    spec = pql_parser.parse(query)
    return spec


def json_encode(obj):
    '''
    Convert pymongo.timestamp.Timestamp to epoch

    :param obj: value to (possibly) convert
    '''
    if isinstance(obj, Timestamp):
        return obj.time
    else:
        return json_encoder.default(obj)
