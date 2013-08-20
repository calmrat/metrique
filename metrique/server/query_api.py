#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import pql
import re

from metrique.server.cubes import get_fields, get_cube
from metrique.server.job import job_save
from metrique.tools import dt2ts

BATCH_SIZE = 16777216  # hard limit is 16M...


@job_save('query distinct')
def distinct(cube, field):
    logger.debug('Running Distinct (%s.%s)' % (cube, field))
    _cube = get_cube(cube)
    return _cube.distinct(field)


@job_save('query count')
def count(cube, query):
    logger.debug('Running Count')
    pql_parser = pql.SchemaFreeParser()
    try:
        spec = pql_parser.parse(query)
    except Exception as e:
        raise ValueError("Invalid Query (%s)" % str(e))

    _cube = get_cube(cube)

    logger.debug('Mongo Query: %s' % spec)

    docs = _cube.find(spec)
    if docs:
        result = docs.count()
    else:
        result = 0
    docs.close()
    return result


def _get_date_pql_string(date, prefix=' and '):
    if date is None:
        return prefix + '_end == None'
    if date == '~':
        return ''

    dt_str = date.replace('T', ' ')
    dt_str = re.sub('(\+\d\d:\d\d)?$', '', dt_str)

    before = lambda d: '_start <= %s' % dt2ts(d)
    after = lambda d: '(_end >= %s or _end == None)' % dt2ts(d)
    split = date.split('~')
    logger.warn(split)
    if len(split) == 1:
        ret = '%s and %s' % (before(dt_str), after(dt_str))
    elif split[0] == '':
        ret = '%s' % before(split[1])
    elif split[1] == '':
        ret = '%s' % after(split[0])
    else:
        ret = '%s and %s' % (before(split[1]), after(split[0]))
    return prefix + ret


def _check_sort(sort):
    if not sort:
        sort = [('_oid', 1)]

    try:
        assert len(sort[0]) == 2
    except (AssertionError, IndexError, TypeError):
        raise ValueError("Invalid sort value; try [('_id': -1)]")

    return sort


@job_save('query find')
def find(cube, query, fields=None, date=None, sort=None, one=False):
    logger.debug('Running Find (%s)' % cube)

    sort = _check_sort(sort)
    _cube = get_cube(cube)
    fields = get_fields(cube, fields)

    query += _get_date_pql_string(date)

    logger.debug("PQL Query: %s" % query)
    pql_parser = pql.SchemaFreeParser()
    try:
        spec = pql_parser.parse(query)
    except Exception as e:
        raise ValueError("Invalid Query (%s): %s" % (str(e), query))

    logger.debug('Query: %s' % spec)

    if one:
        result = _cube.find_one(spec, fields, sort=sort)
    else:
        result = _cube.find(spec, fields, sort=sort)
        result.batch_size(BATCH_SIZE)
        result = tuple(result)
    return result


def parse_ids(ids, delimeter=','):
    if isinstance(ids, basestring):
        ids = [s.strip() for s in ids.split(delimeter)]
    if type(ids) is not list:
        raise TypeError("ids expected to be a list")
    return ids


@job_save('query fetch')
def fetch(cube, fields=None, date=None, sort=None, skip=0, limit=0, ids=None):
    logger.debug('Running Fetch (skip:%s, limit:%s, ids:%s)' % (
        skip, limit, len(ids)))

    sort = _check_sort(sort)
    _cube = get_cube(cube)
    fields = get_fields(cube, fields)

    spec = {'_oid': {'$in': parse_ids(ids)}} if ids else {}
    spec.update(pql.find(_get_date_pql_string(date, '')))

    result = _cube.find(spec, fields, sort=sort, skip=skip, limit=limit)
    result.batch_size(BATCH_SIZE)
    result = tuple(result)
    return result


@job_save('query aggregate')
def aggregate(cube, pipeline):
    logger.debug('Running Aggregation')
    logger.debug('Pipeline (%s): %s' % (type(pipeline), pipeline))
    _cube = get_cube(cube)
    return _cube.aggregate(pipeline)
