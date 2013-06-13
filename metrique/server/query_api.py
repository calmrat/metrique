#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import pql
import re

from metrique.server.cubes import get_fields
from metrique.server.job import job_save


@job_save('query count')
def count(cube, query):
    logger.debug('Running Count')
    pql_parser = pql.SchemaFreeParser()
    try:
        # FIXME: make it a schema aware parser
        spec = pql_parser.parse(query)
    except Exception as e:
        raise ValueError("Invalid Query (%s)" % str(e))

    c = get_cube(cube)
    _cube = c.get_collection()

    logger.debug('Query: %s' % spec)

    docs = _cube.find(spec)
    if docs:
        return docs.count()
    else:
        return 0


def _get_date_pql_string(date):
    before = lambda d: 'start <= date("%s")' % d
    after = lambda d: '(end >= date("%s") or end == None)' % d
    split = date.split('~')
    logger.warn(split)
    if len(split) == 1:
        return '%s and %s' % (before(date), after(date))
    elif split[0] == '':
        return before(split[1])
    elif split[1] == '':
        return after(split[0])
    else:
        return '%s and %s' % (before(split[1]), after(split[0]))


@job_save('query find')
def find(cube, query, fields=None, date=None, most_recent=True):
    logger.debug('Running Find (%s)' % cube)
    if date is not None:
        # we will be doing a timeline query so we need to rename the fields
        # WARNING: might not work if some field is a substring of other field
        all_fields = get_fields(cube, '__all__')
        for f in all_fields:
            query = re.sub(f, 'fields.%s' % f, query)
        # add the date constraint
        query = query + ' and ' + _get_date_pql_string(date)
    pql_parser = pql.SchemaFreeParser()
    try:
        # FIXME: make it a schema aware parser
        spec = pql_parser.parse(query)
    except Exception as e:
        raise ValueError("Invalid Query (%s)" % str(e))

    c = get_cube(cube)
    _cube = c.get_collection(timeline=(date is not None))

    logger.debug('Query: %s' % spec)

    fields = get_fields(cube, fields)

    if date is not None:
        project_d = dict([(f, '$fields.%s' % f) for f in fields])
        project_d.update(dict(_id='$id', _start='$start', _end='$end'))
        if most_recent:
            docs = _cube.aggregate([{'$match': spec},
                                    {'$sort': {'start': -1}},
                                    {'$group': {'_id': '$id',
                                                'fields': {'$first':
                                                           '$fields'},
                                                'start': {'$first': '$start'},
                                                'end':  {'$first': '$end'},
                                                'id': {'$first': '$id'}}},
                                    {'$project': project_d}])
        else:
            docs = _cube.aggregate([{'$match': spec},
                                    {'$project': project_d}])
        docs = docs['result']
    else:
        docs = _cube.find(spec, fields)
        docs.batch_size(10000000)  # hard limit is 16M...
    docs = [d for d in docs]
    return docs


def parse_ids(ids, delimeter=','):
    if isinstance(ids, basestring):
        ids = [s.strip() for s in ids.split(delimeter)]
    if type(ids) is not list:
        raise TypeError("ids expected to be a list")
    return ids


@job_save('query fetch')
def fetch(cube, fields, skip=0, limit=0, ids=None):
    logger.debug('Running Fetch (skip:%s, limit:%s, ids:%s)' % (
        skip, limit, len(ids)))
    logger.debug('... Fields: %s' % fields)

    c = get_cube(cube)
    _cube = c.get_collection()

    fields = get_fields(cube, fields)
    logger.debug('Return Fields: %s' % fields)

    sort = [('_id', 1)]

    if ids:
        spec = {'_id': {'$in': parse_ids(ids)}}
    else:
        spec = {}

    docs = _cube.find(spec, fields,
                      skip=skip, limit=limit,
                      sort=sort)
    docs.batch_size(10000000)  # hard limit is 16M...
    return [d for d in docs]


@job_save('query aggregate')
def aggregate(cube, pipeline):
    logger.debug('Running Aggregation')
    logger.debug('Pipeline (%s): %s' % (type(pipeline), pipeline))
    c = get_cube(cube)
    _cube = c.get_collection()
    return _cube.aggregate(pipeline)
