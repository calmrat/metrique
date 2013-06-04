#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import pql
import re

from metrique.server.drivers.drivermap import get_cube, get_fields


def parse_ids(ids, delimeter=','):
    if isinstance(ids, basestring):
        ids = [s.strip() for s in ids.split(delimeter)]
    if type(ids) is not list:
        raise TypeError("ids expected to be a list")
    return ids


def get_tokens(cube, qspec, return_field=None):
    '''
    shortcut for finding fields tokens;
    return a list of tokens which map to raw_pattern and
    return_field/compare_field
    '''
    c = get_cube(cube)
    _cube = c.get_collection()
    if return_field is None:
        return_field = '_id'
        spec = {}
    else:
        spec = {return_field: {'$exists': True}}

    for compare_field, raw_pattern in qspec.iteritems():
        spec.update({compare_field: raw_pattern})

    rf = {return_field: 1}

    docs = _cube.find(spec, rf, manipulate=False)
    docs.batch_size(10000000)  # hard limit is 16M...

    _tokens = []
    if docs:
        for doc in docs:
            tokens = doc.get(return_field)
            if not tokens:
                continue
            elif type(tokens) is list:
                _tokens.extend(tokens)
            else:
                _tokens.append(tokens)

    if not _tokens:
        _tokens = None
    elif len(_tokens) is 1:
        _tokens = _tokens[0]

    return _tokens


def find_tokens(cube, return_field, raw_pattern,
                compare_field='_id'):
    '''
    wrapper around get_tokens; takes multiple items and automatically
    calls get_tokens per each item
    '''
    qspec = {compare_field: raw_pattern}
    return get_tokens(cube, qspec, return_field)


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


def find(cube, query, fields=None, date=None, most_recent=True):
    logger.debug('Running Find')
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


def aggregate(cube, pipeline):
    logger.debug('Running Aggregation')
    logger.debug('Pipeline (%s): %s' % (type(pipeline), pipeline))
    c = get_cube(cube)
    _cube = c.get_collection()
    return _cube.aggregate(pipeline)
