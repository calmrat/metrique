#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import pql

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


def find(cube, query, fields=None):
    logger.debug('Running Find')
    pql_parser = pql.SchemaFreeParser()
    try:
        # FIXME: make it a schema aware parser
        spec = pql_parser.parse(query)
    except Exception as e:
        raise ValueError("Invalid Query (%s)" % str(e))

    c = get_cube(cube)
    _cube = c.get_collection()

    logger.debug('Query: %s' % spec)

    fields = get_fields(cube, fields)
    logger.debug('Return Fields: %s' % fields)

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
