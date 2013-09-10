#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import pql
import re
import random
from tornado.web import HTTPError

from metriqued.cubes import get_fields, get_collection
from metriqued.utils import dt2ts

BATCH_SIZE = 16777216  # hard limit is 16M...


def log_head(owner, cube, cmd, *args):
    logger.debug('%s (%s.%s): %s' % (cmd, owner, cube, args))


def aggregate(owner, cube, pipeline):
    log_head(owner, cube, 'aggregate', pipeline)
    logger.debug('Pipeline (%s): %s' % (type(pipeline), pipeline))
    _cube = get_collection(owner, cube)
    return _cube.aggregate(pipeline)


def distinct(owner, cube, field):
    log_head(owner, cube, 'distinct', field)
    _cube = get_collection(owner, cube)
    return _cube.distinct(field)


def count(owner, cube, query, date=None):
    log_head(owner, cube, 'count', query, date)
    try:
        spec = pql.find(query + _get_date_pql_string(date))
    except Exception as e:
        raise HTTPError(400, "Invalid Query (%s)" % str(e))

    _cube = get_collection(owner, cube)

    logger.debug('Mongo Query: %s' % spec)

    docs = _cube.find(spec)
    result = docs.count() if docs else 0
    docs.close()
    return result


def _get_date_pql_string(date, prefix=' and '):
    if date is None:
        return prefix + '_end == None'
    if date == '~':
        return ''

    dt_str = date.replace('T', ' ')
    dt_str = re.sub('(\+\d\d:\d\d)?$', '', dt_str)

    before = lambda d: '_start <= %f' % dt2ts(d)
    after = lambda d: '(_end >= %f or _end == None)' % dt2ts(d)
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
        raise HTTPError(400, "Invalid sort value; try [('_id': -1)]")

    return sort


def find(owner, cube, query, fields=None, date=None, sort=None, one=False,
         explain=False, merge_versions=True):
    log_head(owner, cube, 'find', query, date)
    sort = _check_sort(sort)

    fields = get_fields(cube, fields)
    if date is None or ('_id' in fields and fields['_id']):
        merge_versions = False

    query += _get_date_pql_string(date)

    logger.debug("PQL Query: %s" % query)
    pql_parser = pql.SchemaFreeParser()
    try:
        spec = pql_parser.parse(query)
    except Exception as e:
        raise HTTPError(400, "Invalid Query (%s): %s" % (str(e), query))
    logger.debug('Query: %s' % spec)

    _cube = get_collection(owner, cube, admin=False)
    if explain:
        result = _cube.find(spec, fields, sort=sort).explain()
    elif one:
        result = _cube.find_one(spec, fields, sort=sort)
    elif merge_versions:
        # merge_versions ignores sort (for now)
        result = _merge_versions(_cube, spec, fields)
    else:
        result = _cube.find(spec, fields, sort=sort)
        result.batch_size(BATCH_SIZE)
        result = tuple(result)
    return result


def _merge_versions(_cube, spec, fields):
    '''
    merge versions with unchanging fields of interest
    '''
    logger.debug("Merging docs...")
    # contains a dummy document to avoid some condition checks in merge_doc
    ret = [{'_oid': None}]
    no_check = set(['_start', '_end'])

    def merge_doc(doc):
        '''
        merges doc with the last document in ret if possible
        '''
        last = ret[-1]
        ret.append(doc)
        if doc['_oid'] == last['_oid'] and doc['_start'] == last['_end']:
            last_items = set(last.items())
            if all(item in last_items or item[0] in no_check
                   for item in doc.iteritems()):
                # the fields of interest did not change, merge docs:
                last['_end'] = doc['_end']
                ret.pop()

    docs = _cube.find(spec, fields, sort=[('_oid', 1), ('_start', 1)])
    [merge_doc(doc) for doc in docs]
    return ret[1:]


def _parse_oids(oids, delimeter=','):
    if isinstance(oids, basestring):
        oids = [s.strip() for s in oids.split(delimeter)]
    if type(oids) is not list:
        raise HTTPError(400, "ids expected to be a list")
    return oids


def deptree(owner, cube, field, oids, date, level):
    log_head(owner, cube, 'deptree', date)
    oids = _parse_oids(oids)
    _cube = get_collection(owner, cube)
    checked = set(oids)
    fringe = oids
    loop_k = 0

    while len(fringe) > 0:
        if level and loop_k == abs(level):
            break
        spec = pql.find('_oid in %s and %s != None' % (fringe, field) +
                        _get_date_pql_string(date))
        deps = _cube.find(spec, ['_oid', field])
        fringe = set([oid for doc in deps for oid in doc[field]])
        fringe = filter(lambda oid: oid not in checked, fringe)
        checked |= set(fringe)
        loop_k += 1
    return sorted(checked)


def fetch(owner, cube, fields=None, date=None,
          sort=None, skip=0, limit=0, oids=None):
    log_head(owner, cube, 'fetch', date)
    if oids is None:
        oids = []
    sort = _check_sort(sort)
    _cube = get_collection(owner, cube)
    fields = get_fields(cube, fields)

    spec = {'_oid': {'$in': _parse_oids(oids)}} if oids else {}
    dt_str = _get_date_pql_string(date, '')
    if dt_str:
        spec.update(pql.find(dt_str))

    result = _cube.find(spec, fields, sort=sort, skip=skip, limit=limit)
    result.batch_size(BATCH_SIZE)
    result = tuple(result)
    return result


def sample(owner, cube, size, fields, date):
    log_head(owner, cube, 'sample', date)
    _cube = get_collection(owner, cube)
    fields = get_fields(cube, fields)
    dt_str = _get_date_pql_string(date, '')
    spec = pql.find(dt_str) if dt_str else {}
    cursor = _cube.find(spec, fields)
    n = cursor.count()
    if n <= size:
        cursor.batch_size(BATCH_SIZE)
        ret = tuple(cursor)
    else:
        to_sample = set(random.sample(range(n), size))
        ret = []
        for i in range(n):
            ret.append(cursor.next()) if i in to_sample else cursor.next()
        # Alternative approach would be to use
        # ret = [cursor[i] for i in to_sample]
        # but that would be much slower when size is larger
    return ret
