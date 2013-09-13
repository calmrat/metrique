#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from bson.son import SON
import logging
logger = logging.getLogger(__name__)
import pql
import re
import random

from metriqued.cube_api import get_fields, get_collection
from metriqued.utils import cfind, parse_pql_query

from metriqueu.utils import dt2ts, set_default


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
        raise ValueError("Invalid Query (%s)" % str(e))

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


def _check_sort(sort, son=False):
    '''
    son True is required for pymongo's aggregation $sort operator
    '''
    if not sort:
        sort = [('_oid', 1)]

    try:
        assert len(sort[0]) == 2
    except (AssertionError, IndexError, TypeError):
        raise ValueError("Invalid sort value; try [('_id': -1)]")
    if son:
        return SON(sort)
    else:
        return sort


def find(owner, cube, query, fields=None, date=None, sort=None, one=False,
         explain=False, merge_versions=True):
    log_head(owner, cube, 'find', query, date)
    sort = _check_sort(sort)

    fields = get_fields(owner, cube, fields)
    if date is None or ('_id' in fields and fields['_id']):
        merge_versions = False

    query += _get_date_pql_string(date)

    spec = parse_pql_query(query)

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
        raise TypeError("ids expected to be a list")
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
    sort = _check_sort(sort, son=True)
    _cube = get_collection(owner, cube)
    fields = get_fields(owner, cube, fields)

    # b/c there are __special_property__ objects
    # in every collection, we must filter them out
    # checking for _oid should suffice
    base_spec = {'_oid': {'$exists': 1}}

    spec = {'_oid': {'$in': _parse_oids(oids)}} if oids else {}
    dt_str = _get_date_pql_string(date, '')
    if dt_str:
        spec.update(pql.find(dt_str))

    pipeline = [
        {'$match': base_spec},
        {'$match': spec},
        {'$skip': skip},
        {'$project': fields},
        {'$sort': sort},
    ]
    if limit:
        pipeline.append({'$limit': limit})

    result = _cube.aggregate(pipeline)['result']
    return result


def sample(owner, cube, sample_size=None, fields=None,
           date=None, query=None):
    # FIXME: OT: at some point in the future...
    # make 'sample' arg, the first arg
    # log_head(owner, cube, 'sample', date)
    # is more obvious to read like it's output format... like
    # log_head('sample', owner, cube, date)
    _cube = get_collection(owner, cube)
    fields = get_fields(owner, cube, fields)
    dt_str = _get_date_pql_string(date, '')
    # for example, 'doc_version == 1.0'
    query = set_default(query, '', null_ok=True)
    if query:
        query = ' and '.join((dt_str, query))
    spec = parse_pql_query(query)
    _docs = cfind(_cube=_cube, spec=spec, fields=fields)
    n = _docs.count()
    if n <= sample_size:
        docs = tuple(_docs)
    else:
        # testing multiple approaches, on a collection with 1100001 objs
        # In [27]: c.cube_stats(cube='test')
        # {'cube': 'test', 'mtime': 1379078573, 'size': 1100001}

        # this approach has results like these:
        # >>>  %time c.query_sample(cube='test')
        # CPU times: user 7 ms, sys: 0 ns, total: 7 ms
        # Wall time: 7.7 s
        #to_sample = set(random.sample(xrange(n), sample_size))
        #docs = []
        #for i in xrange(n):
        #    docs.append(_docs.next()) if i in to_sample else _docs.next()

        # this approach has results like these:
        # >>>  %time c.query_sample(cube='test')
        # CPU times: user 7 ms, sys: 0 ns, total: 7 ms
        # Wall time: 1.35 s
        # Out[20]:
        #   _end    _oid              _start
        # 0  NaT  916132 2013-09-13 13:22:53
        # note: i saw it go up as high as 3.5 seconds
        # but small sample of tests ;)
        to_sample = sorted(set(random.sample(xrange(n), sample_size)))
        docs = [_docs[i] for i in to_sample]
    return docs
