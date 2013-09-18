#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import pql
import random
from tornado.web import authenticated

from metriqued.utils import ifind, parse_pql_query, log_head
from metriqued.core_api import MetriqueHdlr

from metriqueu.utils import set_default


class AggregateHdlr(MetriqueHdlr):
    '''
    RequestHandler for running mongodb aggregation
    framwork pipeines against a given cube
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        pipeline = self.get_argument('pipeline')
        if not pipeline:
            # alias for pipeline
            pipeline = self.get_argument('query', '[]')
        result = self.aggregate(owner=owner, cube=cube,
                                pipeline=pipeline)
        self.write(result)

    def aggregate(self, owner, cube, pipeline):
        log_head(owner, cube, 'aggregate', pipeline)
        return self.timeline(owner, cube).aggregate(pipeline)


class CountHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back simple integer
    counts of objects matching the given query
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        query = self.get_argument('query')
        date = self.get_argument('date')
        result = self.count(owner=owner, cube=cube,
                            query=query, date=date)
        self.write(result)

    def count(self, owner, cube, query, date=None):
        if not query:
            query = '_oid == exists(True)'
        log_head(owner, cube, 'count', query, date)
        try:
            spec = pql.find(query + self.get_date_pql_string(date))
        except Exception as e:
            raise ValueError("Invalid Query (%s)" % str(e))

        logger.debug('Mongo Query: %s' % spec)

        docs = self.timeline(owner, cube).find(spec)
        result = docs.count() if docs else 0
        docs.close()
        return result


class DeptreeHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back the list of
    oids matching the given tree.
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        field = self.get_argument('field')
        oids = self.get_argument('oids')
        date = self.get_argument('date')
        level = self.get_argument('level')
        result = self.deptree(owner=owner, cube=cube,
                              field=field, oids=oids,
                              date=date, level=level)
        self.write(result)

    def deptree(self, owner, cube, field, oids, date, level):
        log_head(owner, cube, 'deptree', date)
        oids = self.parse_oids(oids)
        checked = set(oids)
        fringe = oids
        loop_k = 0
        pql_date = self.get_date_pql_string(date)
        while len(fringe) > 0:
            if level and loop_k == abs(level):
                break
            spec = pql.find(
                '_oid in %s and %s != None' % (fringe, field) + pql_date)
            deps = self.timeline(owner, cube).find(spec, ['_oid', field])
            fringe = set([oid for doc in deps for oid in doc[field]])
            fringe = filter(lambda oid: oid not in checked, fringe)
            checked |= set(fringe)
            loop_k += 1
        return sorted(checked)


class DistinctHdlr(MetriqueHdlr):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        field = self.get_argument('field')
        result = self.distinct(owner=owner, cube=cube,
                               field=field)
        self.write(result)

    def distinct(self, owner, cube, field):
        log_head(owner, cube, 'distinct', field)
        return self.timeline(owner, cube).distinct(field)


class FetchHdlr(MetriqueHdlr):
    ''' RequestHandler for fetching lumps of cube data '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        sort = self.get_argument('sort', None)
        skip = self.get_argument('skip', 0)
        limit = self.get_argument('limit', 0)
        oids = self.get_argument('oids', [])
        result = self.fetch(owner=owner, cube=cube,
                            fields=fields, date=date,
                            sort=sort, skip=skip,
                            limit=limit, oids=oids)
        self.write(result)

    def fetch(self, owner, cube, fields=None, date=None,
              sort=None, skip=0, limit=0, oids=None):
        log_head(owner, cube, 'fetch', date)
        if oids is None:
            oids = []
        sort = self.check_sort(sort, son=True)

        fields = self.get_fields(owner, cube, fields)

        # b/c there are __special_property__ objects
        # in every collection, we must filter them out
        # checking for _oid should suffice
        base_spec = {'_oid': {'$exists': 1}}

        spec = {'_oid': {'$in': self.parse_oids(oids)}} if oids else {}
        dt_str = self.get_date_pql_string(date, '')
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

        result = self.timeline(owner, cube).aggregate(pipeline)['result']
        return result


class FindHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back object
    matching the given query
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        query = self.get_argument('query')
        fields = self.get_argument('fields', '')
        date = self.get_argument('date')
        sort = self.get_argument('sort', None)
        one = self.get_argument('one', False)
        explain = self.get_argument('explain', False)
        merge_versions = self.get_argument('merge_versions', True)
        result = self.find(owner=owner, cube=cube,
                           query=query, fields=fields,
                           date=date, sort=sort,
                           one=one, explain=explain,
                           merge_versions=merge_versions)
        self.write(result)

    def find(self, owner, cube, query, fields=None, date=None,
             sort=None, one=False, explain=False, merge_versions=True):
        if not query:
            query = '_oid == exists(True)'
        log_head(owner, cube, 'find', query, date)
        sort = self.check_sort(sort)

        fields = self.get_fields(owner, cube, fields)
        if date is None or ('_id' in fields and fields['_id']):
            merge_versions = False

        query += self.get_date_pql_string(date)

        spec = parse_pql_query(query)

        _cube = self.timeline(owner, cube)
        if explain:
            result = _cube.find(spec, fields, sort=sort).explain()
        elif one:
            result = _cube.find_one(spec, fields, sort=sort)
        elif merge_versions:
            # merge_versions ignores sort (for now)
            result = self._merge_versions(_cube, spec, fields)
        else:
            result = _cube.find(spec, fields, sort=sort)
            result = tuple(result)
        return result

    @staticmethod
    def _merge_versions(self, _cube, spec, fields):
        '''
        merge versions with unchanging fields of interest
        '''
        logger.debug("Merging docs...")
        # contains a dummy document to avoid some condition
        # checks in merge_doc
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


class SampleHdlr(MetriqueHdlr):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        sample_size = self.get_argument('sample_size')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        query = self.get_argument('date')
        result = self.sample(owner=owner, cube=cube,
                             sample_size=sample_size,
                             fields=fields, date=date,
                             query=query)
        self.write(result)

    def sample(self, owner, cube, sample_size=None, fields=None,
               date=None, query=None):
        fields = self.get_fields(owner, cube, fields)
        dt_str = self.get_date_pql_string(date, '')
        # for example, 'doc_version == 1.0'
        query = set_default(query, '', null_ok=True)
        if query:
            query = ' and '.join((dt_str, query))
        spec = parse_pql_query(query)
        _cube = self.timeline(owner, cube)
        _docs = ifind(_cube=_cube, spec=spec, fields=fields)
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
            # but ran only a small sample of tests ... ;)
            # key, perhaps, is that it's sorted
            to_sample = sorted(set(random.sample(xrange(n), sample_size)))
            docs = [_docs[i] for i in to_sample]
        return docs
