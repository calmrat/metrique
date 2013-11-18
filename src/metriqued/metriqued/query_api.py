#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from operator import itemgetter
import pql
import random
from tornado.web import authenticated
from collections import defaultdict

from metriqued.utils import parse_pql_query, parse_oids
from metriqued.utils import date_pql_string, query_add_date
from metriqued.core_api import MetriqueHdlr

from metriqueu.utils import set_default, dt2ts


class AggregateHdlr(MetriqueHdlr):
    '''
    RequestHandler for running mongodb aggregation
    framwork pipeines against a given cube
    '''
    @authenticated
    def get(self, owner, cube):
        pipeline = self.get_argument('pipeline')
        result = self.aggregate(owner=owner, cube=cube, pipeline=pipeline)
        self.write(result)

    def aggregate(self, owner, cube, pipeline):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        pipeline = set_default(pipeline, None, null_ok=False)
        return self.timeline(owner, cube).aggregate(pipeline)


class CountHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back simple integer
    counts of objects matching the given query
    '''
    @authenticated
    def get(self, owner, cube):
        query = self.get_argument('query')
        date = self.get_argument('date')
        result = self.count(owner=owner, cube=cube,
                            query=query, date=date)
        self.write(result)

    def count(self, owner, cube, query, date=None):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)

        query = query or ''
        query = query_add_date(query, date)
        self.logger.info('pql query: %s' % query)
        try:
            spec = parse_pql_query(query)
        except Exception as e:
            self._raise(400, "Invalid Query (%s)" % str(e))
        _cube = self.timeline(owner, cube)
        docs = _cube.find(spec=spec)
        return docs.count() if docs else 0


class DeptreeHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back the list of
    oids matching the given tree.
    '''
    @authenticated
    def get(self, owner, cube):
        field = self.get_argument('field')
        oids = self.get_argument('oids')
        date = self.get_argument('date')
        level = self.get_argument('level')
        result = self.deptree(owner=owner, cube=cube,
                              field=field, oids=oids,
                              date=date, level=level)
        self.write(result)

    def deptree(self, owner, cube, field, oids, date, level):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        if level and level <= 0:
            self._raise(400, 'level must be >= 1')
        oids = parse_oids(oids)
        checked = set(oids)
        fringe = oids
        loop_k = 0
        pql_date = date_pql_string(date)
        while len(fringe) > 0:
            if level and loop_k == abs(level):
                break
            spec = pql.find(
                '_oid in %s and %s != None and %s' % (fringe, field, pql_date))
            _cube = self.timeline(owner, cube)
            fields = {'_id': -1, '_oid': 1, field: 1}
            docs = _cube.find(spec, fields=fields)
            fringe = set([oid for doc in docs for oid in doc[field]])
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
        field = self.get_argument('field')
        result = self.distinct(owner=owner, cube=cube, field=field)
        self.write(result)

    def distinct(self, owner, cube, field):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        return self.timeline(owner, cube).distinct(field)


class FindHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back object
    matching the given query
    '''
    @authenticated
    def get(self, owner, cube):
        query = self.get_argument('query')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        sort = self.get_argument('sort')
        one = self.get_argument('one')
        explain = self.get_argument('explain')
        merge_versions = self.get_argument('merge_versions', True)
        skip = self.get_argument('skip')
        limit = self.get_argument('limit')
        result = self.find(owner=owner, cube=cube,
                           query=query, fields=fields,
                           date=date, sort=sort,
                           one=one, explain=explain,
                           merge_versions=merge_versions,
                           skip=skip, limit=limit)
        self.write(result)

    def find(self, owner, cube, query, fields=None, date=None,
             sort=None, one=False, explain=False, merge_versions=True,
             skip=0, limit=0):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)

        sort = self.check_sort(sort)
        fields = self.get_fields(owner, cube, fields)

        if date is None or fields is None or ('_id' in fields and
                                              fields['_id']):
            merge_versions = False

        query = query or ''
        query = query_add_date(query, date)
        self.logger.info('pql query: %s' % query)
        try:
            spec = parse_pql_query(query)
        except Exception as e:
            self._raise(400, "Invalid Query (%s)" % str(e))

        _cube = self.timeline(owner, cube)
        if explain:
            result = _cube.find(spec, fields=fields, sort=sort,
                                skip=skip, limit=limit).explain()
        elif one:
            result = _cube.find_one(spec, fields=fields, sort=sort,
                                    skip=skip, limit=limit)
        elif merge_versions:
            # merge_versions ignores sort (for now)
            result = self._merge_versions(_cube, spec, fields,
                                          skip=skip, limit=limit)
        else:
            result = tuple(_cube.find(spec, fields=fields, sort=sort,
                                      skip=skip, limit=limit))
        return result

    def _merge_versions(self, _cube, spec, fields, skip=0, limit=0):
        '''
        merge versions with unchanging fields of interest
        '''
        self.logger.debug("merging doc version...")
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
                if all(item in last.items() or item[0] in no_check
                       for item in doc.iteritems()):
                    # the fields of interest did not change, merge docs:
                    last['_end'] = doc['_end']
                    ret.pop()

        sort = self.check_sort([('_oid', 1)])
        docs = _cube.find(spec, fields=fields, sort=sort,
                          skip=skip, limit=limit)
        docs = sorted(docs, key=itemgetter('_oid', '_start', '_end'))
        [merge_doc(doc) for doc in docs]
        self.logger.debug('... done')
        return ret[1:]


class HistoryHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back historical counts for
    the given query
    '''
    @authenticated
    def get(self, owner, cube):
        query = self.get_argument('query')
        by_field = self.get_argument('by_field')
        date_list = self.get_argument('date_list')
        result = self.history(owner=owner, cube=cube,
                              query=query, by_field=by_field,
                              date_list=date_list)
        self.write(result)

    def history(self, owner, cube, query, by_field=None, date_list=None):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)

        date_list = sorted(map(dt2ts, date_list))
        query = '%s and _start < %s and (_end >= %s or _end == None)' % (
                query, max(date_list), min(date_list))
        try:
            spec = parse_pql_query(query)
        except Exception as e:
            self._raise(400, "Invalid Query (%s)" % str(e))

        _cube = self.timeline(owner, cube)

        agg = [{'$match': spec},
               {'$group':
                {'_id': '$%s' % by_field if by_field else 'id',
                 'starts': {'$push': '$_start'},
                 'ends': {'$push': '$_end'}}
                }]
        self.logger.debug('Aggregation: %s' % agg)
        data = _cube.aggregate(agg)['result']

        # accumulate the counts
        res = defaultdict(lambda: defaultdict(int))
        for group in data:
            starts = sorted(group['starts'])
            ends = sorted([x for x in group['ends'] if x is not None])
            _id = group['_id']
            ind = 0
            # assuming date_list is sorted
            for date in date_list:
                while ind < len(starts) and starts[ind] < date:
                    ind += 1
                res[date][_id] = ind
            ind = 0
            for date in date_list:
                while ind < len(ends) and ends[ind] < date:
                    ind += 1
                res[date][_id] -= ind

        # convert to the return form
        ret = []
        for date, value in res.items():
            if by_field:
                vals = []
                for field_val, count in value.items():
                    vals.append({by_field: field_val,
                                 "count": count})
                ret.append({"date": date,
                            "values": vals})
            else:
                ret.append({"date": date,
                            "count": value['id']})
        return ret


class SampleHdlr(MetriqueHdlr):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self, owner, cube):
        sample_size = self.get_argument('sample_size')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        query = self.get_argument('query')
        result = self.sample(owner=owner, cube=cube,
                             sample_size=sample_size,
                             fields=fields, date=date,
                             query=query)
        self.write(result)

    def sample(self, owner, cube, sample_size=None, fields=None,
               date=None, query=None):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        fields = self.get_fields(owner, cube, fields)
        query = query_add_date(query, date)
        try:
            spec = parse_pql_query(query)
        except Exception as e:
            self._raise(400, "Invalid Query (%s)" % str(e))
        _cube = self.timeline(owner, cube)
        _docs = _cube.find(spec, fields=fields)
        n = _docs.count()
        if n <= sample_size:
            docs = tuple(_docs)
        else:
            to_sample = sorted(set(random.sample(xrange(n), sample_size)))
            docs = [_docs[i] for i in to_sample]
        return docs
