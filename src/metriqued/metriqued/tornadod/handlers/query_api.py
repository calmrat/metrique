#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from tornado.web import authenticated

from metriqued.tornadod.handlers.core_api import MetriqueHdlr
from metriqued import query_api


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
        result = query_api.find(owner=owner,
                                cube=cube,
                                query=query,
                                fields=fields,
                                date=date,
                                sort=sort,
                                one=one,
                                explain=explain,
                                merge_versions=merge_versions)
        self.write(result)


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
        result = query_api.aggregate(owner=owner, cube=cube,
                                     pipeline=pipeline)
        self.write(result)


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
        result = query_api.fetch(owner=owner, cube=cube,
                                 fields=fields, date=date,
                                 sort=sort, skip=skip,
                                 limit=limit, oids=oids)
        self.write(result)


class CountHdlr(MetriqueHdlr):
    '''
    RequestHandler for returning back simple integer
    counts of objects matching the given query
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        query = self.get_argument('query')
        date = self.get_argument('date', None)
        result = query_api.count(owner=owner, cube=cube,
                                 query=query, date=date)
        self.write(result)


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
        result = query_api.deptree(owner=owner, cube=cube,
                                   field=field, oids=oids,
                                   date=date, level=level)
        self.write(result)


class DistinctHdlr(MetriqueHdlr):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        field = self.get_argument('field')
        result = query_api.distinct(owner=owner, cube=cube,
                                    field=field)
        self.write(result)


class SampleHdlr(MetriqueHdlr):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        size = self.get_argument('size')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        result = query_api.sample(owner=owner, cube=cube,
                                  size=size, fields=fields,
                                  date=date)
        self.write(result)
