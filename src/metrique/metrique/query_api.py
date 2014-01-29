#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.query_api
~~~~~~~~~~~~~~~~~~

This module contains all the query and aggregation
related api functionality.
'''

from metrique.result import Result


def aggregate(self, pipeline, cube=None, owner=None):
    '''
    Run a pql mongodb aggregate pipeline on remote cube

    :param pipeline: The aggregation pipeline. $match, $project, etc.
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'aggregate')
    result = self._get(cmd, pipeline=pipeline)
    return result['result']


def count(self, query=None, date=None, cube=None, owner=None):
    '''
    Run a pql mongodb based query on the given cube and return only
    the count of resulting matches.

    :param query: The query in pql
    :param date: date (metrique date range) that should be queried
                 If date==None then the most recent versions of the
                 objects will be queried.
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'count')
    return self._get(cmd, query=query, date=date)


def find(self, query=None, fields=None, date=None, sort=None, one=False,
         raw=False, explain=False, merge_versions=True, skip=0,
         limit=0, cube=None, owner=None):
    '''
    Run a pql mongodb based query on the given cube.

    :param query: The query in pql
    :param fields: Fields that should be returned (comma-separated)
    :param date: date (metrique date range) that should be queried.
                 If date==None then the most recent versions of the
                 objects will be queried.
    :param explain: return execution plan instead of results
    :param merge_versions: merge versions where fields values equal
    :param one: return back only first matching object
    :param sort: return back results sorted
    :param raw: return back raw JSON results rather than pandas dataframe
    :param skip: number of results matched to skip and not return
    :param limit: number of results matched to return of total found
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'find')
    result = self._get(cmd, query=query, fields=fields,
                       date=date, sort=sort, one=one, explain=explain,
                       merge_versions=merge_versions,
                       skip=skip, limit=limit)
    return result if raw or explain else Result(result, date)


def history(self, query, by_field=None, date_list=None, cube=None, owner=None):
    '''
    Run a pql mongodb based query on the given cube and return back the
    aggregate historical counts of matching results.

    :param query: The query in pql
    :param by_field: Which field to slice/dice and aggregate from
    :param date: list of dates that should be used to bin the results
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'history')
    return self._get(cmd, query=query, by_field=by_field, date_list=date_list)


def deptree(self, field, oids, date=None, level=None, cube=None, owner=None):
    '''
    Dependency tree builder. Recursively fetchs objects that
    are children of the initial set of parent object ids provided.

    :param field: Field that contains the 'parent of' data
    :param oids: Object oids to build depedency tree for
    :param date: date (metrique date range) that should be queried.
                 If date==None then the most recent versions of the
                 objects will be queried.
    :param level: limit depth of recursion
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'deptree')
    result = self._get(cmd, field=field,
                       oids=oids, date=date, level=level)
    return sorted(result)


def distinct(self, field, cube=None, owner=None):
    '''
    Return back a distinct (unique) list of field values
    across the entire cube dataset

    :param field: field to get distinct token values from
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'distinct')
    result = self._get(cmd, field=field)
    return sorted(result)


def sample(self, sample_size=1, fields=None, date=None, raw=False,
           query=None, cube=None, owner=None):
    '''
    Draws a sample of objects at random from the cube.

    :param sample_size: Size of the sample
    :param fields: Fields that should be returned
    :param date: date (metrique date range) that should be queried
                       If date==None then the most recent versions of
                       the objects will be queried
    :param raw: if True, then return result as a dictionary
    :param query: query used to filter sampleset
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'sample')
    result = self._get(cmd, sample_size=sample_size,
                       fields=fields, query=query, date=date)
    return result if raw else Result(result, date)
