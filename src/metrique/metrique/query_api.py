#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
This module contains all the query and aggregation
related api functionality.
'''

import logging
logger = logging.getLogger(__name__)

from metrique.result import Result

SAMPLE_SIZE = 1


def aggregate(self, pipeline, cube=None, owner=None):
    '''
    Proxy for pymongodb's .aggregate framework call
    on a given cube

    :param list pipeline: The aggregation pipeline. $match, $project, etc.
    '''
    cmd = self.get_cmd(owner, cube, 'aggregate')
    result = self._get(cmd, pipeline=pipeline)
    return result['result']


def count(self, query=None, date=None, cube=None, owner=None):
    '''
    Run a `pql` based query on the given cube, but
    only return back the count (Integer)

    :param string query: The query in pql
    :param string date: Date (date range) that should be queried
    '''
    cmd = self.get_cmd(owner, cube, 'count')
    return self._get(cmd, query=query, date=date)


def find(self, query=None, fields=None, date=None, sort=None, one=False,
         raw=False, explain=False, merge_versions=True, skip=0,
         limit=0, cube=None, owner=None):
    '''
    Run a `pql` based query on the given cube. Optionally:

    :param string query: The query in pql
    :param list/string fields: Fields that should be returned
    :type fields: str, or list of str, or str of comma-separated values
    :param string date:
        Date (date range) that should be queried.
        If date==None then the most recent versions of the objects
        will be queried.
    :param bool explain: return execution plan instead of results
    :param boolean merge_versions:
        merge versions with unchanging fields od interest
    :param string cube: name of cube to work with
    :param string owner: owner of cube
    '''
    cmd = self.get_cmd(owner, cube, 'find')
    result = self._get(cmd, query=query, fields=fields,
                       date=date, sort=sort, one=one, explain=explain,
                       merge_versions=merge_versions,
                       skip=skip, limit=limit)
    return result if raw or explain else Result(result, date)


def history(self, query, by_field=None, date_list=None, cube=None, owner=None):
    '''
    '''
    cmd = self.get_cmd(owner, cube, 'history')
    return self._get(cmd, query=query, by_field=by_field, date_list=date_list)


def deptree(self, field, oids, date=None, level=None, cube=None, owner=None):
    '''
    Dependency tree builder recursively fetchs objects that
    are children of the initial set of objects provided.

    :param string field: Field that contains the 'parent of' data
    :param list oids: Object oids to build depedency tree for
    :param string date:
        Date (date range) that should be queried.
        If date==None then the most recent versions of the objects
        will be queried.
    :param integer level: limit depth of recursion
    '''
    cmd = self.get_cmd(owner, cube, 'deptree')
    result = self._get(cmd, field=field,
                       oids=oids, date=date, level=level)
    return sorted(result)


def distinct(self, field, cube=None, owner=None):
    '''
    Return back all distinct token values of a given field

    :param string field:
        Field to get distinct token values from
    '''
    cmd = self.get_cmd(owner, cube, 'distinct')
    result = self._get(cmd, field=field)
    return sorted(result)


def sample(self, sample_size=SAMPLE_SIZE, fields=None,
           date=None, raw=False, query=None, cube=None, owner=None):
    '''
    Draws a sample of objects at random.

    :param integer sample_size: Size of the sample.
    :param fields: Fields that should be returned
    :param string date:
        Date (date range) that should be queried.
        If date==None then the most recent versions of the objects
        will be queried.
    :param boolean raw: if True, then return result as a dictionary
    '''
    cmd = self.get_cmd(owner, cube, 'sample')
    result = self._get(cmd, sample_size=sample_size,
                       fields=fields, query=query, date=date)
    return result if raw else Result(result, date)
