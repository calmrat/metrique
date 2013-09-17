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
from metrique.utils import api_owner_cube

DEFAULT_SAMPLE_SIZE = 1


@api_owner_cube
def aggregate(self, pipeline, **kwargs):
    '''
    Proxy for pymongodb's .aggregate framework call
    on a given cube

    :param list pipeline: The aggregation pipeline. $match, $project, etc.
    '''
    result = self._get(kwargs.get('cmd'), pipeline=pipeline)
    try:
        return result['result']
    except Exception:
        raise RuntimeError(result)


@api_owner_cube
def count(self, query=None, date=None, **kwargs):
    '''
    Run a `pql` based query on the given cube, but
    only return back the count (Integer)

    :param string query: The query in pql
    :param string date: Date (date range) that should be queried
    '''
    if not query:
        query = '_oid == exists(True)'
    return self._get(kwargs.get('cmd'), query=query, date=date)


@api_owner_cube
def find(self, query, fields=None, date=None, sort=None, one=False,
         raw=False, explain=False, merge_versions=True, **kwargs):
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
    '''
    result = self._get(kwargs.get('cmd'), query=query, fields=fields,
                       date=date, sort=sort, one=one,
                       explain=explain,
                       merge_versions=merge_versions)
    return result if raw or explain else Result(result, date)


@api_owner_cube
def deptree(self, field, oids, date=None, level=None, **kwargs):
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
    result = self._get(kwargs.get('cmd'), field=field,
                       oids=oids, date=date, level=level)
    return sorted(result)


@api_owner_cube
def fetch(self, fields=None, date=None, sort=None, skip=0, limit=0,
          oids=None, raw=False, **kwargs):
    '''
    Fetch field values for (potentially) all objects
    of a given, with skip, limit, id "filter" arguments

    :param fields: Fields that should be returned
    :type fields: str, or list of str, or str of comma-separated values
    :param string date:
        Date (date range) that should be queried.
        If date==None then the most recent versions of the objects
        will be queried.
    :param tuple sort: pymongo formated sort tuple
    :param Integer skip:
        number of items (sorted ASC) to skip
    :param Integer limit:
        number of items total to return, given skip
    :param List oids:
        specific list of oids we should fetch
    :param boolean raw: return the documents in their (dict) form
    '''
    result = self._get(kwargs.get('cmd'), fields=fields, date=date, sort=sort,
                       skip=skip, limit=limit, oids=oids)
    return result if raw else Result(result, date)


@api_owner_cube
def distinct(self, field, cube=None, owner=None, **kwargs):
    '''
    Return back all distinct token values of a given field

    :param string field:
        Field to get distinct token values from
    '''
    result = self._get(kwargs.get('cmd'), field=field)
    return sorted(result)


@api_owner_cube
def sample(self, sample_size=DEFAULT_SAMPLE_SIZE, fields=None,
           date=None, raw=False, query=None, **kwargs):
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
    result = self._get(kwargs.get('cmd'), sample_size=sample_size,
                       fields=fields, query=query, date=date)
    return result if raw else Result(result, date)
