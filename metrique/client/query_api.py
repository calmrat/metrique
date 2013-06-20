#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique Query" related funtions '''

import logging
logger = logging.getLogger(__name__)

from metrique.client.result import Result

CMD = 'query'


def aggregate(self, pipeline):
    '''
    Proxy for pymongodb's .aggregate framework call
    on a given cube

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    pipeline : list
        The aggregation pipeline. $match, $project, etc.
    '''
    result = self._get(CMD, 'aggregate', cube=self.name, pipeline=pipeline)
    try:
        return result['result']
    except Exception:
        raise RuntimeError(result)


def count(self, query):
    '''
    Run a `pql` based query on the given cube, but
    only return back the count (int)

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    query : str
        The query in pql
    #### COMING SOON - 0.1.4 ####
    date : str
        Date (date range) that should be queried:
            date -> 'd', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
    most_recent : boolean
        If true and there are multiple historical version of a single
        object matching the query then only the most recent one will
        be returned
    '''
    return self._get(CMD, 'count', cube=self.name, query=query)


def find(self, query, fields=None, date=None, most_recent=False,
         sort=None, one=False, raw=False):
    '''
    Run a `pql` based query on the given cube.
    Optionally:
    * return back accompanying field meta data for
    * query again arbitrary datetimes in the past, if the
    * return back only the most recent date objects which
        match any given query, rather than all.

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    query : str
        The query in pql
    fields : str, or list of str, or str of comma-separated values
        Fields that should be returned
    date : str
        Date (date range) that should be queried:
            date -> 'd', '~', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
    most_recent : boolean
        If true and there are multiple historical version of a single
        object matching the query then only the most recent one will
        be returned
    '''
    result = self._get(CMD, 'find', cube=self.name, query=query,
                       fields=fields, date=date, most_recent=most_recent,
                       sort=sort, one=one)
    if raw:
        return result
    else:
        result = Result(result)
        result.date(date)
        return result


def fetch(self, fields=None, date=None, sort=None, skip=0, limit=0, ids=[],
          raw=False):
    '''
    Fetch field values for (potentially) all objects
    of a given, with skip, limit, id "filter" arguments

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    fields : str, or list of str, or str of comma-separated values
        Fields that should be returned
    date : str
        Date (date range) that should be queried:
            date -> 'd', '~', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
    skip : int
        number of items (sorted ASC) to skip
    limit : int
        number of items total to return, given skip
    ids : list
        specific list of ids we should fetch
    '''
    result = self._get(CMD, 'fetch', cube=self.name, fields=fields,
                       date=date, sort=sort, skip=skip, limit=limit,
                       ids=ids)
    if raw:
        return result
    else:
        return Result(result)


def distinct(self, field):
    '''
    Return back all distinct token values of a given field

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    field : str
        Field to get distinct token values from
    '''
    return self._get(CMD, 'distinct', cube=self.name, field=field)
