#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique Query" related funtions '''

import logging
logger = logging.getLogger(__name__)

from metrique.result import Result

CMD = 'query'


def aggregate(self, pipeline, cube=None):
    '''
    Proxy for pymongodb's .aggregate framework call
    on a given cube

    :param list pipeline: The aggregation pipeline. $match, $project, etc.
    :param string: cube name to use
    '''
    if not cube and self.name:
        cube = self.name
    result = self._get(CMD, 'aggregate', cube=cube, pipeline=pipeline)
    try:
        return result['result']
    except Exception:
        raise RuntimeError(result)


def count(self, query, cube=None, date=None):
    '''
    Run a `pql` based query on the given cube, but
    only return back the count (Integer)

    :param String query: The query in pql

    #### COMING SOON - 0.1.4 ####
    :param String date: Date (date range) that should be queried:
            date -> 'd', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
    :param Boolean most_recent:
        If true and there are multiple historical version of a single
        object matching the query then only the most recent one will
        be returned
    :param string: cube name to use
    '''
    if not cube:
        cube = self.name
    return self._get(CMD, 'count', cube=cube, query=query, date=date)


def find(self, query, fields=None, date=None, sort=None, one=False,
         raw=False, explain=False, cube=None, **kwargs):
    '''
    Run a `pql` based query on the given cube.
    Optionally:
    * return back accompanying field meta data for
    * query again arbitrary datetimes in the past, if the
    * return back only the most recent date objects which
        match any given query, rather than all.

    :param string query:
        The query in pql
    :param list/string fields:
        Fields that should be returned
    :type fields: str, or list of str, or str of comma-separated values
    :param string date:
        Date (date range) that should be queried:
            date -> 'd', '~', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
        If date==None then the most recent versions of the objects will be
        queried.
    :param bool explain:
        If explain is True, the execution plan is returned instead of
        the results (in raw form)
    :param string: cube name to use
    '''
    if not cube:
        cube = self.name
    result = self._get(CMD, 'find', cube=cube, query=query,
                       fields=fields, date=date, sort=sort, one=one,
                       explain=explain)
    if raw or explain:
        return result
    else:
        if hasattr(self, '_result_class') and self._result_class is not None:
            result = self._result_class(result, **kwargs)
        else:
            result = Result(result)

        # this lets the result object know which dates were queried,
        # so that it can set its bounds.
        result.set_date_bounds(date)

        return result


def fetch(self, fields=None, date=None, sort=None, skip=0, limit=0, oids=None,
          raw=False, cube=None, **kwargs):
    '''
    Fetch field values for (potentially) all objects
    of a given, with skip, limit, id "filter" arguments

    :param fields: Fields that should be returned
    :type fields: str, or list of str, or str of comma-separated values
    :param String date:
        Date (date range) that should be queried:
            date -> 'd', '~', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
    :param tuple sort: pymongo formated sort tuple
    :param Integer skip:
        number of items (sorted ASC) to skip
    :param Integer limit:
        number of items total to return, given skip
    :param List oids:
        specific list of oids we should fetch
    :param boolean raw: return the documents in their (dict) form
    :param string: cube name to use
    '''
    if not cube:
        cube = self.name
    result = self._get(CMD, 'fetch', cube=cube, fields=fields,
                       date=date, sort=sort, skip=skip, limit=limit,
                       oids=oids)
    if raw:
        return result
    else:
        if hasattr(self, '_result_class') and self._result_class is not None:
            return self._result_class(result, **kwargs)
        else:
            return Result(result)


def distinct(self, field, cube=None, sort=True):
    '''
    Return back all distinct token values of a given field

    :param String field:
        Field to get distinct token values from
    :param string: cube name to use
    '''
    if not cube:
        cube = self.name
    result = self._get(CMD, 'distinct', cube=cube, field=field)
    if sort:
        return sorted(result)
    else:
        return result
