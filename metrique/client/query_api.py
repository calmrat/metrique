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

    :param list pipeline: The aggregation pipeline. $match, $project, etc.
    '''
    result = self._get(CMD, 'aggregate', cube=self.name, pipeline=pipeline)
    try:
        return result['result']
    except Exception:
        raise RuntimeError(result)


def count(self, query):
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
    '''
    return self._get(CMD, 'count', cube=self.name, query=query)


def find(self, query, fields=None, date=None, most_recent=False,
         sort=None, one=False, raw=False, **kwargs):
    '''
    Run a `pql` based query on the given cube.
    Optionally:
    * return back accompanying field meta data for
    * query again arbitrary datetimes in the past, if the
    * return back only the most recent date objects which
        match any given query, rather than all.

    :param String query: The query in pql
    :param fields: Fields that should be returne
    :type fields: str, or list of str, or str of comma-separated values
    :param String date:
        Date (date range) that should be queried:
            date -> 'd', '~', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
    :param Boolean most_recent:
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
        if hasattr(self, '_result_class') and self._result_class is not None:
            result = self._result_class(result, **kwargs)
        else:
            result = Result(result)

        # FIXME: What's happening here?
        result.date(date)

        return result


def fetch(self, fields=None, date=None, sort=None, skip=0, limit=0, ids=[],
          raw=False, **kwargs):
    '''
    Fetch field values for (potentially) all objects
    of a given, with skip, limit, id "filter" arguments

    :param fields: Fields that should be returned
    :type fields: str, or list of str, or str of comma-separated values
    :param String date:
        Date (date range) that should be queried:
            date -> 'd', '~', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
    :param Integer skip:
        number of items (sorted ASC) to skip
    :param Integer limit:
        number of items total to return, given skip
    :param List ids:
        specific list of ids we should fetch
    '''
    result = self._get(CMD, 'fetch', cube=self.name, fields=fields,
                       date=date, sort=sort, skip=skip, limit=limit,
                       ids=ids)
    if raw:
        return result
    else:
        if hasattr(self, '_result_class') and self._result_class is not None:
            return self._result_class(result, **kwargs)
        else:
            return Result(result)


def distinct(self, field):
    '''
    Return back all distinct token values of a given field

    :param String field:
        Field to get distinct token values from
    '''
    return self._get(CMD, 'distinct', cube=self.name, field=field)
