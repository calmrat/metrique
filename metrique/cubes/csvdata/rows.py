#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.cubes.csvdata.rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the generic metrique cube used
for exctacting data from CSV.
'''

from __future__ import unicode_literals, absolute_import

import itertools
import logging

from metrique import pyclient
from metrique.utils import utcnow, load

logger = logging.getLogger('metrique')


class Rows(pyclient):
    """
    Object used for extracting data from CSV files.

    Esentially, a wrapper for pandas.read_csv method.

    It's possible to load from a http(s):// or file://.

    The field names are defined by the column headers,
    therefore column heders are required in the csv.
    """
    name = 'csvdata_rows'

    def get_objects(self, uri, _oid=None, _start=None, _end=None,
                    load_kwargs=None, **kwargs):
        '''
        Load and transform csv data into a list of dictionaries.

        Each row in the csv will result in one dictionary in the list.

        :param uri: uri (file://, http(s)://) of csv file to load
        :param _oid:
            column or func to apply to map _oid in all resulting objects
        :param _start:
            column or func to apply to map _start in all resulting objects
        :param _end:
            column or func to apply to map _end in all resulting objects
        :param kwargs: kwargs to pass to pandas.read_csv method

        _start and _oid arguments can be a column name or a function
        which accepts a single argument -- the row being extracted.

        If either is a column name (string) then that column will be applied
        as _oid for each object generated.

        If either is a function, the function will be applied per each row
        and the result of the function will be assigned to the _start
        or _oid, respectively.
        '''
        load_kwargs = load_kwargs or {}
        objects = load(path=uri, filetype='csv', **load_kwargs)

        k = itertools.count(1)
        now = utcnow()
        __oid = lambda o: k.next()
        __start = lambda o: now
        __end = lambda o: None

        _oid = _oid or __oid
        _start = _start or __start
        _end = _end or __end

        for v in (_oid, _start, _end):
            if v is None or not (type(v) is type or hasattr(v, '__call__')):
                raise ValueError(
                    "(_oid, _start, _end) must be a callables!" % v)

        for obj in objects:
            obj['_oid'] = _oid(obj)
            obj['_end'] = _end(obj)
            obj['_start'] = _start(obj)
            self.objects.add(obj)

        return super(Rows, self).get_objects(**kwargs)
