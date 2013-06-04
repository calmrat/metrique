#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import simplejson as json
import os
from dateutil.parser import parse as dt_parse

from pandas import DataFrame, Series
import pandas as pd


class Result(DataFrame):
    def __init__(self, data=None):
        super(Result, self).__init__(data)
        self._result_data = data
        if '_start' in self:
            self._start = pd.to_datetime(self._start)
        if '_end' in self:
            self._end = pd.to_datetime(self._end)

    @classmethod
    def from_result_file(cls, path):
        path = os.path.expanduser(path)
        with open(path) as f:
            data = json.load(f)
            return Result(data)

    def to_result_file(self, path):
        path = os.path.expanduser(path)
        with open(path, 'w') as f:
            json.dump(self._result_data, f)

    def on_date(self, date):
        '''
        Filters out only the rows that match the spectified date.
        Works only on a Result that has _start and _end columns.

        Parameters
        ----------
        date : str
        '''
        if isinstance(date, basestring):
            date = dt_parse(date)
        after_start = self._start <= date
        before_end = (self._end >= date) | self._end.isnull()
        return self[before_end & after_start]

    def historical_counts(self):
        '''
        Works only on a Result that has _start and _end columns.
        most_recent=False should be set for this to work
        '''
        start_dts = list(self._start[~self._start.isnull()].values)
        end_dts = list(self._end[~self._end.isnull()].values)
        dts = set(start_dts + end_dts)
        idx, vals = [], []
        for dt in dts:
            idx.append(dt)
            vals.append(len(self.on_date(dt)))
        ret = Series(vals, index=idx)
        return ret.sort_index()
