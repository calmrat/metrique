#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import simplejson as json
import os

from pandas import DataFrame, Series
import pandas.tseries.offsets as off
from pandas.tslib import Timestamp
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

    def on_date(self, date, only_count=False):
        '''
        Filters out only the rows that match the spectified date.
        Works only on a Result that has _start and _end columns.

        Parameters
        ----------
        date : str
        '''
        date = Timestamp(date)
        after_start = self._start <= date
        before_end = (self._end > date) | self._end.isnull()
        if only_count:
            return sum(before_end & after_start)
        else:
            return self[before_end & after_start]

    def history(self, scale='maximum', counts=True, start=None, end=None):
        '''
        Works only on a Result that has _start and _end columns.
        most_recent=False should be set for this to work

        Parameters
        ----------
        scale: {'maximum', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'}
            Scale specifies the sampling intervals.
        counts: boolean
            If True counts will be returned
            If False ids will be returned
        start: str
            First date that will be included.
        end: str
            Last date that will be included
        '''
        start_dts = list(self._start[~self._start.isnull()].values)
        end_dts = list(self._end[~self._end.isnull()].values)
        dts = set(start_dts + end_dts)
        if scale == 'maximum':
            if start is not None:
                start = Timestamp(start)
                dts = filter(lambda ts: ts >= start, dts)
            if end is not None:
                end = Timestamp(end)
                dts = filter(lambda ts: ts <= end, dts)
        else:
            start = min(dts) if start is None else start
            end = max(dts) if end is None else end
            dts = self.get_date_ranges(start, end, scale)
        idx, vals = [], []
        for dt in dts:
            idx.append(dt)
            if counts:
                vals.append(self.on_date(dt, only_count=True))
            else:
                vals.append(list(self.on_date(dt)._id))
        ret = Series(vals, index=idx)
        return ret.sort_index()

    def get_date_ranges(self, start, end, scale='daily', include_bounds=True):
        '''
        Returns a list of dates sampled according to the specified parameters.

        Parameters
        ----------
        start: str
            First date that will be included.
        end: str
            Last date that will be included
        scale: {'daily', 'weekly', 'monthly', 'quarterly', 'yearly'}
            Scale specifies the sampling intervals.
        include_bounds: boolean
            Include start and end in the result if they are not included yet.
        '''
        if scale not in ['daily', 'weekly', 'monthly', 'quarterly', 'yearly']:
            raise ValueError('Incorrect scale: %s' % scale)
        start = Timestamp(start)
        end = Timestamp(end)
        freq = dict(weekly='W', monthly='M', quarterly='3M', yearly='12M')
        offset = dict(weekly=off.Week(), monthly=off.MonthEnd(),
                      quarterly=off.QuarterEnd(), yearly=off.YearEnd())
        if scale == 'daily':
            ret = pd.date_range(start, end, freq='D')
        else:
            ret = pd.date_range(start + offset[scale], end, freq=freq[scale])
        ret = list(ret)
        if include_bounds:
            if start not in ret:
                ret = [start] + ret
            if end not in ret:
                ret = ret + [end]
        return ret
