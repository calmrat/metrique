#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from decorator import decorator
import re
import simplejson as json
import os

from pandas import DataFrame, Series
import pandas.tseries.offsets as off
from pandas.tslib import Timestamp
import pandas as pd
import numpy as np


@staticmethod
def perc(numerator, denominator):
    return (float(numerator) / denominator) * 100


def mask_filter(f):
    '''
        Generic function for getting back filtered frame
        data according to True/False mask filter frame matching

        Parameters
        ----------
        mask_frame : Pandas.DataFrame
            DataFrame that maps index:True/False where True means it
            matches the filter and False means it does not
        filter_ : Boolean
            True will return back a DataFrame that contains only items
            which matched the mask_frame filter. False returns back the
            opposite.
    '''
    return decorator(_mask_filter, f)


def _mask_filter(f, self, *args, **kwargs):
    filter_ = args[-1]  # by convention, filter_ expected as last arg
    mask_frame = f(self, *args, **kwargs)
    if filter_ is None:
        return mask_frame
    else:
        return self[mask_frame == filter_]


class Result(DataFrame):
    ''' Custom DataFrame implementation for Metrique '''
    def __init__(self, data=None):
        super(Result, self).__init__(data)
        self._result_data = data
        # FIXME: Why isn't this already a datetime?
        # FIXME: if it is... don't convert unnecessarily
        if '_start' in self:
            self._start = pd.to_datetime(self._start)
        if '_end' in self:
            self._end = pd.to_datetime(self._end)
        self._lbound = self._rbound = None

    @classmethod
    def from_result_file(cls, path):
        ''' Load saved json data from file '''
        path = os.path.expanduser(path)
        with open(path) as f:
            data = json.load(f)
            return Result(data)

    def to_result_file(self, path):
        ''' Save json data to file '''
        path = os.path.expanduser(path)
        with open(path, 'w') as f:
            json.dump(self._result_data, f)

    def date(self, date):
        '''
        Pass in the date used in the original query.

        Parameters
        ----------
        date : str
            Date (date range) that was queried:
                date -> 'd', '~d', 'd~', 'd~d'
                d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
        '''
        if date is not None:
            split = date.split('~')
            if len(split) == 1:
                self._lbound = Timestamp(date)
                self._rbound = Timestamp(date)
            elif split[0] == '':
                self._rbound = Timestamp(split[1])
            elif split[1] == '':
                self._lbound = Timestamp(split[0])
            else:
                self._lbound = Timestamp(split[0])
                self._rbound = Timestamp(split[1])

    def check_in_bounds(self, date):
        ''' Check that left and right bounds are sane '''
        dt = Timestamp(date)
        return ((self._lbound is None or dt >= self._lbound) and
                (self._rbound is None or dt <= self._rbound))

    def on_date(self, date, only_count=False):
        '''
        Filters out only the rows that match the spectified date.
        Works only on a Result that has _start and _end columns.

        Parameters
        ----------
        date : str
            date can be anything Pandas.Timestamp supports parsing
        '''
        if not self.check_in_bounds(date):
            raise ValueError('Date %s is not in the queried range.' % date)
        date = Timestamp(date)
        after_start = self._start <= date
        before_end = (self._end > date) | self._end.isnull()
        if only_count:
            return np.sum(before_end & after_start)
        else:
            return self[before_end & after_start]

    def _auto_select_scale(self, dts, start=None, end=None, ideal=300):
        '''
        Guess what a good timeseries scale might be,
        given a particular data set, attempting to
        make the total number of x values as close to
        `ideal` as possible

        This is a helper for plotting
        '''
        start = min(dts) if start is None else start
        end = max(dts) if end is None else end
        maximum_count = len(filter(lambda dt: start <= dt and dt <= end, dts))
        daily_count = (end - start).days
        if maximum_count <= ideal:
            return 'maximum'
        elif daily_count <= ideal:
            return 'daily'
        elif daily_count / 7 <= ideal:
            return 'weekly'
        elif daily_count / 30 <= ideal:
            return 'monthly'
        elif daily_count / 91 <= ideal:
            return 'quarterly'
        else:
            return 'yearly'

    def history(self, scale='auto', counts=True, start=None, end=None):
        '''
        Works only on a Result that has _start and _end columns.
        most_recent=False should be set for this to work

        Parameters
        ----------
        scale: {'auto', 'maximum', 'daily', 'weekly', 'monthly',
                'quarterly', 'yearly'}
            Scale specifies the sampling intervals.
            'auto' will heuritically choose such scale that will give you
            fast results.
        counts: boolean
            If True counts will be returned
            If False ids will be returned
        start: str
            First date that will be included.
        end: str
            Last date that will be included
        '''
        start = self._start.min() if start is None else start
        end = max(self._end.max(), self._start.max()) if end is None else end
        start = start if self.check_in_bounds(start) else self._lbound
        end = end if self.check_in_bounds(end) else self._rbound

        if scale == 'auto' or scale == 'maximum':
            start_dts = list(self._start[~self._start.isnull()].values)
            end_dts = list(self._end[~self._end.isnull()].values)
            dts = map(Timestamp, set(start_dts + end_dts))
            dts = filter(lambda ts: self.check_in_bounds(ts) and
                         ts >= start and ts <= end, dts)
        if scale == 'auto':
            scale = self._auto_select_scale(dts, start, end)
        if scale != 'maximum':
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

    ######################## FILTERS ##########################

    def group_size(self, column, to_dict=False):
        '''
            Simply group items by the given column and return
            dictionary (or Pandas Series) with each bucket size
        '''
        gby = self.groupby(column).size()
        if to_dict:
            return gby.to_dict()
        else:
            return gby

    def column_count(self, column='_id'):
        return float(self[column].count())

    @mask_filter
    def ids(self, ids, _filter=True):
        ''' filter for only objects with matching object ids '''
        # there *should* be an easier way to do this, without lambda...
        return self['_id'].map(lambda x: True if x in ids else False)

    ######################## Plotting #####################

    def plot_column(self, column, **kwargs):
        x = self.group_size(column)
        return x.plot(**kwargs)
