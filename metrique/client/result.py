#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from decorator import decorator
import simplejson as json
import os
import datetime

from pandas import DataFrame, Series
import pandas.tseries.offsets as off
from pandas.tslib import Timestamp
import pandas as pd
import numpy as np


def mask_filter(f):
    '''
        Generic function for getting back filtered frame
        data according to True/False mask filter frame matching

        :param Pandas.DataFrame mask_frame:
            DataFrame that maps index:True/False where True means it
            matches the filter and False means it does not
        :param Boolean filter_ :
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
        return type(self)(self[mask_frame == filter_])


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

        :param String date:
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

        :param String date:
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

        :param String scale: {'auto', 'maximum', 'daily', 'weekly', 'monthly',
                'quarterly', 'yearly'}
            Scale specifies the sampling intervals.
            'auto' will heuritically choose such scale that will give you
            fast results.
        :param Boolean counts:
            If True counts will be returned
            If False ids will be returned
        :param String start:
            First date that will be included.
        :param String end:
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

        :param String start:
            First date that will be included.
        :param String end:
            Last date that will be included
        :param String scale: {'daily', 'weekly', 'monthly', 'quarterly', 'yearly'}
            Scale specifies the sampling intervals.
        :param Boolean include_bounds:
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

    def count(self, column='_id'):
        return float(self[column].count())

    @mask_filter
    def ids(self, ids, _filter=True):
        ''' filter for only objects with matching object ids '''
        # there *should* be an easier way to do this, without lambda...
        return self['_id'].map(lambda x: True if x in ids else False)

    def unfinished_only(self):
        '''
        Leaves only those entities that has some version with _end equal None.
        '''
        oids = self[self._end.isnull()]._oid.tolist()
        return type(self)(self[self._oid.apply(lambda oid: oid in oids)])

    def last_versions_with_age(self, col_name='age'):
        '''
        Leaves only the latest version for each entity.
        Add a new column which represents age - it is computed by taking
        _start of the oldest version and subtracting it from current time.

        Parameters
        ----------
        col_name: str
            Name of the new column.
        '''
        def prep(df):
            age = now_ts - df._start.min()
            last = df[df._end.isnull()].copy()
            last[col_name] = age
            return last

        now_ts = datetime.datetime.now()
        res = pd.concat([prep(df) for _, df in self.groupby(self._oid)])
        return type(self)(res)

    def last_chain_only(self):
        '''
        Leaves only the last chain for each entity.
        Chain is a series of consecutive versions
            (_end of one is _start of another) .
        '''
        def prep(df):
            ends = df._end.tolist()
            maxend = pd.NaT if pd.NaT in ends else max(ends)
            ends = set(df._end.tolist()) - set(df._start.tolist() + [maxend])
            if len(ends) == 0:
                return df
            else:
                cutoff = max(ends)
                return df[df._start > cutoff]

        res = pd.concat([prep(df) for _, df in self.groupby(self._oid)])
        return type(self)(res)

    def first_versions(self):
        '''
        Leaves only the first version for each entity.
        '''
        def prep(df):
            return df[df._start == df._start.min()]

        res = pd.concat([prep(df) for _, df in self.groupby(self._oid)])
        return type(self)(res)

    def started_after(self, dt):
        '''
        Leaves only those entities whose first version started after the
        specified date.
        '''
        starts = self._start.groupby(self._oid).min()
        ids = starts[starts > dt].index.tolist()
        res = self[self._oid.apply(lambda v: v in ids)]
        return type(self)(res)

    ######################## Plotting #####################

    def plot_column(self, column, **kwargs):
        x = self.group_size(column)
        return x.plot(**kwargs)
