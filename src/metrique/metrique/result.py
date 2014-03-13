#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.result
~~~~~~~~~~~~~~~~~

This module contains a Pandas DataFrame wrapper and
additional Pandas object helper functions which is
used to load and manipulate cube objects.
'''

from decorator import decorator
from datetime import datetime, timedelta
import logging
import numpy as np
from pandas import DataFrame, Series
import pandas.tseries.offsets as off
from pandas.tslib import Timestamp
import pandas as pd

from metriqueu.utils import dt2ts

logger = logging.getLogger(__name__)

NUMPY_NUMERICAL = [np.float16, np.float32, np.float64, np.float128,
                   np.int8, np.int16, np.int32, np.int64]


def filtered(f):
    '''
    Decorator function that wraps functions returning pandas
    dataframes, such that the dataframe is filtered
    according to left and right bounds set.
    '''
    def _filter(f, self, *args, **kwargs):
        frame = f(self, *args, **kwargs)
        ret = type(self)(frame)
        ret._lbound = self._lbound
        ret._rbound = self._rbound
        return ret

    return decorator(_filter, f)


class Result(DataFrame):
    ''' Custom DataFrame implementation for Metrique '''
    def __init__(self, data=None, date=None):
        super(Result, self).__init__(data)
        # The converts are here so that None is converted to NaT
        self.to_datetime('_start')
        self.to_datetime('_end')
        if isinstance(data, Result):
            self._lbound = data._lbound
            self._rbound = data._rbound
        else:
            self._lbound = self._rbound = None
            self.set_date_bounds(date)

    def to_datetime(self, column):
        '''
        The json serialization/deserialization process leaves dates as
        timestamps (in s).

        This function converts the column to datetimes.

        :param column: column to convert from current state -> datetime
        '''
        if column in self:
            if self[column].dtype in NUMPY_NUMERICAL:
                self[column] = pd.to_datetime(self[column], unit='s')
            else:
                self[column] = pd.to_datetime(self[column], utc=True)

    def set_date_bounds(self, date):
        '''
        Pass in the date used in the original query.

        :param date: Date (date range) that was queried:
            date -> 'd', '~d', 'd~', 'd~d'
            d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
        '''
        if date is not None:
            split = date.split('~')
            if len(split) == 1:
                self._lbound = Timestamp(date)
                self._rbound = Timestamp(date)
            elif len(split) == 2:
                if split[0] != '':
                    self._lbound = Timestamp(split[0])
                if split[1] != '':
                    self._rbound = Timestamp(split[1])
            else:
                raise Exception('Date %s is not in the correct format' % date)

    def check_in_bounds(self, date):
        '''Check that left and right bounds are sane

        :param date: date to validate left/right bounds for
        '''
        dt = Timestamp(date)
        return ((self._lbound is None or dt >= self._lbound) and
                (self._rbound is None or dt <= self._rbound))

    def on_date(self, date, only_count=False):
        '''
        Filters out only the rows that match the spectified date.
        Works only on a Result that has _start and _end columns.

        :param date: date can be anything Pandas.Timestamp supports parsing
        :param only_count: return back only the match count
        '''
        if not self.check_in_bounds(date):
            raise ValueError('Date %s is not in the queried range.' % date)
        date = Timestamp(date)
        after_start = self._start <= date
        before_end = (self._end > date) | self._end.isnull()
        if only_count:
            return np.sum(before_end & after_start)
        else:
            return self.filter(before_end & after_start)

    def history(self, dates=None, linreg_since=None, lin_reg_days=20):
        '''
        Works only on a Result that has _start and _end columns.

        :param dates: list of dates to query
        :param linreg_since: estimate future values using linear regression.
        :param lin_reg_days: number of past days to use as prediction basis
        '''
        dates = dates or self.get_dates_range()
        vals = [self.on_date(dt, only_count=True) for dt in dates]
        ret = Series(vals, index=dates)
        if linreg_since is not None:
            ret = self._linreg_future(ret, linreg_since, lin_reg_days)
        return ret.sort_index()

    def _linreg_future(self, series, since, days=20):
        '''
        Predicts future using linear regression.

        :param series:
            A series in which the values will be places.
            The index will not be touched.
            Only the values on dates > `since` will be predicted.
        :param since:
            The starting date from which the future will be predicted.
        :param days:
            Specifies how many past days should be used in the linear
            regression.
        '''
        last_days = pd.date_range(end=since, periods=days)
        hist = self.history(last_days)

        xi = np.array(map(dt2ts, hist.index))
        A = np.array([xi, np.ones(len(hist))])
        y = hist.values
        w = np.linalg.lstsq(A.T, y)[0]

        for d in series.index[series.index > since]:
            series[d] = w[0] * dt2ts(d) + w[1]
            series[d] = 0 if series[d] < 0 else series[d]

        return series

    ############################# DATES RANGE ################################

    def get_dates_range(self, scale='auto', start=None, end=None):
        '''
        Returns a list of dates sampled according to the specified parameters.

        :param scale: {'auto', 'maximum', 'daily', 'weekly', 'monthly',
            'quarterly', 'yearly'}
            Scale specifies the sampling intervals.
            'auto' will heuristically choose a scale for quick processing
        :param start: First date that will be included.
        :param end: Last date that will be included
        '''
        if scale not in ['auto', 'maximum', 'daily', 'weekly', 'monthly',
                         'quarterly', 'yearly']:
            raise ValueError('Incorrect scale: %s' % scale)
        start = Timestamp(start or self._start.min() or '2010-01-01')
        start = Timestamp('2010-01-01') if repr(start) == 'NaT' else start
        end = Timestamp(end or max(Timestamp(self._end.max()),
                                   self._start.max()))
        end = datetime.utcnow() if repr(end) == 'NaT' else end
        start = start if self.check_in_bounds(start) else self._lbound
        end = end if self.check_in_bounds(end) else self._rbound

        if scale == 'auto':
            scale = self._auto_select_scale(start, end)
        if scale == 'maximum':
            start_dts = list(self._start.dropna().values)
            end_dts = list(self._end.dropna().values)
            dts = map(Timestamp, set(start_dts + end_dts))
            dts = filter(lambda ts: self.check_in_bounds(ts) and
                         ts >= start and ts <= end, dts)
            return dts

        freq = dict(daily='D', weekly='W', monthly='M', quarterly='3M',
                    yearly='12M')
        offset = dict(daily=off.Day(n=0), weekly=off.Week(),
                      monthly=off.MonthEnd(), quarterly=off.QuarterEnd(),
                      yearly=off.YearEnd())
        # for some reason, weekly date range gives one week less:
        end_ = end + off.Week() if scale == 'weekly' else end
        ret = list(pd.date_range(start + offset[scale], end_,
                                 freq=freq[scale]))
        ret = [dt for dt in ret if dt <= end]
        ret = [start] + ret if ret and start < ret[0] else ret
        ret = ret + [end] if ret and end > ret[-1] else ret
        ret = filter(lambda ts: self.check_in_bounds(ts), ret)
        return ret

    def _auto_select_scale(self, start=None, end=None, ideal=300):
        '''
        Guess what a good timeseries scale might be,
        given a particular data set, attempting to
        make the total number of x values as close to
        `ideal` as possible

        This is a helper for plotting
        '''
        start = start or self._start.min()
        end = end or max(self._end.max(), self._start.max())
        daily_count = (end - start).days
        if daily_count <= ideal:
            return 'daily'
        elif daily_count / 7 <= ideal:
            return 'weekly'
        elif daily_count / 30 <= ideal:
            return 'monthly'
        elif daily_count / 91 <= ideal:
            return 'quarterly'
        else:
            return 'yearly'

    ############################### FILTERS ##################################

    @filtered
    def filter_oids(self, oids):
        '''
        Leaves only objects with specified oids.

        :param oids: list of oids to include
        '''
        oids = set(oids)
        return self[self['_oid'].map(lambda x: x in oids)]

    @filtered
    def unfinished_objects(self):
        '''
        Leaves only versions of those objects that has some version with
        `_end == None` or with `_end > right cutoff`.
        '''
        mask = self._end.isnull()
        if self._rbound is not None:
            mask = mask | (self._end > self._rbound)
        oids = set(self[mask]._oid.tolist())
        return self[self._oid.apply(lambda oid: oid in oids)]

    def persistent_oid_counts(self, dates):
        '''
        Counts have many objects (identified by their oids) existed before
        or on a given date.

        :param dates: list of the dates the count should be computed.
        '''
        total = pd.Series([self.on_date(d)._oid for d in dates],
                          index=dates)
        for i in range(1, total.size):
            a1 = total[total.index[i - 1]]
            a2 = total[total.index[i]]
            total[total.index[i]] = list(set(a1) | set(a2))
        return total.apply(len)

    @filtered
    def last_versions_with_age(self, col_name='age'):
        '''
        Leaves only the latest version for each object.
        Adds a new column which represents age.
        The age is computed by subtracting _start of the oldest version
        from one of these possibilities::

            # psuedo-code
            if self._rbound is None:
                if latest_version._end is pd.NaT:
                    current_time is used
                else:
                    min(current_time, latest_version._end) is used
            else:
                if latest_version._end is pd.NaT:
                    self._rbound is used
                else:
                    min(self._rbound, latest_version._end) is used

        :param index: name of the new column.
        '''
        def prep(df):
            ends = set(df._end.tolist())
            end = pd.NaT if pd.NaT in ends else max(ends)
            if end is pd.NaT:
                age = cut_ts - df._start.min()
            else:
                age = min(cut_ts, end) - df._start.min()
            # for some reason this is not working:
            #last = df[df._end == end].copy()
            # but this is:
            last = df[df._end.isin([end])].copy()
            last[col_name] = age - timedelta(microseconds=age.microseconds)
            return last

        cut_ts = self._rbound or datetime.utcnow()
        res = pd.concat([prep(df) for _, df in self.groupby(self._oid)])
        return res

    @filtered
    def last_chain(self):
        '''
        Leaves only the last chain for each object.

        Chain is a series of consecutive versions where
        `_end` of one is `_start` of another.
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

        return pd.concat([prep(df) for _, df in self.groupby(self._oid)])

    @filtered
    def one_version(self, index=0):
        '''
        Leaves only one version for each object.

        :param index: List-like index of the version.  0 == first; -1 == last
        '''
        def prep(df):
            start = sorted(df._start.tolist())[index]
            return df[df._start == start]

        return pd.concat([prep(df) for _, df in self.groupby(self._oid)])

    def first_version(self):
        '''
        Leaves only the first version for each object.
        '''
        return self.one_version(0)

    def last_version(self):
        '''
        Leaves only the last version for each object.
        '''
        return self.one_version(-1)

    @filtered
    def started_after(self, date):
        '''
        Leaves only those objects whose first version started after the
        specified date.

        :param date: date string to use in calculation
        '''
        dt = Timestamp(date)
        starts = self.groupby(self._oid).apply(lambda df: df._start.min())
        oids = set(starts[starts > dt].index.tolist())
        return self[self._oid.apply(lambda v: v in oids)]

    @filtered
    def filter(self, mask):
        '''alias for pandas mask filters

        :param mask: pandas mask filter query
        '''
        return self[mask]

    @filtered
    def object_apply(self, function):
        '''
        Groups by _oid, then applies the function to each group
        and finally concatenates the results.

        :param function: func that takes a DataFrame and returns a DataFrame
        '''
        return pd.concat([function(df) for _, df in self.groupby(self._oid)])

    ################################ SAVE ####################################

    def save_to_cube(self, oid, pyclient, cube='results', owner=None):
        '''
        Saves this result objects to the specified metrique cube.

        :param oid: The _oid to be used for this result
        :param pyclient: A client that should be used to connect to the cube
        :param cube: cube name
        :param owner: username of cube owner
        '''
        frame = self.to_dict('list')
        obj = {'_oid': str(oid), 'frame': frame,
               'lbound': self._lbound, 'rbound': self._rbound}
        pyclient.cube_save([obj], cube=cube, owner=owner)
