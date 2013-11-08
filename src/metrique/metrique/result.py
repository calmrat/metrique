#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
This module contains a Pandas DataFrame wrapper and
additional Pandas object helper functions.

Version = one row of a dataframe, it has its _oid, _start and _end
Object = specified by its _oid, one object can have multiple versions in the
    result object.
'''

import logging
logger = logging.getLogger(__name__)

from decorator import decorator
from datetime import datetime, timedelta

from pandas import DataFrame, Series
import pandas.tseries.offsets as off
from pandas.tslib import Timestamp
import pandas as pd
import numpy as np

from metriqueu.utils import dt2ts

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
        self._lbound = self._rbound = None
        self.set_date_bounds(date)

    def to_datetime(self, column):
        '''
        The json serialization/deserialization process leaves dates as
        timestamps (in s).
        This function converts the column to datetimes.
        '''
        if column in self:
            if self[column].dtype in NUMPY_NUMERICAL:
                self[column] = pd.to_datetime(self[column], unit='s')
            else:
                self[column] = pd.to_datetime(self[column], utc=True)

    def set_date_bounds(self, date):
        '''
        Pass in the date used in the original query.

        :param string date:
            Date (date range) that was queried:
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
        ''' Check that left and right bounds are sane '''
        dt = Timestamp(date)
        return ((self._lbound is None or dt >= self._lbound) and
                (self._rbound is None or dt <= self._rbound))

    def on_date(self, date, only_count=False):
        '''
        Filters out only the rows that match the spectified date.
        Works only on a Result that has _start and _end columns.

        :param string date:
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
            return self.filter(before_end & after_start)

    def history(self, dates=None, linreg_since=None, lin_reg_days=20):
        '''
        Works only on a Result that has _start and _end columns.

        :param list dates: List of dates
        :param datetime linreg_since:
            estimate the future values using linear regression.
        :param integer lin_reg_days:
            Set how many past days should we use for prediction calulation
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

        :param pandas.Series series:
            A series in which the values will be places.
            The index will not be touched.
            Only the values on dates > `since` will be predicted.
        :param datetime since:
            The starting date from which the future will be predicted.
        :param integer days:
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

        :param string scale:
            {'auto', 'maximum', 'daily', 'weekly', 'monthly',
            'quarterly', 'yearly'}
            Scale specifies the sampling intervals.
            'auto' will heuritically choose such scale that will give you
            fast results.
        :param string start:
            First date that will be included.
        :param string end:
            Last date that will be included
        :param boolean include_bounds:
            Include start and end in the result if they are not included yet.

        '''
        if scale not in ['auto', 'maximum', 'daily', 'weekly', 'monthly',
                         'quarterly', 'yearly']:
            raise ValueError('Incorrect scale: %s' % scale)
        start = Timestamp(start or self._start.min())
        end = Timestamp(end or max(Timestamp(self._end.max()),
                                   self._start.max()))
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
        offset = dict(daily=off.Day(), weekly=off.Week(),
                      monthly=off.MonthEnd(), quarterly=off.QuarterEnd(),
                      yearly=off.YearEnd())
        ret = list(pd.date_range(start + offset[scale], end, freq=freq[scale]))
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

        :param list dates:
            List of the dates at which the count should be computed.
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

        :param string index: Name of the new column.
        '''
        def prep(df):
            ends = set(df._end.tolist())
            end = pd.NaT if pd.NaT in ends else max(ends)
            if end is pd.NaT:
                age = cut_ts - df._start.min()
            else:
                age = min(cut_ts, end) - df._start.min()
            last = df[df._end == end].copy()
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

        :param int index:
            List-like index of the version.
            0 means first version, -1 means last.
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
    def started_after(self, dt):
        '''
        Leaves only those objects whose first version started after the
        specified date.
        '''
        dt = Timestamp(dt)
        starts = self._start.groupby(self._oid).min()
        oids = set(starts[starts > dt].index.tolist())
        return self[self._oid.apply(lambda v: v in oids)]

    @filtered
    def filter(self, mask):
        return self[mask]

    @filtered
    def object_apply(self, function):
        '''
        Groups by _oid, then applies the function to each group
        and finally concatenates the results.

        :param (DataFrame -> DataFrame) function:
            function that takes a DataFrame and returns a DataFrame
        '''
        return pd.concat([function(df) for _, df in self.groupby(self._oid)])

    ################################ SAVE ####################################

    def save_to_cube(self, oid, pyclient, cube='results', owner=None):
        '''
        Saves this result objects to the specified metrique cube.

        :param str oid:
            The _oid to be used for this result.
        :param HTTPClient pyclient:
            A client that should be used to connect to the cube.
        :param str cube:
            Cube's name.
        :param str owner:
            Cube's owner.
        '''
        frame = self.to_dict('list')
        obj = {'_oid': str(oid), 'frame': frame,
               'lbound': self._lbound, 'rbound': self._rbound}
        pyclient.cube_save([obj], cube=cube, owner=owner)


#################################### LOAD ####################################

def load_from_cube(oid, pyclient, cube='results', owner=None):
    '''
    Loads a result object from cube.

    :param str oid:
        The _oid of the result to be loaded.
    :param HTTPClient pyclient:
        A client that should be used to connect to the cube.
    :param str cube:
        Cube's name.
    :param str owner:
        Cube's owner.
    '''
    res = pyclient.find('_oid == "%s"' % oid,
                        fields='__all__', cube=cube, owner=owner, raw=True)
    result = Result(res[0]['frame'])
    result._lbound = res[0]['lbound']
    result._rbound = res[0]['rbound']
    return result
