#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import simplejson as json
import os
from dateutil.parser import parse as dt_parse

from metrique.tools.constants import UTC


class Result(object):
    def __init__(self, data=None, load_frame=True):
        self.load(data)
        if load_frame:
            try:
                self.frame
            except ImportError:
                pass

    def __repr__(self):
        try:
            return str(self.frame.head())
        except Exception:
            return str(self.data)

    def __str__(self):
        return self.__repr__()

    def __getitem__(self, key):
        if hasattr(self, '_frame'):
            return self._frame[key]
        else:
            return self.data[key]

    def __getattr__(self, attr):
        if hasattr(self, '_frame'):
            return self._frame[attr]
        else:
            return self.data[attr]

    @property
    def frame(self):
        '''
        pandas DataFrame
        '''
        try:
            from pandas import DataFrame
        except ImportError:
            raise ImportError("Install python-pandas")

        if hasattr(self, '_frame'):
            return self._frame

        self._frame = DataFrame(self.data)

        return self._frame

    @frame.setter
    def frame(self, value):
        self._frame = value

    def load(self, data):
        if isinstance(data, basestring):
            try:
                self._load_file(data)
            except Exception:
                logger.debug('Failed to load data file (%s)' % data)
                self.data = None
        elif data is not None:
            self._load_py(data)
        else:
            self.data = None
        if hasattr(self, '_frame'):
            del self._frame  # Clear cached frame

    def _load_py(self, data):
        self.data = data
        if hasattr(self, '_frame'):
            del self._frame  # Clear cached frame

    def _load_file(self, path):
        path = os.path.expanduser(path)
        with open(path) as f:
            self.data = json.load(f)

    def save(self, path):
        path = os.path.expanduser(path)
        with open(path, 'w') as f:
            json.dump(self.data, f)

    def convert_dates(self, utc=True, inplace=True):
        '''
        Takes a pandas DataFrame and converts to dates those columns
        that can be parsed as dates.
        Returns the converted DataFrame.
        '''
        if not hasattr(self, '_frame'):
            raise ValueError("No frame available")
        if utc:
            dt_p = lambda d: dt_parse(d).replace(UTC)
        else:
            dt_p = lambda d: dt_parse(d)

        result = self.frame.apply(lambda col:
                                  col.apply(dt_p)
                                  if col.apply(is_date).all()
                                  else col)
        if inplace:
            self.frame = result

        return result


def is_date(datestr):
    try:
        dt_parse(datestr)
        return True
    except:
        return False
