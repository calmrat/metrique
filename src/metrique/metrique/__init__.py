#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique dataglue library
~~~~~~~~~~~~~~~~~~~~~~~~~
**Python/MongoDB Data Warehouse and Data Glue**

Metrique can be used to bring data into an intuitive,
indexable data object collection that supports
transparent historical version snapshotting,
advanced ad-hoc server-side querying, including (mongodb)
aggregations and (mongodb) mapreduce, along with python,
ipython, pandas, numpy, matplotlib, and so on, is well
integrated with the scientific python computing stack.

    >>> from metrique import pyclient
    >>> g = pyclient(cube="gitrepo_commit"")
    >>> g.ping()
    pong
    >>> ids = g.extract(uri='https://github.com/drpoovilleorg/metrique.git')
    >>> q = c.query.fetch('git_commit', 'author, committer_ts')
    >>> q.groupby(['author']).size().plot(kind='barh')
    >>> <matplotlib.axes.AxesSubplot at 0x6f77ad0>

:copyright: 2013 "Chris Ward" <cward@redhat.com>
:license: GPLv3, see LICENSE for more details
:sources: https://github.com/drpoovilleorg/metrique
'''

from result import Result
from metrique.utils import set_cube_path, get_cube
from metrique.config import DEFAULT_CLIENT_CUBES_PATH
from metrique.http_api import HTTPClient as pyclient

# auto_load default cube paths (eg, ~/.metrique/cubes
# load defaults; can be overridden later
try:
    set_cube_path(DEFAULT_CLIENT_CUBES_PATH)
except:
    pass
