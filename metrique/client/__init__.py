#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Client package covers client side of metrique,
including tornado access, work with results,
client side configuration and opensource cubes.
'''

from result import Result
from metrique.client.cubes import set_cube_path
from metrique.client.cubes.utils import get_cube
from metrique.client.config import DEFAULT_CLIENT_CUBES_PATH

__pkg__ = 'metrique-client'
__version__ = '0.1.3-alpha14'
__pkgs__ = ['metrique.client']
__provides__ = __pkgs__
__desc__ = 'Python/MongoDB Information Platform - Client'
__scripts__ = [
    'install/metrique-setup-client',
]
__requires__ = [
    'bson (>=0.12)',
    'celery (>=3.0)',
    'dateutils (>=0.6.6)',
    'decorator (>=3.4)',
    'dulwich (>=0.9)',
    'futures (>=2.1)',
    'jsonconf (>=0.1.0)',
    'mysqldb (>=1.2)',
    'pandas (>=0.12)',
    'pql (>=0.3.2)',
    'psycopg2 (>=2.5)',
    'pytz',  # (>=2013b)
    'requests (>=1.2)',
    'simplejson (>=3.3)',
    'tornado (>=3.0)',
]
__irequires__ = [
    'bson>=0.12',
    'celery>=3.0',
    'decorator>=3.4',
    'dulwich>=0.9',
    'futures>=2.1',
    'jsonconf>=0.1.0',
    'mysqldb>=1.2',
    'pandas>=0.12',
    'pql>=0.3.2',
    'psycopg2>=2.5',
    'python-dateutil>=2.1',
    'pytz>=2013b',
    'requests>=1.2',
    'simplejson>=3.3',
    'tornado>=3.0',
]
pip_src = 'https://pypi.python.org/packages/source'
__deplinks__ = [
    '%s/b/bson/bson-0.3.3.tar.gz' % pip_src,
    '%s/c/celery/celery-3.0.22.tar.gz' % pip_src,
    '%s/d/decorator/decorator-3.4.0.tar.gz' % pip_src,
    'http://samba.org/~jelmer/dulwich/dulwich-0.9.0.tar.gz',  # EXT. SITE
    '%s/f/futures/futures-2.1.4.tar.gz' % pip_src,
    '%s/j/jsonconf/jsonconf-0.1.1.tar.gz' % pip_src,
    '%s/M/MySQL-python/MySQL-python-1.2.4.zip' % pip_src,
    '%s/p/pandas/pandas-0.12.0.tar.gz' % pip_src,
    '%s/p/pql/pql-0.3.2.tar.gz' % pip_src,
    '%s/p/psycopg2/psycopg2-2.5.1.tar.gz' % pip_src,
    '%s/p/python-dateutil/python-dateutil-2.1.tar.gz' % pip_src,
    '%s/p/pytz/pytz-2013b.tar.gz' % pip_src,
    '%s/r/requests/requests-1.2.3.tar.gz' % pip_src,
    '%s/s/simplejson/simplejson-3.3.0.tar.gz' % pip_src,
    '%s/t/tornado/tornado-3.1.tar.gz' % pip_src,
]

# auto_load default cube paths (eg, ~/.metrique/cubes
# load defaults; can be overridden later
try:
    set_cube_path(DEFAULT_CLIENT_CUBES_PATH)
except:
    pass
