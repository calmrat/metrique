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
from metrique.client.config import CLIENT_CUBES_PATH

__pkg__ = 'metrique-client'

__version__ = '0.1.3-alpha14'

__pkgs__ = ['metrique.client']

__provides__ = __pkgs__

__desc__ = 'Python/MongoDB Information Platform - Client'

__requires__ = [
    'pandas (>=0.12)', 'psycopg2 (>=2.5)', 'mysqldb (>=1.2)',
    'tornado (>=3.0)', 'pql (>=0.3.2)', 'dateutils (>=0.6.6)',
    'bson (>=0.12)', 'decorator (>=3.4)', 'requests (>=1.2)',
    'simplejson (>=3.3)', 'futures (>=2.1)', 'dulwich (>=0.9)',
    'celery (>=3.0)', 'jsonconf (>=0.1.0)',
    'pytz',
]

__irequires__ = [
    'pandas>=0.12', 'psycopg2>=2.5', 'mysqldb>=1.2',
    'tornado>=3.0', 'pql>=0.3.2', 'dateutils>=0.6.6',
    'bson>=0.12', 'decorator>=3.4', 'requests>=1.2',
    'simplejson>=3.3', 'futures>=2.1', 'dulwich>=0.9',
    'celery>=3.0', 'jsonconf>=0.1.0',
    'pytz>=2013b',
]

__scripts__ = [
    'install/metrique-setup-client',
]

__deplinks__ = [
    'https://pypi.python.org/packages/source/p/pandas/pandas-0.12.0.tar.gz',
    'https://pypi.python.org/packages/source/p/psycopg2/psycopg2-2.5.1.tar.gz',
    'https://pypi.python.org/packages/source/M/MySQL-python/MySQL-python-1.2.4.zip',
    'https://pypi.python.org/packages/source/t/tornado/tornado-3.1.tar.gz',
    'https://pypi.python.org/packages/source/p/pql/pql-0.3.2.tar.gz',
    'https://pypi.python.org/packages/source/p/python-dateutil/python-dateutil-2.1.tar.gz',
    'https://pypi.python.org/packages/source/s/simplejson/simplejson-3.3.0.tar.gz',
    'https://pypi.python.org/packages/source/b/bson/bson-0.3.3.tar.gz',
    'https://pypi.python.org/packages/source/d/decorator/decorator-3.4.0.tar.gz',
    'https://pypi.python.org/packages/source/r/requests/requests-1.2.3.tar.gz',
    'https://pypi.python.org/packages/source/f/futures/futures-2.1.4.tar.gz',
    'http://samba.org/~jelmer/dulwich/dulwich-0.9.0.tar.gz',   # WARNING:  EXTERNAL SITE
    'https://pypi.python.org/packages/source/c/celery/celery-3.0.22.tar.gz',
    'https://pypi.python.org/packages/source/j/jsonconf/jsonconf-0.1.1.tar.gz',
    #'',
    # MISSING: tz
]

# auto_load default cube paths (eg, ~/.metrique/cubes
# load defaults; can be overridden later

# add default cube root paths to sys.path
try:
    set_cube_path()
except:
    pass
