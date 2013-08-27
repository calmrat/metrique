#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Server package covers server side of metrique,
including http api (via tornado) server side
configuration, ETL, warehouse, query,
usermanagement, logging, etc.
'''

__pkg__ = 'metrique-server'
__version__ = '0.1.3-alpha14'
__pkgs__ = ['metrique.server']
__provides__ = __pkgs__
__desc__ = 'Python/MongoDB Information Platform - Server'
__scripts__ = [
    'metrique/server/bin/metrique-server',
    'install/metrique-setup-server',
]
__requires__ = [
    'tornado (>=3.0)',
    'pql (>=0.3.2)',
    'simplejson',
    'pymongo (>=2.1)',
    'bson',
    'decorator',
    'futures',
    'jsonconf',
]
__irequires__ = [
    'bson>=0.12',
    'celery>=3.0',
    'decorator>=3.4',
    'futures>=2.1',
    'jsonconf>=0.1.0',
    'mysqldb>=1.2',
    'pql>=0.3.2',
    'python-dateutil>=2.1',
    'pytz>=2013b',
    'simplejson>=3.3',
    'tornado>=3.0',
]
pip_src = 'https://pypi.python.org/packages/source'
__deplinks__ = [
    '%s/b/bson/bson-0.3.3.tar.gz' % pip_src,
    '%s/c/celery/celery-3.0.22.tar.gz' % pip_src,
    '%s/d/decorator/decorator-3.4.0.tar.gz' % pip_src,
    '%s/f/futures/futures-2.1.4.tar.gz' % pip_src,
    '%s/j/jsonconf/jsonconf-0.1.1.tar.gz' % pip_src,
    '%s/p/pql/pql-0.3.2.tar.gz' % pip_src,
    '%s/p/pymongo/pymongo-2.6.tar.gz' % pip_src,
    '%s/p/python-dateutil/python-dateutil-2.1.tar.gz' % pip_src,
    '%s/p/pytz/pytz-2013b.tar.gz' % pip_src,
    '%s/s/simplejson/simplejson-3.3.0.tar.gz' % pip_src,
    '%s/t/tornado/tornado-3.1.tar.gz' % pip_src,
]
