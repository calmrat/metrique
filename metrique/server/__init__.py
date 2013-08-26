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

__irequires__ = __requires__

__scripts__ = [
    'metrique/server/bin/metrique-server',
    'install/metrique-setup-server',
]

__deplinks__ = [
    'https://pypi.python.org/packages/source/b/bson/bson-0.3.3.tar.gz',
    'https://pypi.python.org/packages/source/d/decorator/decorator-3.4.0.tar.gz',
    'https://pypi.python.org/packages/source/f/futures/futures-2.1.4.tar.gz',
    'https://pypi.python.org/packages/source/j/jsonconf/jsonconf-0.1.1.tar.gz',
    'https://pypi.python.org/packages/source/p/pql/pql-0.3.2.tar.gz',
    'https://pypi.python.org/packages/source/p/pymongo/pymongo-2.6.tar.gz',
    'https://pypi.python.org/packages/source/s/simplejson/simplejson-3.3.0.tar.gz',
    'https://pypi.python.org/packages/source/t/tornado/tornado-3.1.tar.gz',
]
