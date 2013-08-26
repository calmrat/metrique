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

__version__ = '0.1.3-alpha'

__pkgs__ = ['metrique.server']

__provides__ = __pkgs__

__desc__ = 'Python/MongoDB Information Platform - Server'

__requires__ = [
    'tornado (>=3.0)',
    'pql (>=0.3.2)',
    'argparse',
    'simplejson',
    'pymongo (>=2.1)',
    'bson',
    'decorator',
    'futures',
    'jsonconf',
]

__scripts__ = [
    'metrique/server/bin/metrique-server',
    'install/metrique-setup-server'
]
