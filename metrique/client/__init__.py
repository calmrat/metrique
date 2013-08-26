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

__version__ = '0.1.3-alpha'

__pkgs__ = ['metrique.client']

__provides__ = __pkgs__

__desc__ = 'Python/MongoDB Information Platform - Client'

__requires__ = [
    'pandas (>=0.12.0)', 'psycopg2', 'MySQLdb',
    'tornado (>=3.0)', 'pql', 'argparse',
    'dateutils', 'simplejson', 'bson',
    'decorator', 'requests', 'futures',
    'dulwich', 'tz', 'celery', 'jsonconf',
]

__scripts__ = [
    'metrique/server/bin/metrique-server',
    'install/metrique-setup-client'
]

# auto_load default cube paths (eg, ~/.metrique/cubes
# load defaults; can be overridden later

# add default cube root paths to sys.path
try:
    set_cube_path()
except:
    pass
