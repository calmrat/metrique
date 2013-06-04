#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from distutils.core import setup
from pkgutil import walk_packages

import metrique


def find_packages(path='./', prefix=""):
    yield prefix
    prefix = prefix + "."
    for _, name, ispkg in walk_packages(path, prefix):
        if ispkg:
            yield name


setup(
    name='metrique',
    version='0.1.0',
    packages=list(find_packages(metrique.__path__, metrique.__name__)),
    url='https://github.com/drpoovilleorg/metrique',
    license='GPLv3',
    author='Chris Ward',
    author_email='cward@redhat.com',
    description='Python/MongoDB Information Platform',
    requires=['pandas', 'psycopg2', 'MySQLdb', 'tornado (>=3.0)', 'pql',
              'argparse', 'dateutil', 'simplejson', 'pymongo',
              'bson', 'decorator', 'requests', 'futures',
              'gitdb', 'tz'],
    scripts=['metrique/server/bin/metrique-server',
             'install/metrique-setup-server',
             'install/metrique-setup-client']
)
