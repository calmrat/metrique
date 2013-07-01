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

with open('readme.rst') as _file:
    readme = _file.read()

from metrique import __version__ as version

github = 'https://github.com/drpoovilleorg/metrique'
download_url = '%s/archive/%s.tar.gz' % (github, version)

setup(
    name='metrique',
    version=version,
    packages=list(find_packages(metrique.__path__, metrique.__name__)),
    url='https://github.com/drpoovilleorg/metrique',
    license='GPLv3',
    author='Chris Ward',
    author_email='cward@redhat.com',
    download_url=download_url,
    description='Python/MongoDB Information Platform',
    long_description=readme,
    data_files=[('metrique', ['readme.rst', 'version.txt'])],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only',
        'Topic :: Database',
        'Topic :: Office/Business',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: Scientific/Engineering :: Visualization',
        'Topic :: Utilities',
    ],
    keywords=['data', 'mining', 'information', 'mongo',
              'etl', 'analysis', 'search', 'query'],
    provides=['metrique'],
    requires=['pandas', 'psycopg2', 'MySQLdb', 'tornado (>=3.0)', 'pql',
              'argparse', 'dateutils', 'simplejson', 'pymongo',
              'bson', 'decorator', 'requests', 'futures',
              'dulwich', 'tz'],
    scripts=['metrique/server/bin/metrique-server',
             'install/metrique-setup-server',
             'install/metrique-setup-client']
)
