#!/usr/bin/env
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import re
from setuptools import setup

# FIXME: any way to add 'optional' 'extra' dependencies?
# 'matplotlib (>=1.3.1)',
# psycopg2, gittle, etc...

VERSIONFILE = "./metrique/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    __version__ = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

__pkg__ = 'metrique'
__pkgs__ = [
    'metrique', 'metrique.sql',
    'metrique.cubes', 'metrique.cubes.csvdata',
    'metrique.cubes.gitdata', 'metrique.cubes.sqldata',
    'metrique.plotting'
]
__provides__ = ['metrique']
__desc__ = 'Metrique - Client Libraries'
__scripts__ = ['metrique/bin/metrique']
__requires__ = [
    'decorator (>=3.4.0)',
    'pandas (>=0.13.0)',
    'pql (>=0.4.2)',
    'pymongo (>=2.6.3)',
    'python_dateutil (>=2.2.0)',
    'pytz'
    'simplejson (>=3.3.2)',
]
__irequires__ = [
    'decorator>=3.4.0',
    'pandas>=0.13.0',
    'pql>=0.4.2',
    'pymongo>=2.6.3',
    'python_dateutil>=2.2.0',
    'pytz',
    'simplejson>=3.3.2',
]
pip_src = 'https://pypi.python.org/packages/source'
__deplinks__ = []

with open('README.rst') as _file:
    readme = _file.read()

github = 'https://github.com/drpoovilleorg/metrique'
download_url = '%s/archive/master.zip' % github

default_setup = dict(
    url=github,
    license='GPLv3',
    author='Chris Ward',
    author_email='cward@redhat.com',
    download_url=download_url,
    long_description=readme,
    data_files=[],
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
    dependency_links=__deplinks__,
    description=__desc__,
    install_requires=__irequires__,
    name=__pkg__,
    packages=__pkgs__,
    provides=__provides__,
    requires=__requires__,
    scripts=__scripts__,
    version=__version__,
)

setup(**default_setup)
