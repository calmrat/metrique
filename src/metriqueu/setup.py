#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
from setuptools import setup

__pkg__ = 'metriqueu'
__version__ = '0.2.2'
__release__ = 12
__nvr__ = '%s-%s' % (__version__, __release__)
__pkgs__ = ['metriqueu']
__provides__ = ['metriqueu']
__desc__ = 'Metrique - Shared Utility Libraries'
__scripts__ = []
__requires__ = [
    'decorator (>=3.4)',
    'futures (>=2.1)',
    'pql (>=0.3.2)',
    'python_dateutil (>=2.1)',
    'pytz',  # (>=2013b)
    'simplejson (>=3.3)',
]
__irequires__ = [
    'decorator>=3.4',
    'futures>=2.1',
    'pql>=0.3.2',
    'python_dateutil>=2.1',
    'pytz>=2013b',
    'simplejson>=3.3',
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
    dependency_links=__deplinks__,
    description=__desc__,
    install_requires=__irequires__,
    name=__pkg__,
    packages=__pkgs__,
    provides=__provides__,
    requires=__requires__,
    scripts=__scripts__,
    version=__nvr__,
)

setup(**default_setup)
