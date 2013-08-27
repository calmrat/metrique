#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
from distutils.core import setup

# FIXME: include metrique/server/static/

__pkg__ = 'metrique-server'
__release__ = 27
__version__ = '0.1.3-alpha%i' % __release__
__pkgs__ = [
    'metrique',
    'metrique.server',
    'metrique.server.mongodb',
    'metrique.server.tornado',
]
__provides__ = __pkgs__
__datafiles__ = []
__desc__ = 'Python/MongoDB Information Platform - Server'
__scripts__ = [
    'install/metrique-setup-server',
]
__requires__ = [
    'bson (>=0.3.3)',
    'decorator (>=3.4)',
    'futures (>=2.1)',
    'jsonconf (>=0.1.3)',
    'pql (>=0.3.2)',
    'python_dateutil (>=2.1)',
    'pytz',  # (>=2013b)
    'simplejson (>=3.3)',
    'tornado (>=3.0)',
]
__irequires__ = [
    'bson>=0.3.3',
    'decorator>=3.4',
    'futures>=2.1',
    'jsonconf>=0.1.3',
    'pql>=0.3.2',
    'python_dateutil>=2.1',
    'pytz>=2013b',
    'simplejson>=3.3',
    'tornado>=3.0',
]
pip_src = 'https://pypi.python.org/packages/source'
__deplinks__ = [
    '%s/b/bson/bson-0.3.3.tar.gz' % pip_src,
    '%s/d/decorator/decorator-3.4.0.tar.gz' % pip_src,
    '%s/f/futures/futures-2.1.4.tar.gz' % pip_src,
    '%s/j/jsonconf/jsonconf-0.1.1.tar.gz' % pip_src,
    '%s/p/pql/pql-0.3.2.tar.gz' % pip_src,
    '%s/p/python-dateutil/python-dateutil-2.1.tar.gz' % pip_src,
    '%s/p/pytz/pytz-2013b.tar.gz' % pip_src,
    '%s/s/simplejson/simplejson-3.3.0.tar.gz' % pip_src,
    '%s/t/tornado/tornado-3.1.tar.gz' % pip_src,
]

with open('README') as _file:
    readme = _file.read()

github = 'https://github.com/drpoovilleorg/metrique'
download_url = '%s/archive/master.zip' % github

default_setup = dict(
    url='https://github.com/drpoovilleorg/metrique',
    license='GPLv3',
    author='Chris Ward',
    author_email='cward@redhat.com',
    download_url=download_url,
    long_description=readme,
    data_files=__datafiles__,
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
