#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from setuptools import setup, Extension
try:
    from Cython.Distutils import build_ext
    CYTHON = True
except ImportError:
    CYTHON = False

VERSION_FILE = "metrique/_version.py"
VERSION_EXEC = ''.join(open(VERSION_FILE).readlines())
__version__ = ''
exec(VERSION_EXEC)  # update __version__
if not __version__:
    raise RuntimeError("Unable to find version string in %s." % VERSION_FILE)

__pkg__ = 'metrique'
__pkgs__ = [
    'metrique',
    'metrique.cubes',
    'metrique.cubes.csvdata',
    'metrique.cubes.gitdata',
    'metrique.cubes.osinfo',
    'metrique.cubes.sqldata',
]
__provides__ = ['metrique']
__desc__ = 'Metrique - Client Libraries'
__scripts__ = []
__requires__ = [
    'anyconfig',
    #'cython',
    'decorator',
    'lockfile',
    'joblib',
    # bug when installing numpy as dep;
    # https://github.com/numpy/numpy/issues/2434
    # install manually with metrique.py deploy or `pip install pandas`
    'pandas (>=0.13.0)',
    'python_dateutil',
    'pytz',
    'psycopg2',
    'simplejson',
    'sqlalchemy (>=0.9.4)',
    'virtualenv (>=1.11)',
]
__irequires__ = [
    'anyconfig',
    #'cython',
    'decorator',
    'lockfile',
    'pandas',
    'python_dateutil',
    'pytz',
    'simplejson',
    'sqlalchemy>=0.9.4',
    #'virtualenv>=1.11',
]

pip_src = 'https://pypi.python.org/packages/source'
__deplinks__ = []

with open('README.rst') as _file:
    readme = _file.read()

github = 'https://github.com/kejbaly2/metrique'
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
    keywords=['data', 'mining', 'information', 'postgresql'
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
    zip_safe=False,  # we reference __file__; see [1]
)
# http://stackoverflow.com/questions/8362510

if CYTHON:
    default_setup['cmdclass'] = {'build_ext': build_ext}
    default_setup['ext_modules'] = [
        Extension("metrique.core_api", ['metrique/core_api.py']),
         Extension("metrique.metrique", ['metrique/metrique.py']),
         Extension("metrique._version", ['metrique/_version.py']),
         Extension("metrique.utils", ['metrique/utils.py'])
         # FIXME: these fail to compile
         #Extension("metrique.parse", ['metrique/parse.py']),
         #Extension("metrique.plotting", ['metrique/plotting.py']),
         #Extension("metrique.result", ['metrique/result.py']),
     ]

setup(**default_setup)
