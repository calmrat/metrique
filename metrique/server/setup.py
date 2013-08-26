#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
from distutils.core import setup
import os
import shutil

# CLIENT SPECIFIC
import metrique.server as mclient

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

    dependency_links=mclient.__deplinks__,
    description=mclient.__desc__,
    install_requires=mclient.__irequires__,
    name=mclient.__pkg__,
    packages=mclient.__pkgs__,
    provides=mclient.__provides__,
    requires=mclient.__requires__,
    scripts=mclient.__scripts__,
    version=mclient.__version__,
)


try:
    os.remove('setup.py')
except OSError:
    pass
shutil.copyfile('metrique/server/setup.py', 'setup.py')
setup(**default_setup)
os.remove('setup.py')
