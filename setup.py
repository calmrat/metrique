#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)

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
    description='Python/MongoDB Information Platform - Server',
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
)


if __name__ == '__main__':
    import subprocess
    import argparse

    VALID_PKGS = ['all', 'server', 'client']
    VALID_TYPES = ['sdist']

    def build(pkg, _type):
        setup_py = '%s_setup.py' % pkg
        cmd = 'python %s build %s' % (setup_py, _type)
        logger.warn(cmd)
        return subprocess.call(cmd.split(' '))

    def build_all(_type):
        build_client(_type)
        build_server(_type)

    def build_server(_type):
        return build('server', _type)

    def build_client(_type):
        return build('client', _type)

    cli = argparse.ArgumentParser(description='Metrique setup.py cli')
    cli.add_argument('--pkg',
                     choices=VALID_PKGS,
                     default='all')
    cli.add_argument('--type',
                     choices=VALID_TYPES,
                     default='sdist')
    cli.add_argument('--upload', action='store_true')

    args = cli.parse_args()

    assert args.pkg in VALID_PKGS
    assert args.type in VALID_TYPES

    if args.pkg == 'all':
        build_all(_type=args.type)
    elif args.pkg == 'client':
        build_client(_type=args.type)
    elif args.pkg == 'server':
        build_server(_type=args.type)
