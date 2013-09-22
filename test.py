#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>


'''
**test.py**

This module contains a CLI for testing metrique.
'''


import argparse
import os
import logging
#import subprocess as sp

# init logging
logging.basicConfig(format='')
logger = logging.getLogger(__name__)

__src__ = 'src/'
__pkgs__ = ['metrique', 'metriqued', 'metriquec', 'metriqueu']
__tests__ = 'tests/'

'''
** Options **
 --debug: Enabled/Disable debug output
 --target: nvr to test (%s)
''' % (__pkgs__)

# init cli argparser
cli = argparse.ArgumentParser(
    description='Metrique Build CLI')
cli.add_argument('-d', '--debug',
                 action='store_true',
                 default=False)
cli.add_argument('-t', '--target',
                 choices=__pkgs__,
                 default='all')

# parse argv
args = cli.parse_args()

# Turn on debug?
if args.debug:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

cwd = os.getcwd()
src = os.path.join(cwd, __src__)
if args.target == 'all':
    test_paths = [os.path.join(src, pkg, __tests__) for pkg in __pkgs__]
else:
    test_paths = [os.path.join(src, args.target, __tests__)]

