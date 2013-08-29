#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import argparse
import os
import logging
import subprocess as sp
import re

# init logging
logging.basicConfig(format='')
logger = logging.getLogger(__name__)

# header definitions
__pkgs__ = ['metrique', 'metriqued']
__src__ = 'src/'
__actions__ = ['build', 'sdist', 'install']
__bumps__ = ['release', 'version']
RE_VERSION = re.compile(r"__version__\s+=\s+'(\d+.\d+(.\d+)?)'")
RE_RELEASE = re.compile(r"__release__ = (\d+)")

# init cli argparser
cli = argparse.ArgumentParser(description='Metrique Build CLI')
cli.add_argument('-d', '--debug',
                 action='store_true',
                 default=False)
cli.add_argument('-t', '--target',
                 choices=__pkgs__,
                 default='all')
cli.add_argument('-a', '--action',
                 choices=__actions__,
                 default='sdist')
cli.add_argument('-u', '--upload',
                 action='store_true',
                 default=False)
cli.add_argument('-n', '--dry-run',
                 action='store_true',
                 default=False)
cli.add_argument('-b', '--bump',
                 choices=__bumps__,
                 default='release')
cli.add_argument('-bo', '--bump-only',
                 action='store_true',
                 default=False)

# parse argv
args = cli.parse_args()

# Turn on debug?
if args.debug:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

CWD = os.getcwd()
SRC = os.path.join(CWD, __src__)
PKG_PATHS = [os.path.join(SRC, pkg) for pkg in __pkgs__]


def bump_release(path, line, reset=False):
    m = RE_RELEASE.match(line)
    if not m:
        raise ValueError(
            'Expected line matching __release__, got: %s' % line)
    current = int(m.groups()[0])
    if reset:
        bumped = 1
    else:
        bumped = current + 1  # bump by one
    logger.debug('BUMP (RELEASE): %s->%s' % (current, bumped))
    return '__release__ = %s\n' % bumped


def bump_version(path, line):
    # if we're bumping version, reset release to 1
    bump(path, 'release')
    m = RE_VERSION.match(line)
    if not m:
        raise ValueError(
            'Expected line matching __version__, got: %s' % line)
    current = m.groups()[0]
    current_parts = map(int, current.split('.'))
    current_parts[-1] += 1
    bumped = '.'.join(map(str, current_parts))
    logger.debug('BUMP (VERSION) - %s->%s' % (current, bumped))
    return "__version__ = '%s'\n" % bumped


def bump(path, kind='release'):
    # pull current
    with open(path) as setup:
        content = setup.readlines()
        for i, line in enumerate(content):
            if kind == 'version' and RE_VERSION.match(line):
                content[i] = bump_version(path, line)
            elif kind == 'release' and RE_RELEASE.match(line):
                content[i] = bump_release(path, line, reset=True)
            else:
                continue
            break  # stop after the first replace...

    # write out the new setup file with bumped
    with open(path, 'w') as setup:
        setup.write(''.join(content))


def build(path, action='sdist', upload=False, dry_run=False,
          bump_kind='release', bump_only=False):
    assert action in __actions__
    assert bump_kind in __bumps__
    os.chdir(path)

    if upload and dry_run:
        raise RuntimeError("It doesn't make sense to dry-run upload...")
    elif upload and action != 'sdist':
        raise RuntimeError("It doesn't make sense to `%s upload`" % action)
    else:
        pass  # ok!

    # bump the version/release
    bump(os.path.join(path, 'setup.py'), bump_kind)

    if bump_only:
        return

    cmd = ['python', 'setup.py']

    cmd.append('--dry-run') if dry_run else None

    cmd.append(action)

    cmd.append('upload') if upload else None

    cmd_str = ' '.join(cmd)
    logger.info('(%s) %s' % (os.getcwd(), cmd_str))
    sp.call(cmd)


action = args.action
upload = args.upload
dry_run = args.dry_run
bump_kind = args.bump
bump_only = args.bump_only

[build(path, action,
       upload, dry_run,
       bump_kind, bump_only) for path in PKG_PATHS]
