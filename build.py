#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>


'''
**build.py**

This module contains a CLI for building metrique
with setup.py and distributing via pypi pip.

metrique (https://pypi.python.org/pypi/metrique)
metriqued (https://pypi.python.org/pypi/metriqued)
'''


import argparse
from functools import partial
import os
import logging
import subprocess as sp
import re

# init logging
logging.basicConfig(format='')
logger = logging.getLogger(__name__)

# header definitions
__pkgs__ = ['metrique', 'metriqued', 'metriquec', 'metriqueu']
__src__ = 'src/'
__actions__ = ['build', 'sdist', 'install', 'develop']
__bumps__ = ['x', 'y', 'z', 'r']
#RE_RELEASE = re.compile(r"__release__ = [\"']?((\d+)a?)[\"']?")
RE_VERSION_X = re.compile(r"__version__\s+=\s+[\"']((\d+).\d+.\d+)[\"']")
RE_VERSION_Y = re.compile(r"__version__\s+=\s+[\"'](\d+.(\d+).\d+)[\"']")
RE_VERSION_Z = re.compile(r"__version__\s+=\s+[\"'](\d+.\d+.(\d+))[\"']")
RE_RELEASE = re.compile(r"__release__ = [\"']?((\d+)a?)[\"']?")

'''
** Options **
 --debug: Enabled/Disable debug output
 --target: package to build (%s)
 --action: setup action (%s)
 --upload: Upload builds to pypi?
 --dry-run: flag to not actually do anything
 --nobump: don't bump default:release count
 --bump-kind: bump release by default; choices: %s
 --bump-only: bump nvr and quit
 --bump-only: bump nvr and quit
''' % (__pkgs__, __actions__, __bumps__)

# init cli argparser
cli = argparse.ArgumentParser(
    description='Metrique Build CLI')
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
cli.add_argument('-b', '--nobump',
                 action='store_true',
                 default=False)
cli.add_argument('-bk', '--bump-kind',
                 choices=__bumps__,
                 default='z')
cli.add_argument('-bo', '--bump-only',
                 action='store_true',
                 default=False)
cli.add_argument('-ga', '--ga-release',
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
if args.target == 'all':
    pkg_paths = [os.path.join(SRC, pkg) for pkg in __pkgs__]
else:
    pkg_paths = [os.path.join(SRC, args.target)]

setup_paths = ['%s/setup.py' % path for path in pkg_paths]


def bump_version(regex, line, i, reset, **kwargs):
    # if we're bumping version, reset release to 1
    m = regex.match(line)
    if not m:
        raise ValueError(
            'Expected line matching __version__, got: %s' % line)
    current = m.groups()[0]
    current_parts = map(int, current.split('.'))
    if reset:
        current_parts[i] = 0
    else:
        current_parts[i] += 1
    bumped = '.'.join(map(str, current_parts))
    logger.debug('BUMP (VERSION) - %s->%s' % (current, bumped))
    return "__version__ = '%s'\n" % bumped


def bump_version_x(line, reset=False, **kwargs):
    return bump_version(RE_VERSION_X, line, 0, reset, **kwargs)


def bump_version_y(line, reset=False, **kwargs):
    return bump_version(RE_VERSION_Y, line, 1, reset, **kwargs)


def bump_version_z(line, reset=False, **kwargs):
    return bump_version(RE_VERSION_Z, line, 2, reset, **kwargs)


def bump_release(line, reset=False, ga=False):
    m = RE_RELEASE.match(line)
    if not m:
        raise ValueError(
            'Expected line matching __release__, got: %s' % line)
    current = int(m.groups()[1])
    if reset:
        bumped = 1
    else:
        bumped = current + 1  # bump by one

    if not ga:
        bumped = '"%sa"' % bumped

    logger.debug('BUMP (RELEASE): %s->%s' % (current, bumped))
    return '__release__ = %s\n' % bumped


def update_line(path, regex, bump_func):
    with open(path) as setup:
        content = setup.readlines()
        for i, line in enumerate(content):
            if regex.match(line):
                content[i] = bump_func(line)
            else:
                continue
            break  # stop after the first replace...
    if not content:
        raise ValueError("content was empty; didn't want to overwrite...")
    elif None in content:
        raise RuntimeError(
            "check your nrv strings; "
            " they're missing our regex: \n%s" % regex.pattern)
    content_str = ''.join(content)
    # write out the new setup file with bumped
    with open(path, 'w') as setup:
        setup.write(content_str)


def bump(path, kind='r', reset=False, ga=False):
    assert kind in __bumps__
    # pull current
    if kind == 'x':
        regex = RE_VERSION_X
        bump(path=path, kind='y', reset=True, ga=ga)
        bump_func = partial(bump_version_x, reset=reset,
                            ga=ga)
    elif kind == 'y':
        regex = RE_VERSION_Y
        bump(path=path, kind='z', reset=True, ga=ga)
        bump_func = partial(bump_version_y, reset=reset,
                            ga=ga)
    elif kind == 'z':
        regex = RE_VERSION_Z
        bump(path=path, kind='r', reset=True, ga=ga)
        bump_func = partial(bump_version_z, reset=reset,
                            ga=ga)
    elif kind == 'r':
        regex = RE_RELEASE
        bump_func = partial(bump_release, reset=reset,
                            ga=ga)
    update_line(path, regex, bump_func)


def build(path, action='sdist', upload=False, dry_run=False):
    assert action in __actions__
    os.chdir(path)

    if upload and dry_run:
        raise RuntimeError("It doesn't make sense to dry-run upload...")
    elif upload and action != 'sdist':
        raise RuntimeError("It doesn't make sense to `%s upload`" % action)
    else:
        pass  # ok!

    cmd = ['python', 'setup.py']
    cmd.append('--dry-run') if dry_run else None
    cmd.append(action)
    cmd.append('upload') if upload else None
    cmd_str = ' '.join(cmd)
    logger.info('(%s) %s' % (os.getcwd(), cmd_str))
    sp.call(cmd)


def develop(path):
    cmd = ['python', 'setup.py' 'develop']
    cmd_str = ' '.join(cmd)
    logger.info('(%s) %s' % (os.getcwd(), cmd_str))
    sp.call(cmd)


action = args.action
upload = args.upload
dry_run = args.dry_run
nobump = args.nobump
bump_kind = args.bump_kind
bump_only = args.bump_only
ga = args.ga_release

if action == 'develop':
    [develop(path=path) for path in setup_paths]

if not nobump:
    [bump(path=path,
          kind=bump_kind,
          ga=ga) for path in setup_paths]

if not bump_only:
    [build(
        path=path, action=action,
        upload=upload, dry_run=dry_run) for path in pkg_paths]
