#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>


'''
CLI for building metrique with setup.py and distributing via pypi pip

metrique (https://pypi.python.org/pypi/metrique)
metriquec (https://pypi.python.org/pypi/metriquec)
metriqued (https://pypi.python.org/pypi/metriqued)
metriqueu (https://pypi.python.org/pypi/metriqueu)
plotrique (https://pypi.python.org/pypi/plotrique)
'''

import argparse
from functools import partial
import os
import logging
import subprocess as sp
import re

logging.basicConfig(format='')
logger = logging.getLogger('metrique')

# header definitions
__pkgs__ = ['metrique', 'metriqued', 'metriquec', 'metriqueu',
            'plotrique']
__src__ = 'src/'
__actions__ = ['build', 'sdist', 'install', 'develop', 'register',
               'bump', 'status']
__bumps__ = ['x', 'y', 'z', 'r']
#RE_RELEASE = re.compile(r"__release__ = [\"']?((\d+)a?)[\"']?")
RE_VERSION_X = re.compile(r"__version__\s+=\s+[\"']((\d+).\d+.\d+)[\"']")
RE_VERSION_Y = re.compile(r"__version__\s+=\s+[\"'](\d+.(\d+).\d+)[\"']")
RE_VERSION_Z = re.compile(r"__version__\s+=\s+[\"'](\d+.\d+.(\d+))[\"']")
RE_RELEASE = re.compile(r"__release__ = [\"']?((\d+)a?)[\"']?")

# init cli argparser
cli = argparse.ArgumentParser(
    description='Metrique Build CLI')
cli.add_argument('action',
                 choices=__actions__)
cli.add_argument('-d', '--debug',
                 action='store_true',
                 default=False)
cli.add_argument('-P', '--packages',
                 choices=__pkgs__ + ['all'],
                 default='all')
cli.add_argument('--no-mirrors',
                 action='store_true',
                 default=False)
cli.add_argument('-u', '--upload',
                 action='store_true',
                 default=False)
cli.add_argument('--bump-kind',
                 choices=__bumps__)
cli.add_argument('--ga',
                 action='store_true',
                 default=False)
cli.add_argument('--pypi',
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
if args.packages == 'all':
    pkg_paths = [os.path.join(SRC, pkg) for pkg in __pkgs__]
else:
    pkg_paths = [os.path.join(SRC, args.packages)]

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
    else:
        # drop any 'a' if there is one
        bumped = re.sub('a$', '', str(bumped))

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


def bump(path, kind=None, reset=False, ga=False):
    if kind is None:
        kind = 'r'
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


def call(cmd):
    _cmd = cmd.strip().split(' ')
    logger.info('(%s) %s' % (os.getcwd(), str(_cmd)))
    try:
        p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        print p.communicate()
    except:
        raise


def install(path, args, develop=False):
    os.chdir(path)

    virtenv = os.environ.get('VIRTUAL_ENV')
    if virtenv:
        activate = os.path.join(virtenv, 'bin', 'activate_this.py')
        execfile(activate, dict(__file__=activate))

    cmd = 'pip-accel install -U '

    if args.pypi:
        # install from pypi
        if args.upload:
            # build and upload the current version
            build(path=path, args=args, sdist=True)
        pkg = os.path.basename(path)
        cmd += pkg
    else:
        # install from local repo
        cmd += '-e %s' % path
    cmd += '' if args.no_mirrors else ' --use-mirrors'
    call(cmd)


def build(path, args, sdist=False):
    os.chdir(path)
    action = 'sdist' if sdist else 'build'
    cmd = './setup.py %s' % action
    cmd += ' upload' if args.upload else ''
    call(cmd)


def register(path):
    os.chdir(path)
    cmd = './setup.py register'
    call(cmd)


def status(path):
    pkg = os.path.basename(os.path.dirname(path))
    p = call('pip show %s' % pkg)
    print p.communicate()


def fast_check():
    # make sure we have pip-accel available
    try:
        call('pip-accel')
    except OSError:
        call('pip install pip-accel')


if __name__ == '__main__':
    if args.action == 'status':
        [status(path=path) for path in setup_paths]
    elif args.action == 'register':
        [register(path=path) for path in setup_paths]
    elif args.action == 'bump':
        [bump(path=path, kind=args.bump_kind,
              ga=args.ga) for path in setup_paths]
    else:
        if args.action == 'build':
            [build(path=path, args=args, sdist=False) for path in pkg_paths]
        elif args.action == 'sdist':
            [build(path=path, args=args, sdist=True) for path in pkg_paths]
        elif args.action == 'install':
            fast_check()
            [install(path=path, args=args,
                     develop=False) for path in pkg_paths]
        elif args.action == 'develop':
            fast_check()
            [install(path=path, args=args,
                     develop=True) for path in pkg_paths]
        else:
            raise ValueError("Unknown action: %s" % args.action)
