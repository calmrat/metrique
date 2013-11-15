#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
CLI for deploying metrique

'''

import os
import sys
import virtualenv

logger = virtualenv.Logger([(0, sys.stdout)])
call_subprocess = virtualenv.call_subprocess

__pkgs__ = ['metrique', 'metriqued', 'metriquec', 'metriqueu',
            'plotrique']

USER_DIR = os.path.expanduser('~/.metrique')

# set cache dir so pip doesn't have to keep downloading over and over
PIP_CACHE = os.path.join(USER_DIR, 'pip')
PIP_EGGS = os.path.join(USER_DIR, '.python-eggs')
os.environ['PIP_DOWNLOAD_CACHE'] = PIP_CACHE

# make sure the the default user python eggs dir is secure
if not os.path.exists(PIP_EGGS):
    os.makedirs(PIP_EGGS, 0700)
else:
    os.chmod(PIP_EGGS, 0700)

expand = os.path.expanduser
makedirs = lambda x: os.makedirs(expand(x)) if not os.path.exists(x) else None

for _dir in [USER_DIR]:
    # create dirs we need, in advance
    makedirs(_dir)


def call(cmd, cwd=None):
    if not cwd:
        cwd = os.getcwd()
    cmd = cmd.strip().split(' ')
    try:
        call_subprocess(cmd, cwd=cwd, show_stdout=True)
    except:
        logger.notify(str(cmd))
        raise


def extend_parser(parser):
    parser.add_option(
        '-a', '--action',
        default='install',
        choices=['install', 'develop'],
        help='setup.py action to execute')

    parser.add_option(
        '-P', '--packages',
        action='append',
        choices=__pkgs__ + ['all'],
        default=[],
        help='packages to install')

    parser.add_option(
        '-U', '--git-uri',
        default='.',
        help='git repository to use for the installation (DEFAULT: ".")')

    parser.add_option(
        '-B', '--git-branch',
        default='master',
        help='git branch to install (default: master)')

    parser.add_option(
        '--nopull',
        action='store_true',
        default=False,
        help='do not update (pull) git branch before install')

    parser.add_option(
        '--pypi',
        action='store_true',
        default=False,
        help='install pypi packages (not git)')

    parser.add_option(
        '--test',
        action='store_true',
        default=False,
        help='run tests after deployment completes')

    parser.add_option(
        '--ipython',
        action='store_true',
        default=False,
        help='install ipython')

virtualenv.extend_parser = extend_parser


def adjust_options(options, args):
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


def after_install(options, home_dir):
    git_uri = options.git_uri
    git_branch = options.git_branch
    pkgs = options.packages
    action = options.action
    pypi = options.pypi

    src_dir = os.getcwd()
    if git_uri == '.' and os.path.exists('.git'):
        # if git_uri == '.'; assume cwd is target git repo
        logger.notify('Using git repo at %s' % src_dir)
    elif not os.path.exists(src_dir):
        logger.notify('Installing %s -> %s' % (git_uri, home_dir))
        call('git clone %s %s' % (git_uri, home_dir))
        src_dir = os.path.join(home_dir, 'metrique')
    else:
        raise IOError("cwd is not a git repo! (%s)" % src_dir)

    if not options.nopull and git_uri != '.':
        call('git checkout %s' % git_branch)
        call('git pull')

    # some debug output
    call('git --no-pager status')
    call('git --no-pager log -3')

    pip = os.path.join(home_dir, 'bin', 'pip')
    pipa = os.path.join(home_dir, 'bin', 'pip-accel')
    activate = os.path.join(home_dir, 'bin', 'activate_this.py')
    pytest = os.path.join(home_dir, 'bin', 'py.test')

    # force override existing 'activated' virt envs, if there is one
    os.environ['VIRTUAL_ENV'] = home_dir
    # activate the virtenv so all actions will be made within it
    execfile(activate, dict(__file__=activate))

    # make sure we have the installer basics and their up2date
    # argparse is needed for py2.6; pip-accel caches compiled binaries
    # first run for a new virt-env will take forever...
    # second run should be 90% faster!
    call('%s install -U pip-accel' % pip)
    call('%s install -U pip distribute setuptools' % pipa)
    call('%s install -U argparse virtualenv' % pipa)

    # this dependency is installed separately because virtenv
    # path resolution issues; fails due to being unable to find
    # the python headers in the virtenv for some reason.
    if not pkgs == ['metriqued']:
        call('%s install -U pandas' % pipa)
    # optional dependency, that's highly recommended!
    if options.ipython:
        call('%s install -U ipython' % pipa)
    # install all metrique base packages or only the selected ones
    if pkgs:
        pkgs = ['-P'] + pkgs

    cmd = './build.py %s' % action
    cmd += ' --pypi ' if pypi else ''
    cmd += ' '.join(pkgs)
    call(cmd)

    # run py.test after install
    if options.test:
        call('%s install pytest' % pipa)
        call(pytest, cwd=src_dir)
virtualenv.after_install = after_install


if __name__ == '__main__':
    virtualenv.main()
