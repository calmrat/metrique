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

__pkgs__ = ['metrique', 'metriqued', 'metriquec', 'metriqueu']

GIT_LOCATION = 'https://github.com/drpoovilleorg/metrique.git'
SRC_DIR = 'src'
BUILD_DIR = 'metrique'
USER_DIR = '~/.metrique'

# set cache dir so pip doesn't have to keep downloading over and over
PIP_CACHE = os.path.expanduser(os.path.join(USER_DIR, 'pip'))
os.environ['PIP_DOWNLOAD_CACHE'] = PIP_CACHE


def extend_parser(parser):
    parser.add_option(
        '-P', '--packages',
        nargs='+',
        choices=__pkgs__ + ['all'],
        default='all',
        help='packages to install')

    parser.add_option(
        '-U', '--git-uri',
        default=GIT_LOCATION,
        help='git repository to use for the installation')

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
        '--notest',
        action='store_true',
        default=False,
        help='do not run tests after deployment completes')

virtualenv.extend_parser = extend_parser


def adjust_options(options, args):
    if not args:
        return  # caller will raise error
    # build the venv in a subdirectory
    base_dir = args[0]
    args[0] = os.path.join(base_dir, BUILD_DIR)
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


def after_install(options, home_dir):
    src_dir = os.path.join(home_dir, SRC_DIR)
    git_uri = options.git_uri
    git_branch = options.git_branch
    packages = options.packages

    makedirs(src_dir)

    install_dir = os.path.join(src_dir, BUILD_DIR)
    # if git_uri == '.'; assume cwd is a git env and work in it as-is
    if git_uri == '.':
        install_dir = os.getcwd()
        logger.notify('Using git repo at %s' % install_dir)
    elif not os.path.exists(install_dir):
        logger.notify('Installing from %s to %s' % (git_uri, install_dir))
        call_subprocess(['git', 'clone', git_uri, install_dir],
                        show_stdout=True)

    if not options.nopull and git_uri != '.':
        os.chdir(install_dir)
        logger.notify('Pulling %s' % git_branch)
        call_subprocess(['git', 'checkout', git_branch], show_stdout=True)
        call_subprocess(['git', 'pull'], show_stdout=True)

    call_subprocess(['git', '--no-pager', 'status'],
                    show_stdout=True)

    call_subprocess(['git', '--no-pager', 'log', '-3'],
                    show_stdout=True)

    activate = os.path.abspath(os.path.join(home_dir, 'bin',
                                            'activate_this.py'))
    py = os.path.abspath(os.path.join(home_dir, 'bin', 'python'))
    pytest = os.path.abspath(os.path.join(home_dir, 'bin', 'py.test'))
    pip = os.path.abspath(os.path.join(home_dir, 'bin', 'pip'))

    # activate the virtenv so all actions will be made within it
    execfile(activate, dict(__file__=activate))

    # make sure we have the installer basics and their up2date
    call_subprocess(
        [pip, 'install', '-U', 'pip', 'distribute', 'setuptools'],
        cwd=os.path.abspath(install_dir),
        show_stdout=True)
    # this is needed for py2.6
    call_subprocess([pip, 'install', '-U', 'argparse'],
                    cwd=os.path.abspath(install_dir),
                    show_stdout=True)
    # this dependency is installed separately because virtenv
    # path resolution issues; fails due to being unable to find
    # the python headers in the virtenv for some reason.
    call_subprocess([pip, 'install', '-U', 'pandas'],
                    cwd=os.path.abspath(install_dir),
                    show_stdout=True)
    # FIXME: any reason user might want to use 'develop' here?
    call_subprocess([py, 'build.py', 'install', '--packages', packages],
                    cwd=os.path.abspath(install_dir),
                    show_stdout=True)

    # make the user directory, where configs and logs, etc are stored
    if not os.path.exists(USER_DIR):
        makedirs(USER_DIR)

    if not options.notest:
        call_subprocess(
            [pip, 'install', 'pytest'],
            cwd=os.path.abspath(install_dir),
            show_stdout=True)
        os.chdir(install_dir)
        call_subprocess([pytest],
                        cwd=os.path.abspath(install_dir),
                        show_stdout=True)
virtualenv.after_install = after_install


def makedirs(_dir):
    _dir = os.path.expanduser(_dir)
    if not os.path.exists(_dir):
        logger.info('Creating directory %s' % _dir)
        os.makedirs(_dir)


if __name__ == '__main__':
    virtualenv.main()
