#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
CLI for deploying metrique
'''

import datetime
from functools import partial
import os
import re
import shlex
import shutil
import sys

try:
    import virtualenv
except ImportError:
    os.system('pip install virtualenv')
finally:
    logger = virtualenv.Logger([(0, sys.stdout)])
    call_subprocess = virtualenv.call_subprocess

__pkgs__ = ['metrique', 'metriqued', 'metriquec', 'metriqueu',
            'plotrique']
__src__ = 'src/'
__actions__ = ['build', 'sdist', 'install', 'develop', 'register',
               'bump', 'status']
__bumps__ = ['x', 'y', 'z', 'r']
RE_VERSION_X = re.compile(r"__version__\s+=\s+[\"']((\d+).\d+.\d+)[\"']")
RE_VERSION_Y = re.compile(r"__version__\s+=\s+[\"'](\d+.(\d+).\d+)[\"']")
RE_VERSION_Z = re.compile(r"__version__\s+=\s+[\"'](\d+.\d+.(\d+))[\"']")
RE_RELEASE = re.compile(r"__release__ = [\"']?((\d+)a?)[\"']?")

CWD = os.getcwd()
SRC_DIR = os.path.join(CWD, __src__)
USER_DIR = os.path.expanduser('~/.metrique')
TRASH_DIR = os.path.expanduser('~/.metrique/trash')

# set cache dir so pip doesn't have to keep downloading over and over
PIP_CACHE = '~/.pip/download-cache'
PIP_ACCEL = '~/.pip-accel'
os.environ['PIP_DOWNLOAD_CACHE'] = PIP_CACHE
os.environ['PIP_ACCEL_CACHE'] = PIP_ACCEL
PIP_EGGS = os.path.join(USER_DIR, '.python-eggs')


def get_pid(pid_file):
    try:
        return int(''.join(open(pid_file).readlines()).strip())
    except IOError:
        return 0


def makedirs(path, mode=0700):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        os.makedirs(path, mode)
    return path


def remove(path):
    if os.path.exists(path):
        os.remove(path)


def call(cmd, cwd=None):
    if not cwd:
        cwd = os.getcwd()
    cmd = shlex.split(cmd.strip())
    try:
        call_subprocess(cmd, cwd=cwd, show_stdout=True)
    except:
        sys.stderr.write(str(cmd))
        raise


def adjust_options(options, args):
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


def celeryd(args):
    '''
    START, STOP, RESTART, RELOAD,
    '''
    call('celery --help')
    return


def metriqued(args):
    '''
    START, STOP, RESTART, RELOAD,
    '''
    call('metriqued %s' % args.command)


def nginx(args):
    if os.getuid() != 0:
        raise RuntimeError("must be run as root")
    if not args.config_file:
        logger.warn("nginx config is broken")
        raise ImportError("nginx config is broken")
    else:
        config_file = os.path.expanduser(args.config_file)

    cmd = 'nginx -c %s' % config_file
    if args.command == 'test':
        return call('%s -t' % cmd)
    elif args.command == 'start':
        return call(cmd)
    elif args.command == 'stop':
        return call('%s -s stop' % cmd)
    elif args.command == 'restart':
        for cmd in ('stop', 'start'):
            args.command = cmd
            nginx(args)
    elif args.command == 'reload':
        return call('%s -s reload' % cmd)
    else:
        raise ValueError("unknown command %s" % args.command)


def mongodb(args):
    db_dir = makedirs(args.db_dir)
    lock_file = os.path.join(db_dir, 'mongod.lock')

    config_dir = makedirs(args.config_dir)
    config_file = os.path.join(config_dir, args.config_file)

    pid_dir = makedirs(args.pid_dir)
    pid_file = os.path.join(pid_dir, 'mongodb.pid')

    if args.command == 'start':
        return call('mongod -f %s --fork' % config_file)
    elif args.command == 'stop':
        signal = 15
        pid = get_pid(pid_file)
        code = os.kill(pid, signal)
        args.command = 'clean'
        mongodb(args)
        return code
    elif args.command == 'restart':
        for cmd in ('stop', 'start'):
            args.command = cmd
            mongodb(args)
    elif args.command == 'clean':
        remove(lock_file)
        remove(pid_file)
    elif args.command == 'trash':
        now = datetime.datetime.now().isoformat()
        dest = os.path.join(TRASH_DIR, 'mongodb-%s' % now)
        shutil.move(db_dir, dest)
        makedirs(db_dir)
    else:
        raise ValueError("unknown command %s" % args.command)


def clean(args):
    # http://stackoverflow.com/a/785534/1289080
    os.system('find . -name "*.pyc" -exec rm -f {} \;')


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

    logger.notify('BUMP (RELEASE): %s->%s' % (current, bumped))
    return '__release__ = %s\n' % bumped


def get_packages(args):
    global __pkgs__
    global SRC_DIR
    return [os.path.join(SRC_DIR, pkg) for pkg in __pkgs__]


def update_line(args, regex, bump_func):
    pkg_paths = get_packages(args)
    for path in pkg_paths:
        path = '%s/setup.py' % path
        with open(path) as setup_py:
            content = setup_py.readlines()
            for i, line in enumerate(content):
                if regex.match(line):
                    content[i] = bump_func(line)
                else:
                    continue
                break  # stop after the first replace...
        if not content:
            raise ValueError(
                "content was empty; didn't want to overwrite...")
        elif None in content:
            raise RuntimeError(
                "check your nrv strings; "
                " they're missing our regex: \n%s" % regex.pattern)
        content_str = ''.join(content)
        # write out the new setup file with bumped
        with open(path, 'w') as setup_py:
            setup_py.write(content_str)


def activate(args):
    if (hasattr(args, 'virtenv') and args.virtenv):
        activate = os.path.join(args.virtenv, 'bin', 'activate_this.py')
        execfile(activate, dict(__file__=activate))
        logger.notify('Virtual Env (%s): Activated' % args.virtenv)


def setup(args, cmd, pip=False):
    global __pkgs__
    if isinstance(cmd, basestring):
        cmd = cmd.strip()
    else:
        cmd = ' '.join([s.strip() for s in cmd])
    cwd = os.getcwd()
    for path in __pkgs__:
        abspath = os.path.join(cwd, 'src', path)
        if pip and args.slow:
            logger.notify('pip %s -e %s' % (cmd, abspath))
            os.system('pip %s -e %s' % (cmd, abspath))
        elif pip:
            logger.notify('pip-accel %s -e %s' % (cmd, abspath))
            os.system('pip-accel %s -e %s' % (cmd, abspath))
        else:
            os.chdir(abspath)
            _cmd = ['python', 'setup.py'] + cmd.split(' ')
            logger.notify(str(_cmd))
            call_subprocess(_cmd, show_stdout=True)
    os.chdir(cwd)


def deploy(args):
    '''
    Order of Operations
    (NOT IN VIRTENV)
    + clone repo/pull branch
    + bump r
    + sdist upload
    (IN VIRTENV)
    + install dep
    + install metrique and friends
    '''
    virtenv = getattr(args, 'virtenv', None)
    if virtenv:
        # virtualenv.main; pass in only the virtenv path
        sys.argv = sys.argv[0:1] + [virtenv]
        # run the virtualenv script to install the virtenv
        virtualenv.main()
        # activate the newly installed virtenv
        activate(args)

    pip = 'pip' if args.slow else 'pip-accel'
    # make sure we have the installer basics and their up2date
    # argparse is needed for py2.6; pip-accel caches compiled binaries
    # first run for a new virt-env will take forever...
    # second run should be 90% faster!
    call('pip install -U pip setuptools')
    call('pip install -U %s virtualenv argparse' % pip)

    # this required dep is installed separately b/c virtenv
    # path resolution issues; fails due to being unable to find
    # the python headers in the virtenv for some reason.
    call('%s install -U numpy pandas' % pip)
    call('%s install -U numexpr cython' % pip)

    # optional dependencies; highly recommended! but slow to
    # install if we're not testing
    if args.matplotlib or args.test:
        call('%s install -U matplotlib' % pip)
    if args.ipython:
        call('%s install -U ipython' % pip)

    cmd = 'install'
    no_pre = getattr(args, 'no_pre', False)
    if not no_pre:
        cmd += ' --pre'
    setup(args, cmd, pip=True)

    # run py.test after install
    if args.test:
        call('%s install -U pytest' % pip)
        for pkg in __pkgs__:
            if pkg == 'plotrique' and not args.matplotlib:
                continue
            else:
                call('py.test tests/%s' % pkg)


def bump(args, kind=None, reset=None):
    kind = kind or getattr(args, 'bump_kind', None) or 'r'
    reset = reset or getattr(args, 'reset', None)
    ga = getattr(args, 'ga', None)
    global __bumps__
    assert kind in __bumps__
    if kind == 'x':
        regex = RE_VERSION_X
        bump(args=args, kind='y', reset=True)
        bump_func = partial(bump_version_x, reset=reset)
    elif kind == 'y':
        regex = RE_VERSION_Y
        bump(args=args, kind='z', reset=True)
        bump_func = partial(bump_version_y, reset=reset)
    elif kind == 'z':
        regex = RE_VERSION_Z
        bump(args=args, kind='r', reset=True)
        bump_func = partial(bump_version_z, reset=reset)
    elif kind == 'r':
        regex = RE_RELEASE
        bump_func = partial(bump_release, reset=reset, ga=ga)
    update_line(args, regex, bump_func)


def build(args):
    cmd = 'build'
    setup(args, cmd)


def sdist(args, upload=None, bump_r=None):
    upload = upload or args.upload
    bump_r = bump_r or args.bump_r
    cmd = 'sdist'
    if upload:
        if bump_r:
            bump(args)
        cmd += ' upload'
    setup(args, cmd)


def develop(args):
    cmd = 'develop'
    setup(args, cmd)


def register(args):
    cmd = 'register'
    setup(args, cmd)


def status(path):
    pkg = os.path.basename(os.path.dirname(path))
    call('pip show %s' % pkg)


def main():
    import argparse

    # create default dirs in advance
    [makedirs(p) for p in (USER_DIR, PIP_CACHE, PIP_ACCEL,
                           PIP_EGGS, TRASH_DIR)]

    # make sure the the default user python eggs dir is secure
    os.chmod(PIP_EGGS, 0700)

    cli = argparse.ArgumentParser(description='Metrique Manage CLI')

    _sub = cli.add_subparsers(description='action')

    _deploy = _sub.add_parser('deploy')
    _deploy.add_argument('virtenv', type=str, nargs='?')
    _deploy.add_argument(
        '--slow', action='store_true', help="don't use pip-accel")
    _deploy.add_argument(
        '--no-pre', action='store_true',
        help='ignore pre-release versions')
    _deploy.add_argument(
        '--test', action='store_true', help='run tests after deployment')
    _deploy.add_argument(
        '--ipython', action='store_true', help='install ipython')
    _deploy.add_argument(
        '--matplotlib', action='store_true', help='install matplotlib')
    _deploy.set_defaults(func=deploy)

    _build = _sub.add_parser('build')
    _build.set_defaults(func=build)

    _sdist = _sub.add_parser('sdist')
    _sdist.add_argument('-u', '--upload', action='store_true')
    _sdist.add_argument('-b', '--bump-r', action='store_true')
    _sdist.set_defaults(func=sdist)

    _develop = _sub.add_parser('develop')
    _develop.set_defaults(func=develop)

    _register = _sub.add_parser('register')
    _register.set_defaults(func=register)

    _bump = _sub.add_parser('bump')
    _bump.add_argument('-k', '--bump-kind', choices=__bumps__)
    _bump.add_argument('-r', '--reset', choices=__bumps__)
    _bump.add_argument('-ga', action='store_true', dest='ga')
    _bump.set_defaults(func=bump)

    _status = _sub.add_parser('status')
    _status.set_defaults(func=status)

    _clean = _sub.add_parser('clean')
    _clean.set_defaults(func=clean)

    _mongodb = _sub.add_parser('mongodb')
    _mongodb.add_argument('command',
                          choices=['start', 'stop', 'restart',
                                   'clean', 'trash'])
    _mongodb.add_argument('-c', '--config-file', type=str,
                          default='mongodb.conf')
    _mongodb.add_argument('-cd', '--config-dir', type=str,
                          default='~/.metrique/etc')
    _mongodb.add_argument('-dd', '--db-dir', type=str,
                          default='~/.metrique/mongodb')
    _mongodb.add_argument('-pd', '--pid-dir', type=str,
                          default='~/.metrique/pids')
    _mongodb.set_defaults(func=mongodb)

    _nginx = _sub.add_parser('nginx')
    _nginx.add_argument('command',
                        choices=['start', 'stop', 'reload',
                                 'restart', 'test'])
    _nginx.add_argument('-c', '--config-file', type=str)
    _nginx.set_defaults(func=nginx)

    _metriqued = _sub.add_parser('metriqued')
    _metriqued.add_argument('command', type=str)
    _metriqued.set_defaults(func=metriqued)

    _celeryd = _sub.add_parser('celeryd')
    _celeryd.add_argument('command',
                          choices=['start', 'stop', 'restart'])
    _celeryd.add_argument('cwd', type=str)
    _celeryd.set_defaults(func=celeryd)

    # parse argv
    args = cli.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
