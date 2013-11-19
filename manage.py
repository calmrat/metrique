#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
CLI for deploying metrique
'''

from functools import partial
import os
import sys
import re

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

# set cache dir so pip doesn't have to keep downloading over and over
PIP_CACHE = '~/.pip/download-cache'
PIP_ACCEL = '~/.pip-accel'
os.environ['PIP_DOWNLOAD_CACHE'] = PIP_CACHE
os.environ['PIP_ACCEL_CACHE'] = PIP_ACCEL
PIP_EGGS = os.path.join(USER_DIR, '.python-eggs')

# create dirs we need, in advance
for _dir in [USER_DIR, PIP_CACHE, PIP_ACCEL, PIP_EGGS]:
    _dir = os.path.expanduser(_dir)
    if not os.path.exists(_dir):
        os.makedirs(_dir)

# make sure the the default user python eggs dir is secure
os.chmod(PIP_EGGS, 0700)


def call(cmd, cwd=None):
    if not cwd:
        cwd = os.getcwd()
    cmd = cmd.strip().split(' ')
    try:
        call_subprocess(cmd, cwd=cwd, show_stdout=True)
    except:
        sys.stderr.write(str(cmd))
        raise


def adjust_options(options, args):
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


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
            setup_py = os.path.join(abspath, 'setup.py')
            setup_py = re.sub('\s+', ' ', setup_py)
            _cmd = ['python', setup_py] + cmd.split(' ')
            logger.notify(str(_cmd))
            call_subprocess(_cmd, show_stdout=True)


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

    # optional dependencies; highly recommended! but slow for testing
    if args.pandas:
        # this dependency is installed separately b/c virtenv
        # path resolution issues; fails due to being unable to find
        # the python headers in the virtenv for some reason.
        call('%s install -U numpy pandas' % pip)
    if args.matplotlib:
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
    kind = kind or getattr(args, 'bump_kind', 'r')
    reset = reset or getattr(args, 'reset', None)
    ga = getattr(args, 'ga', None)
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


def sdist(args, upload=None):
    upload = upload or args.upload
    cmd = 'sdist'
    cmd += ' upload' if upload else ''
    setup(args, cmd)


def develop(args):
    cmd = 'develop'
    setup(args, cmd)


def register(args):
    cmd = 'register'
    setup(args, cmd)


def status(path):
    #[status(path=path) for path in setup_paths]
    pkg = os.path.basename(os.path.dirname(path))
    p = setup('pip show %s' % pkg)
    print p.communicate()


if __name__ == '__main__':
    import argparse

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
        '--pandas', action='store_true', help='install pandas')
    _deploy.add_argument(
        '--ipython', action='store_true', help='install ipython')
    _deploy.add_argument(
        '--matplotlib', action='store_true', help='install matplotlib')
    _deploy.set_defaults(func=deploy)

    _build = _sub.add_parser('build')
    _build.set_defaults(func=build)

    _sdist = _sub.add_parser('sdist')
    _sdist.add_argument('-u', '--upload', action='store_true')
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
    # parse argv
    args = cli.parse_args()
    args.func(args)
