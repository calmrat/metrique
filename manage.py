#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
CLI for deploying metrique
'''

import datetime
from functools import partial
import getpass
import glob
import importlib
import os
import re
import shlex
import shutil
import socket
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

HOSTNAME = socket.gethostname()

CWD = os.getcwd()
SRC_DIR = os.path.join(CWD, __src__)
USER_DIR = os.path.expanduser('~/.metrique')
TRASH_DIR = os.path.join(USER_DIR, 'trash')
LOGS_DIR = os.path.join(USER_DIR, 'logs')
ETC_DIR = os.path.join(USER_DIR, 'etc')
PID_DIR = os.path.join(USER_DIR, 'pids')
BACKUP_DIR = os.path.join(USER_DIR, 'backup')
MONGODB_DIR = os.path.join(USER_DIR, 'mongodb')

FIRSTBOOT_PATH = os.path.join(USER_DIR, '.firstboot')

SSL_CERT = os.path.join(ETC_DIR, 'metrique.crt')
SSL_KEY = os.path.join(ETC_DIR, 'metrique.key')
SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

METRIQUE_JSON = os.path.join(ETC_DIR, 'metrique.json')
METRIQUED_JSON = os.path.join(ETC_DIR, 'metriqued.json')
MONGODB_JSON = os.path.join(ETC_DIR, 'mongodb.json')

MONGODB_CONF = os.path.join(ETC_DIR, 'mongodb.conf')
MONGODB_PID = os.path.join(PID_DIR, 'mongodb.pid')
MONGODB_LOG = os.path.join(LOGS_DIR, 'mongodb.log')

# set cache dir so pip doesn't have to keep downloading over and over
PIP_CACHE = '~/.pip/download-cache'
PIP_ACCEL = '~/.pip-accel'
os.environ['PIP_DOWNLOAD_CACHE'] = PIP_CACHE
os.environ['PIP_ACCEL_CACHE'] = PIP_ACCEL
PIP_EGGS = os.path.join(USER_DIR, '.python-eggs')

USER = getpass.getuser()
NOW = datetime.datetime.utcnow().strftime('%FT%H:%M:%S')

DEFAULT_METRIQUE_JSON = '''
{
    "batch_size": 5000,
    "debug": true,
    "host": "127.0.0.1",
    "log2file": true,
    "logstdout": false,
    "password": "__UPDATE_PASSWORD",
    "port": 5420,
    "sql_batch_size": 1000,
    "ssl": true,
    "ssl_verify": false
}
'''
DEFAULT_METRIQUE_JSON = DEFAULT_METRIQUE_JSON.strip()

DEFAULT_METRIQUED_JSON = '''
{
    "cookie_secret": "____UPDATE_COOKIE_SECRET____",
    "debug": true,
    "host": "127.0.0.1",
    "krb_auth": false,
    "log2file": true,
    "logstdout": false,
    "port": 5420,
    "realm": "metrique",
    "ssl": true,
    "ssl_certificate": "%s",
    "ssl_certificate_key": "%s",
    "superusers": ["%s"]
}
''' % (SSL_CERT, SSL_KEY, USER)
DEFAULT_METRIQUED_JSON = DEFAULT_METRIQUED_JSON.strip()

DEFAULT_MONGODB_JSON = '''
{
    "auth": false,
    "admin_password": "",
    "data_password": "",
    "host": "127.0.0.1",
    "journal": true,
    "port": 27017,
    "ssl": true,
    "ssl_certificate": "%s",
    "write_concern": 1
}
''' % SSL_PEM
DEFAULT_MONGODB_JSON = DEFAULT_MONGODB_JSON.strip()

DEFAULT_MONGODB_CONF = '''
#auth = true
bind_ip = 127.0.0.1
fork = true
dbpath = %s
pidfilepath = %s
logpath = %s
noauth = true
nohttpinterface = true
sslOnNormalPorts = true
sslPEMKeyFile = %s
''' % (MONGODB_DIR, MONGODB_PID, MONGODB_LOG, SSL_PEM)
DEFAULT_MONGODB_CONF = DEFAULT_MONGODB_CONF.strip()


def activate(args):
    if (hasattr(args, 'virtenv') and args.virtenv):
        virtenv = args.virtenv
    elif isinstance(args, basestring):
        # virtenv path is passed in direct as a string
        virtenv = args
    activate_this = os.path.join(virtenv, 'bin', 'activate_this.py')
    assert os.path.exists(activate_this)
    execfile(activate_this, dict(__file__=activate_this))
    logger.info('Virtual Env (%s): Activated' % virtenv)


# Activate the virtual environment in this python session if
# parent env has one set
virtenv = os.environ.get('VIRTUAL_ENV')
if virtenv:
    activate(virtenv)


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


def call(cmd, cwd=None, stdout=True):
    if not cwd:
        cwd = os.getcwd()
    cmd = shlex.split(cmd.strip())
    logger.info("[%s] Running `%s` ..." % (cwd, ' '.join(cmd)))
    try:
        call_subprocess(cmd, cwd=cwd, show_stdout=stdout)
    except:
        sys.stderr.write(str(cmd))
        raise
    logger.info(" ... Done!")


def adjust_options(options, args):
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


def _celeryd_loop(args):
    call('celery worker -B --app %s' % args.tasks_mod)


def _celeryd_run(args):
    tasks = importlib.import_module(args.tasks_mod)
    task = getattr(tasks, args.task)
    return task.run()


def celeryd(args):
    '''
    '''
    activate(args)
    if args.loop:
        result = _celeryd_loop(args)
    else:
        result = _celeryd_run(args)
    return result


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

    config_file = os.path.expanduser(args.config_file)

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
        dest = os.path.join(TRASH_DIR, 'mongodb-%s' % NOW)
        shutil.move(db_dir, dest)
        makedirs(db_dir)
    else:
        raise ValueError("unknown command %s" % args.command)


def mongodb_backup(args):
    from metriqued.config import mongodb_config

    config = mongodb_config(args.config_file)

    out_dir = args.out_dir or BACKUP_DIR
    out_dir = os.path.expanduser(out_dir)
    makedirs(out_dir)

    prefix = 'mongodb'
    saveas = '__'.join((prefix, HOSTNAME, NOW))
    out = os.path.join(out_dir, saveas)

    host = config.host
    port = config.port
    p = config.admin_password
    password = '--password %s' % p if p else ''
    username = '--username %s' % config.admin_user if password else ''
    authdb = '--authenticationDatabase admin' if password else ''
    ssl = '--ssl' if config.ssl else ''

    cmd = ('mongodump', '--host %s' % host, '--port %s' % port,
           ssl, username, password, '--out %s' % out, authdb)
    cmd = ' '.join(cmd).replace('  ', ' ')
    call(cmd)

    if args.compress or args.scp_export:
        # compress if asked to or if we're going to export
        out_tgz = out + '.tar.gz'
        call('tar cvfz %s %s' % (out_tgz, out), stdout=False)
        shutil.rmtree(out)

    mongodb_clean(args, out_dir)

    if args.scp_export:
        user = args.scp_user
        host = args.scp_host
        out_dir = args.scp_out_dir
        cmd = 'scp %s %s@%s:%s' % (out_tgz, user, host, out_dir)
        call(cmd)


def mongodb_clean(args, path, prefix='mongodb'):
    keep = args.keep if args.keep != 0 else 3
    if args.compress:
        path = os.path.join(path, '%s*.tar.gz' % prefix)
    else:
        path = os.path.join(path, prefix)
    files = sorted(glob.glob(path), reverse=True)
    to_remove = files[keep:]
    logger.debug('Removing %i backups' % len(to_remove))
    [os.remove(f) for f in to_remove]


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

    logger.info('BUMP (RELEASE): %s->%s' % (current, bumped))
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
            logger.info('pip %s -e %s' % (cmd, abspath))
            os.system('pip %s -e %s' % (cmd, abspath))
        elif pip:
            logger.info('pip-accel %s -e %s' % (cmd, abspath))
            os.system('pip-accel %s -e %s' % (cmd, abspath))
        else:
            os.chdir(abspath)
            _cmd = ['python', 'setup.py'] + cmd.split(' ')
            logger.info(str(_cmd))
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

    # optional dependencies; highly recommended! but slow to
    # install if we're not testing
    if args.matplotlib or args.test:
        call('%s install -U matplotlib' % pip)
    if args.ipython:
        call('%s install -U ipython' % pip)
    if args.extras:
        call('%s install -U numexpr cython pytest' % pip)

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


def ssl(args=None):
    logger.info("Generating self-signed SSL certificate + key + combined pem")
    call('openssl req -new -x509 -days 365 -nodes '
         '-out %s -keyout %s -batch' % (SSL_CERT, SSL_KEY))
    with open(SSL_PEM, 'w') as pem:
        with open(SSL_CERT) as cert:
            pem.write(''.join(cert.readlines()))
        with open(SSL_KEY) as key:
            pem.write(''.join(key.readlines()))


def default_conf(path, template):
    if os.path.exists(path):
        path = '.'.join([path, 'default'])
    with open(path, 'w') as f:
        f.write(template)


def firstboot(args=None):
    if os.path.exists(FIRSTBOOT_PATH):
        # skip if we have already run this before
        return

    # create default dirs in advance
    [makedirs(p) for p in (USER_DIR, PIP_CACHE, PIP_ACCEL,
                           PIP_EGGS, TRASH_DIR, LOGS_DIR,
                           ETC_DIR, BACKUP_DIR, MONGODB_DIR)]

    # make sure the the default user python eggs dir is secure
    os.chmod(PIP_EGGS, 0700)

    # generate self-signed ssl certs
    ssl()

    # install default configuration files
    default_conf(METRIQUE_JSON, DEFAULT_METRIQUE_JSON)
    default_conf(METRIQUED_JSON, DEFAULT_METRIQUED_JSON)
    default_conf(MONGODB_JSON, DEFAULT_MONGODB_JSON)
    default_conf(MONGODB_CONF, DEFAULT_MONGODB_CONF)

    with open(FIRSTBOOT_PATH, 'w') as f:
        f.write(NOW)


def main():
    import argparse

    cli = argparse.ArgumentParser(description='Metrique Manage CLI')

    _sub = cli.add_subparsers(description='action')

    # Automated metrique deployment
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
        '--extras', action='store_true', help='install numexpr, cython, ...')
    _deploy.add_argument(
        '--matplotlib', action='store_true', help='install matplotlib')
    _deploy.set_defaults(func=deploy)

    # PIP standard build
    _build = _sub.add_parser('build')
    _build.set_defaults(func=build)

    # PIP sdist build
    _sdist = _sub.add_parser('sdist')
    _sdist.add_argument('-u', '--upload', action='store_true')
    _sdist.add_argument('-b', '--bump-r', action='store_true')
    _sdist.set_defaults(func=sdist)

    # PIP `develop` deployment
    _develop = _sub.add_parser('develop')
    _develop.set_defaults(func=develop)

    # PIP pkg register
    _register = _sub.add_parser('register')
    _register.set_defaults(func=register)

    # Bump metrique setup.py version/release
    _bump = _sub.add_parser('bump')
    _bump.add_argument('-k', '--bump-kind', choices=__bumps__)
    _bump.add_argument('-r', '--reset', choices=__bumps__)
    _bump.add_argument('-ga', action='store_true', dest='ga')
    _bump.set_defaults(func=bump)

    # PIP status
    _status = _sub.add_parser('status')
    _status.set_defaults(func=status)

    # Clean-up routines
    _clean = _sub.add_parser('clean')
    _clean.set_defaults(func=clean)

    # MongoDB Server
    _mongodb = _sub.add_parser('mongodb')
    _mongodb.add_argument('command',
                          choices=['start', 'stop', 'restart',
                                   'clean', 'trash'])
    _mongodb.add_argument('-c', '--config-file', type=str,
                          default=MONGODB_CONF)
    _mongodb.add_argument('-dd', '--db-dir', type=str,
                          default=MONGODB_DIR)
    _mongodb.add_argument('-pd', '--pid-dir', type=str,
                          default=PID_DIR)
    _mongodb.set_defaults(func=mongodb)

    # MongoDB Backup
    _mongodb_backup = _sub.add_parser('mongodb_backup')
    _mongodb_backup.add_argument('-c', '--config-file')
    _mongodb_backup.add_argument('-o', '--out-dir')
    _mongodb_backup.add_argument('-z', '--compress', action='store_true')
    _mongodb_backup.add_argument('-k', '--keep', type=int, default=3)
    _mongodb_backup.add_argument('-x', '--scp-export', action='store_true')
    _mongodb_backup.add_argument('-H', '--scp-host')
    _mongodb_backup.add_argument('-u', '--scp-user', default='backup')
    _mongodb_backup.add_argument('-O', '--scp-out-dir', default='~/')
    _mongodb_backup.set_defaults(func=mongodb_backup)

    # nginx Server
    _nginx = _sub.add_parser('nginx')
    _nginx.add_argument('command',
                        choices=['start', 'stop', 'reload',
                                 'restart', 'test'])
    _nginx.add_argument('config_file', type=str)
    _nginx.set_defaults(func=nginx)

    # metriqued Server
    _metriqued = _sub.add_parser('metriqued')
    _metriqued.add_argument('command', type=str)
    _metriqued.set_defaults(func=metriqued)

    # celery Server
    _celeryd = _sub.add_parser('celeryd')
    _celeryd.add_argument('command',
                          choices=['start', 'stop', 'restart'])
    _celeryd.add_argument('virtenv', type=str, nargs='?')
    _celeryd.add_argument('tasks_mod', type=str, nargs='?')
    _celeryd.add_argument('task', type=str, nargs='?')
    _celeryd.add_argument('-l', '--loop', action='store_true')
    _celeryd.set_defaults(func=celeryd)

    # SSL creation
    _ssl = _sub.add_parser('ssl')
    _ssl.set_defaults(func=ssl)

    # SSL creation
    _firstboot = _sub.add_parser('firstboot')
    _firstboot.set_defaults(func=firstboot)

    # parse argv
    args = cli.parse_args()

    # make sure we have some basic defaults configured in the environment
    if args.func is not firstboot:
        firstboot(args)

    # run command
    args.func(args)


if __name__ == '__main__':
    main()
