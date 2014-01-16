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
import random
import re
import signal
import shlex
import shutil
import socket
import string
import sys
import time

try:
    import virtualenv
except ImportError:
    os.system('pip install virtualenv')
finally:
    virtenv_activated = False
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
LOCAL_IP = socket.gethostbyname(HOSTNAME)

CWD = os.getcwd()
SRC_DIR = os.path.join(CWD, __src__)
USER_DIR = os.path.expanduser('~/.metrique')
TRASH_DIR = os.path.join(USER_DIR, 'trash')
LOGS_DIR = os.path.join(USER_DIR, 'logs')
ETC_DIR = os.path.join(USER_DIR, 'etc')
PID_DIR = os.path.join(USER_DIR, 'pids')
BACKUP_DIR = os.path.join(USER_DIR, 'backup')
TEMP_DIR = os.path.join(USER_DIR, 'tmp')
CACHE_DIR = os.path.join(USER_DIR, 'cache')
MONGODB_DIR = os.path.join(USER_DIR, 'mongodb')
CELERY_DIR = os.path.join(USER_DIR, 'celery')
GNUPG_DIR = os.path.join(USER_DIR, 'gnupg')

cwd = os.getcwd()
STATIC_PATH = os.path.join(cwd, 'src/metriqued/metriqued/static/')

SYS_FIRSTBOOT_PATH = os.path.join(USER_DIR, '.firstboot_sys')
METRIQUED_FIRSTBOOT_PATH = os.path.join(USER_DIR, '.firstboot_metriqued')
METRIQUED_JSON = os.path.join(ETC_DIR, 'metriqued.json')
METRIQUE_JSON = os.path.join(ETC_DIR, 'metrique.json')

SSL_CERT = os.path.join(ETC_DIR, 'metrique.crt')
SSL_KEY = os.path.join(ETC_DIR, 'metrique.key')
SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

MONGODB_FIRSTBOOT_PATH = os.path.join(USER_DIR, '.firstboot_mongodb')
MONGODB_CONF = os.path.join(ETC_DIR, 'mongodb.conf')
MONGODB_PID = os.path.join(PID_DIR, 'mongodb.pid')
MONGODB_LOG = os.path.join(LOGS_DIR, 'mongodb.log')
MONGODB_JSON = os.path.join(ETC_DIR, 'mongodb.json')
MONGODB_JS = os.path.join(ETC_DIR, 'mongodb.js')
MONGODB_KEYFILE = os.path.join(ETC_DIR, 'mongodb.key')

CELERY_JSON = os.path.join(ETC_DIR, 'celery.json')
CELERY_PIDFILE = os.path.expanduser('~/.metrique/pids/celeryd.pid')
CELERY_LOG = os.path.expanduser('~/.metrique/logs/celeryd.log')

NGINX_CONF = os.path.join(ETC_DIR, 'nginx.conf')
NGINX_ACCESS_LOG = os.path.join(LOGS_DIR, 'nginx_access.log')
NGINX_ERROR_LOG = os.path.join(LOGS_DIR, 'nginx_error.log')
NGINX_PIDFILE = os.path.join(PID_DIR, 'nginx.pid')

# set cache dir so pip doesn't have to keep downloading over and over
PIP_CACHE = '~/.pip/download-cache'
PIP_ACCEL = '~/.pip-accel'
os.environ['PIP_DOWNLOAD_CACHE'] = PIP_CACHE
os.environ['PIP_ACCEL_CACHE'] = PIP_ACCEL
PIP_EGGS = os.path.join(USER_DIR, '.python-eggs')


def rand_chars(size=6, chars=string.ascii_uppercase + string.digits):
    # see: http://stackoverflow.com/questions/2257441
    return ''.join(random.choice(chars) for x in range(size))


USER = getpass.getuser()
NOW = datetime.datetime.utcnow().strftime('%FT%H:%M:%S')

DEFAULT_METRIQUE_JSON = '''
{
    "auto_login": false,
    "batch_size": 5000,
    "cube_autoregister": false,
    "debug": true,
    "host": "127.0.0.1",
    "log2file": true,
    "logstdout": false,
    "password": "%s",
    "port": 5420,
    "sql_batch_size": 1000,
    "ssl": false,
    "ssl_verify": false
}
''' % (rand_chars())
DEFAULT_METRIQUE_JSON = DEFAULT_METRIQUE_JSON.strip()

DEFAULT_METRIQUED_JSON = '''
{
    "cookie_secret": "%s",
    "debug": true,
    "host": "127.0.0.1",
    "krb_auth": false,
    "log2file": true,
    "logstdout": false,
    "port": 5420,
    "realm": "metrique",
    "ssl": false,
    "ssl_certificate": "%s",
    "ssl_certificate_key": "%s",
    "superusers": ["admin", "%s"]
}
''' % (rand_chars(20), SSL_CERT, SSL_KEY, USER)
DEFAULT_METRIQUED_JSON = DEFAULT_METRIQUED_JSON.strip()

root_password = rand_chars()
admin_password = rand_chars()
data_password = rand_chars()

DEFAULT_MONGODB_JSON = '''
{
    "auth": false,
    "root_password": "%s",
    "admin_password": "%s",
    "data_password": "%s",
    "host": "127.0.0.1",
    "journal": true,
    "port": 27017,
    "ssl": false,
    "ssl_certificate": "%s",
    "write_concern": 1
}
''' % (root_password, admin_password, data_password, SSL_PEM)
DEFAULT_MONGODB_JSON = DEFAULT_MONGODB_JSON.strip()

DEFAULT_MONGODB_CONF = '''
fork = true
nohttpinterface = true
dbpath = %s
logpath = %s
pidfilepath = %s

#auth = true
noauth = true

bind_ip = 127.0.0.1
#bind_ip = %s

#sslOnNormalPorts = true
#sslPEMKeyFile = %s

#replSet = rs0
#keyFile = %s
''' % (LOCAL_IP, MONGODB_DIR, MONGODB_LOG, MONGODB_PID, SSL_PEM,
       MONGODB_KEYFILE)
DEFAULT_MONGODB_CONF = DEFAULT_MONGODB_CONF.strip()

DEFAULT_MONGODB_JS = '''
db = db.getSiblingDB('admin')
db.addUser({'user': 'root', 'pwd': '%s', 'roles': ['dbAdminAnyDatabase',
           'userAdminAnyDatabase', 'clusterAdmin', 'readWriteAnyDatabase']});
db.addUser({'user': 'admin', 'pwd': '%s', 'roles': ['dbAdminAnyDatabase',
           'userAdminAnyDatabase', 'readWriteAnyDatabase']});
db.addUser({'user': 'metrique', 'pwd': '%s', 'roles': ['readAnyDatabase']});
''' % (root_password, admin_password, data_password)
DEFAULT_MONGODB_JS = DEFAULT_MONGODB_JS.strip()

DEFAULT_CELERY_JSON = '''
{
    "BROKER_URL": "mongodb://admin:%s@127.0.0.1:27017",
    "BROKER_USE_SSL": false
}
''' % (admin_password)
DEFAULT_CELERY_JSON = DEFAULT_CELERY_JSON.strip()

DEFAULT_NGINX_CONF = '''
worker_processes auto;

error_log %s;
pid %s;

events {
    worker_connections 1024;
    use epoll;
}

http {
    charset utf-8;
    client_max_body_size 0;  # disabled
    client_body_temp_path  %s 1 2;
    client_header_buffer_size 256k;
    large_client_header_buffers 8 1024k;

    proxy_temp_path   %s  1 2;
    proxy_cache_path  %s  levels=1:2     keys_zone=proxy_one:10m;

    fastcgi_temp_path   %s  1 2;
    fastcgi_cache_path  %s  levels=1:2   keys_zone=fastcgi_one:10m;

    uwsgi_temp_path   %s  1 2;
    uwsgi_cache_path  %s  levels=1:2     keys_zone=uwsgi_one:10m;

    scgi_temp_path   %s  1 2;
    scgi_cache_path  %s  levels=1:2     keys_zone=scgi_one:10m;

    # Enumerate all the Tornado servers here
    upstream frontends {
        server 127.0.0.1:5421;
        server 127.0.0.1:5422;
        server 127.0.0.1:5423;
        server 127.0.0.1:5424;
    }

    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    error_log %s;
    access_log %s;

    keepalive_timeout 65;
    proxy_read_timeout 200;
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    gzip on;
    gzip_min_length 1000;
    gzip_proxied any;
    gzip_types text/plain text/css text/xml
               application/x-javascript application/xml
               application/atom+xml text/javascript
               application/json;

    # Only retry if there was a communication error, not a timeout
    # on the Tornado server (to avoid propagating "queries of death"
    # to all frontends)
    proxy_next_upstream error;

    server {
        listen 127.0.0.1:5420;
        ssl                 off;
        ssl_certificate     %s;
        ssl_certificate_key %s;

        ssl_protocols        SSLv3 TLSv1 TLSv1.1 TLSv1.2;
        ssl_ciphers RC4:HIGH:!aNULL:!MD5;
        ssl_prefer_server_ciphers on;
        keepalive_timeout    60;
        ssl_session_cache    shared:SSL:10m;
        ssl_session_timeout  10m;

        location ^~ /static/ {
            root %s;
            if ($query_string) {
                expires max;
            }
        }

        location / {
            proxy_pass_header Server;
            proxy_set_header Host $http_host;
            proxy_redirect off;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Scheme $scheme;
            proxy_pass http://frontends;

            proxy_set_header        Accept-Encoding   "";
            proxy_set_header        X-Forwarded-For $proxy_add_x_forwarded_for;

            ### Most PHP, Python, Rails, Java App can use this header ###
            #proxy_set_header X-Forwarded-Proto https;##
            #This is better##
            proxy_set_header        X-Forwarded-Proto $scheme;
            add_header              Front-End-Https   on;

            ### force timeouts if one of backend is died ##
            proxy_next_upstream error timeout invalid_header http_500 http_502
                                                             http_503 http_504;
        }
    }
}
''' % (NGINX_ERROR_LOG, NGINX_PIDFILE, TEMP_DIR,
       TEMP_DIR, CACHE_DIR, TEMP_DIR, CACHE_DIR, TEMP_DIR, CACHE_DIR,
       TEMP_DIR, CACHE_DIR,
       NGINX_ERROR_LOG, NGINX_ACCESS_LOG, SSL_CERT, SSL_KEY, STATIC_PATH)

DEFAULT_NGINX_CONF = DEFAULT_NGINX_CONF.strip()


def activate(args=None):
    global virtenv_activated

    if virtenv_activated:
        return
    elif (hasattr(args, 'virtenv') and args.virtenv):
        virtenv = args.virtenv
    elif isinstance(args, basestring):
        # virtenv path is passed in direct as a string
        virtenv = args
    else:
        virtenv = os.environ.get('VIRTUAL_ENV')

    if virtenv:
        activate_this = os.path.join(virtenv, 'bin', 'activate_this.py')
        if os.path.exists(activate_this):
            execfile(activate_this, dict(__file__=activate_this))
            virtenv_activated = True
            logger.info('Virtual Env (%s): Activated' % virtenv)


# Activate the virtual environment in this python session if
# parent env has one set
activate()


def get_pid(pidfile):
    try:
        return int(''.join(open(pidfile).readlines()).strip())
    except IOError:
        return 0


def makedirs(path, mode=0700):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        os.makedirs(path, mode)
    return path


def remove(path):
    if isinstance(path, (list, tuple)):
        [remove(p) for p in path]
    else:
        assert isinstance(path, basestring)
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)


def run(cmd, cwd, show_stdout):
    try:
        call_subprocess(cmd, cwd=cwd, show_stdout=show_stdout)
    except Exception:
        logger.error(' '.join(cmd))
        raise


def call(cmd, cwd=None, show_stdout=True, fork=False, pidfile=None,
         sig=None, sig_func=None):
    if not cwd:
        cwd = os.getcwd()
    cmd = shlex.split(cmd.strip())
    logger.info("[%s] Running ...\n`%s`" % (cwd, ' '.join(cmd)))

    if sig and sig_func:
        signal.signal(sig, sig_func)

    if fork:
        pid = os.fork()
        if pid == 0:
            run(cmd, cwd, show_stdout)
        elif pidfile:
            with open(pidfile, 'w') as f:
                f.write(str(pid))
    else:
        run(cmd, cwd, show_stdout)
    logger.info(" ... Done!")


def adjust_options(options, args):
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


def celeryd_terminate(sig=None, frame=None):
    if not os.path.exists(CELERY_PIDFILE):
        logger.warn("%s does not exist" % CELERY_PIDFILE)
        return
    pid = get_pid(CELERY_PIDFILE)
    os.kill(pid, signal.SIGTERM)


def celeryd_loop(args):
    cmd = 'celery worker -f %s -l INFO -B --pidfile=%s --app %s' % (
          CELERY_LOG, CELERY_PIDFILE, args.tasks_mod)
    call(cmd, fork=True, sig=signal.SIGTERM, sig_func=celeryd_terminate)


def celeryd_run(args):
    tasks = importlib.import_module(args.tasks_mod)
    task = getattr(tasks, args.task)
    return task.run()


def celeryd(args):
    if args.command == "start":
        if args.loop:
            celeryd_loop(args)
        else:
            celeryd_run(args)
    elif args.command == "stop":
        celeryd_terminate()
    elif args.command == "clean":
        remove(CELERY_PIDFILE)
    else:
        raise ValueError("unknown command %s" % args.command)


def metrique_user_register():
    from metrique import pyclient
    m = pyclient()
    m.user_register()


def metriqued_firstboot(args):
    if os.path.exists(METRIQUED_FIRSTBOOT_PATH):
        return
    args.command = 'start'
    metriqued(args)
    metrique_user_register()
    with open(METRIQUED_FIRSTBOOT_PATH, 'w') as f:
        f.write(NOW)
    args.command = 'stop'
    metriqued(args)


def metriqued(args):
    '''
    START, STOP, RESTART, RELOAD,
    '''
    if args.command == 'firstboot':
        metriqued_firstboot(args)
    else:
        call('metriqued %s' % args.command)


def nginx(args):
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


def mongodb_firstboot(args):
    if os.path.exists(MONGODB_FIRSTBOOT_PATH):
        return
    ssl = ' --ssl' if args.ssl else ''
    call('mongo %s/admin %s %s' % (args.host, ssl, MONGODB_JS))
    with open(MONGODB_FIRSTBOOT_PATH, 'w') as f:
        f.write(NOW)


def mongodb(args):
    db_dir = makedirs(args.db_dir)
    lockfile = os.path.join(db_dir, 'mongod.lock')

    config_file = os.path.expanduser(args.config_file)

    pid_dir = makedirs(args.pid_dir)
    pidfile = os.path.join(pid_dir, 'mongodb.pid')

    if args.command == 'start':
        cmd = 'mongod -f %s --fork' % config_file
        cmd += ' --noprealloc --nojournal' if args.fast else ''
        cmd += ' --replSet %s' % args.repl_set if args.repl_set else ''
        call(cmd)
        time.sleep(1)  # give mongodb a second to start
        mongodb_firstboot(args)
    elif args.command == 'stop':
        signal = 15
        pid = get_pid(pidfile)
        try:
            if pid == 0:
                raise OSError
            os.kill(pid, signal)
        except OSError:
            logger.error('MongoDB PID %s does not exist' % pid)
        args.command = 'clean'
        mongodb(args)
    elif args.command == 'restart':
        for cmd in ('stop', 'start'):
            args.command = cmd
            mongodb(args)
    elif args.command == 'clean':
        remove(lockfile)
        remove(pidfile)
    elif args.command == 'trash':
        args.command = 'stop'
        mongodb(args)
        dest = os.path.join(TRASH_DIR, 'mongodb-%s' % NOW)
        shutil.move(db_dir, dest)
        makedirs(db_dir)
        remove(MONGODB_FIRSTBOOT_PATH)
    elif args.command == 'status':
        call('mongod %s --sysinfo' % args.host)
    elif args.command == 'keyfile':
        call('openssl rand -base64 741 -out %s' % MONGODB_KEYFILE)
        os.chmod(MONGODB_KEYFILE, 0600)
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
        call('tar cvfz %s %s' % (out_tgz, out), show_stdout=False)
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
    [remove(f) for f in to_remove]


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
            call('python setup.py %s' % cmd, show_stdout=True)
            #_cmd = ['python', 'setup.py'] + cmd.split(' ')
            #logger.info(str(_cmd))
            #call_subprocess(_cmd, show_stdout=True)
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
    # make sure we have the installer basics and their up2date
    # argparse is needed for py2.6; pip-accel caches compiled binaries
    # first run for a new virt-env will take forever...
    # second run should be 90% faster!
    virtenv = getattr(args, 'virtenv')
    if not virtenv:
        raise RuntimeError("virtenv install path required!")

    # we can't be in a virtenv when running the virtualenv.main() script
    sys.path = [p for p in sys.path if not p.startswith(virtenv)]

    # virtualenv.main; pass in only the virtenv path
    sys.argv = sys.argv[0:1] + [virtenv]
    # run the virtualenv script to install the virtenv
    virtualenv.main()
    # activate the newly installed virtenv
    activate(args)

    call('pip install -U pip setuptools')
    call('pip install pip-accel')

    pip = 'pip' if args.slow else 'pip-accel'

    call('%s install -U virtualenv argparse' % pip)

    # this required dep is installed separately b/c virtenv
    # path resolution issues; fails due to being unable to find
    # the python headers in the virtenv for some reason.
    call('%s install -U cython numpy pandas' % pip)

    # optional dependencies; highly recommended! but slow to
    # install if we're not testing
    if args.matplotlib or args.test:
        call('%s install -U matplotlib' % pip)
    if args.ipython:
        call('%s install -U ipython' % pip)
    if args.test or args.pytest:
        call('%s install -U pytest' % pip)

    cmd = 'install'
    no_pre = getattr(args, 'no_pre', False)
    if not no_pre:
        cmd += ' --pre'
    setup(args, cmd, pip=True)

    if args.develop:
        path = os.path.join(virtenv, 'lib/python2.7/site-packages/metrique*')
        mods = glob.glob(path)
        remove(mods)
        develop(args)

    # run py.test after install
    if args.test:
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


def sys_firstboot(args=None):
    if os.path.exists(SYS_FIRSTBOOT_PATH):
        # skip if we have already run this before
        return

    # create default dirs in advance
    [makedirs(p) for p in (USER_DIR, PIP_CACHE, PIP_ACCEL,
                           PIP_EGGS, TRASH_DIR, LOGS_DIR,
                           ETC_DIR, BACKUP_DIR, MONGODB_DIR,
                           CELERY_DIR, TEMP_DIR, CACHE_DIR,
                           GNUPG_DIR, PID_DIR)]

    # make sure the the default user python eggs dir is secure
    os.chmod(PIP_EGGS, 0700)

    # generate self-signed ssl certs
    ssl()

    # install default configuration files
    default_conf(METRIQUE_JSON, DEFAULT_METRIQUE_JSON)
    default_conf(METRIQUED_JSON, DEFAULT_METRIQUED_JSON)
    default_conf(MONGODB_JSON, DEFAULT_MONGODB_JSON)
    default_conf(MONGODB_CONF, DEFAULT_MONGODB_CONF)
    default_conf(MONGODB_JS, DEFAULT_MONGODB_JS)
    default_conf(CELERY_JSON, DEFAULT_CELERY_JSON)
    default_conf(NGINX_CONF, DEFAULT_NGINX_CONF)

    with open(SYS_FIRSTBOOT_PATH, 'w') as f:
        f.write(NOW)


def main():
    import argparse

    cli = argparse.ArgumentParser(description='Metrique Manage CLI')
    cli.add_argument('-V', '--virtenv')

    _sub = cli.add_subparsers(description='action')

    # Automated metrique deployment
    _deploy = _sub.add_parser('deploy')
    _deploy.add_argument(
        '--slow', action='store_true', help="don't use pip-accel")
    _deploy.add_argument(
        '--no-pre', action='store_true',
        help='ignore pre-release versions')
    _deploy.add_argument(
        '--develop', action='store_true', help='install in "develop mode"')
    _deploy.add_argument(
        '--test', action='store_true', help='run tests after deployment')
    _deploy.add_argument(
        '--ipython', action='store_true', help='install ipython')
    _deploy.add_argument(
        '--pytest', action='store_true', help='install pytest')
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
                                   'clean', 'trash', 'status',
                                   'keyfile'])
    _mongodb.add_argument('-c', '--config-file', default=MONGODB_CONF)
    _mongodb.add_argument('-dd', '--db-dir', default=MONGODB_DIR)
    _mongodb.add_argument('-pd', '--pid-dir', default=PID_DIR)
    _mongodb.add_argument('-H', '--host', default='127.0.0.1')
    _mongodb.add_argument('-f', '--fast', action='store_true')
    _mongodb.add_argument('-s', '--ssl', action='store_true')
    _mongodb.add_argument('-r', '--repl-set')
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
    _nginx.add_argument('-c', '--config-file', default=NGINX_CONF)
    _nginx.add_argument('-m', '--metriqued-config-file',
                        default=METRIQUED_JSON)
    _nginx.set_defaults(func=nginx)

    # metriqued Server
    _metriqued = _sub.add_parser('metriqued')
    _metriqued.add_argument('command')
    _metriqued.set_defaults(func=metriqued)

    # celeryd Server
    _celeryd = _sub.add_parser('celeryd')
    _celeryd.add_argument('command',
                          choices=['start', 'stop', 'clean'])
    _celeryd.add_argument('tasks_mod', nargs='?')
    _celeryd.add_argument('task', nargs='?')
    _celeryd.add_argument('-l', '--loop', action='store_true')
    _celeryd.set_defaults(func=celeryd)

    # SSL creation
    _ssl = _sub.add_parser('ssl')
    _ssl.set_defaults(func=ssl)

    # parse argv
    args = cli.parse_args()

    activate(args)

    # make sure we have some basic defaults configured in the environment
    sys_firstboot(args)

    # run command
    args.func(args)


if __name__ == '__main__':
    main()
