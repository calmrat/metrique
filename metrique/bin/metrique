#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Commandline interface for managing metrique deployments
'''

import datetime
import getpass
import glob
import importlib
import logging
import os
import random
import re
import signal
import shlex
import shutil
import socket
import string
import subprocess
import sys
import time
import virtualenv

log_format = "%(message)s"
logging.basicConfig(format=log_format)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

pjoin = os.path.join
env = os.environ

active_virtualenv = lambda: os.environ.get('VIRTUAL_ENV', '')
call_subprocess = virtualenv.call_subprocess

__actions__ = ['build', 'sdist', 'install', 'develop', 'register',
               'bump', 'status']


def deactivate():
    virtenv = active_virtualenv()
    if virtenv:
        to_remove = [p for p in sys.path if p.startswith(virtenv)]
        if to_remove:
            sys.path = [p for p in sys.path if p not in to_remove]
            logger.debug(' ... paths cleared: %s' % sorted(to_remove))
        env['VIRTUAL_ENV'] = ''
        logger.debug('Virtual Env (%s): Deactivated' % virtenv)
    else:
        logger.debug('Deactivate: Virtual Env not detected')


def activate(args=None):
    virtenv = getattr(args, 'virtenv') or active_virtualenv()
    if not virtenv:
        logger.info('Activate: No virtenv defined')
        return  # nothing to activate
    elif virtenv == active_virtualenv():
        logger.debug('Virtual Env already active')
        return  # nothing to activate
    else:
        deactivate()  # deactive active virtual first

    activate_this = pjoin(virtenv, 'bin', 'activate_this.py')
    if os.path.exists(activate_this):
        execfile(activate_this, dict(__file__=activate_this))
        env['VIRTUAL_ENV'] = virtenv
        logger.info('Virtual Env (%s): Activated' % active_virtualenv())
    else:
        raise OSError("Invalid virtual env; %s not found" % activate_this)


def rand_chars(size=6, chars=string.ascii_uppercase + string.digits):
    # see: http://stackoverflow.com/questions/2257441
    return ''.join(random.choice(chars) for x in range(size))


USER = getpass.getuser()
VIRTUAL_ENV = active_virtualenv()
NOW = datetime.datetime.utcnow().strftime('%FT%H%M%S')

HOSTNAME = socket.gethostname()
try:
    # try to get one of the local inet device ip addresses
    LOCAL_IP = socket.gethostbyname(HOSTNAME)
except Exception:
    LOCAL_IP = '127.0.0.1'

PASSWORD = rand_chars(10)
COOKIE_SECRET = rand_chars(50)

HOME_DIR = env.get('METRIQUE_HOME', os.path.expanduser('~/'))
USER_DIR = env.get('METRIQUE_USR', pjoin(HOME_DIR, '.metrique'))

# set cache dir so pip doesn't have to keep downloading over and over
PIP_DIR = pjoin(USER_DIR, '.pip')
PIP_CACHE_DIR = pjoin(PIP_DIR, 'download-cache')
PIP_ACCEL_DIR = pjoin(USER_DIR, '.pip-accel')
PIP_EGGS = pjoin(USER_DIR, '.python-eggs')
env['PIP_DOWNLOAD_CACHE'] = env.get('PIP_DOWNLOAD_CACHE', PIP_CACHE_DIR)
env['PIP_ACCEL_CACHE'] = env.get('PIP_ACCEL_CACHE', PIP_ACCEL_DIR)
env['PYTHON_EGG_CACHE'] = env.get('PYTHON_EGG_CACHE', PIP_EGGS)

TRASH_DIR = env.get('METRIQUE_TRASH', pjoin(USER_DIR, 'trash'))
LOGS_DIR = env.get('METRIQUE_LOGS', pjoin(USER_DIR, 'logs'))
ETC_DIR = env.get('METRIQUE_ETC', pjoin(USER_DIR, 'etc'))
PIDS_DIR = env.get('METRIQUE_PIDS', pjoin(USER_DIR, 'pids'))
BACKUP_DIR = env.get('METRIQUE_BACKUP', pjoin(USER_DIR, 'backup'))
TMP_DIR = env.get('METRIQUE_TMP', pjoin(USER_DIR, 'tmp'))
CACHE_DIR = env.get('METRIQUE_CACHE', pjoin(USER_DIR, 'cache'))
MONGODB_DIR = env.get('METRIQUE_MONGODB', pjoin(USER_DIR, 'mongodb'))
CELERY_DIR = env.get('METRIQUE_CELERY', pjoin(USER_DIR, 'celery'))
STATIC_DIR = env.get('METRIQUE_STATIC', pjoin(USER_DIR, 'static'))

METRIQUE_FIRSTBOOT_PATH = pjoin(USER_DIR, '.firstboot_metrique')
METRIQUE_JSON = pjoin(ETC_DIR, 'metrique.json')

API_DOCS_PATH = env.get('METRIQUE_API_DOCS', 'docs/build/html/')

SYS_FIRSTBOOT_PATH = pjoin(USER_DIR, '.firstboot_sys')

SSL_CERT = pjoin(ETC_DIR, 'metrique.crt')
SSL_KEY = pjoin(ETC_DIR, 'metrique.key')
SSL_PEM = pjoin(ETC_DIR, 'metrique.pem')

MONGODB_FIRSTBOOT_PATH = pjoin(USER_DIR, '.firstboot_mongodb')
MONGODB_CONF = pjoin(ETC_DIR, 'mongodb.conf')
MONGODB_PIDFILE = pjoin(PIDS_DIR, 'mongodb.pid')
MONGODB_LOCKFILE = pjoin(MONGODB_DIR, 'mongod.lock')
MONGODB_LOG = pjoin(LOGS_DIR, 'mongodb.log')
MONGODB_JSON = pjoin(ETC_DIR, 'mongodb.json')
MONGODB_JS = pjoin(ETC_DIR, 'mongodb.js')
MONGODB_KEYFILE = pjoin(ETC_DIR, 'mongodb.key')

CELERY_FIRSTBOOT_PATH = pjoin(USER_DIR, '.firstboot_celery')
CELERY_JSON = pjoin(ETC_DIR, 'celery.json')
CELERYD_PIDFILE = pjoin(PIDS_DIR, 'celeryd.pid')
CELERYBEAT_PIDFILE = pjoin(PIDS_DIR, 'celerybeat.pid')
CELERY_LOGFILE = pjoin(LOGS_DIR, 'celeryd.log')

NGINX_FIRSTBOOT_PATH = pjoin(USER_DIR, '.firstboot_nginx')
NGINX_CONF = pjoin(ETC_DIR, 'nginx.conf')
NGINX_ACCESS_LOG = pjoin(LOGS_DIR, 'nginx_access.log')
NGINX_ERROR_LOG = pjoin(LOGS_DIR, 'nginx_error.log')
NGINX_PIDFILE = pjoin(PIDS_DIR, 'nginx.pid')

SUPERVISORD_FIRSTBOOT_PATH = pjoin(USER_DIR, '.firstboot_supervisord')
SUPERVISORD_CONF = pjoin(ETC_DIR, 'supervisord.conf')
SUPERVISORD_PIDFILE = pjoin(PIDS_DIR, 'supervisord.pid')
SUPERVISORD_LOGFILE = pjoin(LOGS_DIR, 'supervisord.log')
SUPERVISORD_HISTORYFILE = pjoin(TMP_DIR, 'supervisord_history')

############################## DEFAULT CONFS #################################
DEFAULT_METRIQUE_JSON = '''{
    "batch_size": 5000,
    "debug": true,
    "log2file": true,
    "logstdout": false,
    "max_workers": 4,
    "sql_batch_size": 1000,
    "mongodb": {
        "auth": false,
        "fsync": false,
        "host": "127.0.0.1",
        "_host": "%s",
        "journal": false,
        "password": "%s",
        "port": 27017,
        "read_preference": "NEAREST",
        "replica_set": "",
        "ssl": false,
        "ssl_certificate": "%s",
        "tz_aware": true,
        "username": "admin",
        "write_concern": 1
    }
}'''


DEFAULT_MONGODB_CONF = '''
#fork = true
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
#keyFile = %s'''

DEFAULT_MONGODB_JS = '''
db = db.getSiblingDB('admin')
db.addUser({'user': 'admin', 'pwd': '%s', 'roles': ['dbAdminAnyDatabase',
        'userAdminAnyDatabase', 'clusterAdmin', 'readWriteAnyDatabase']});
'''

DEFAULT_CELERY_JSON = '''{
    "BROKER_URL": "mongodb://admin:%s@127.0.0.1:27017",
    "BROKER_URL_LOCAL": "mongodb://admin:%s@%s:27017",
    "BROKER_USE_SSL": false
}'''

DEFAULT_NGINX_CONF = '''
worker_processes auto;
daemon off;  # see warnings: http://wiki.nginx.org/CoreModule#daemon
user %s;
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
        #server %s:5421;
        #server %s:5422;
        #server %s:5423;
        #server %s:5424;
    }

    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    error_log %s;
    access_log %s;

    # Timeouts
    keepalive_timeout 3m;
    client_header_timeout  3m;
    client_body_timeout  3m;
    proxy_connect_timeout 3m;
    proxy_send_timeout 3m;
    proxy_read_timeout 3m;

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
        #listen %s:5420;
        ssl                 off;
        ssl_certificate     %s;
        ssl_certificate_key %s;

        ssl_protocols        SSLv3 TLSv1 TLSv1.1 TLSv1.2;
        ssl_ciphers          RC4:HIGH:!aNULL:!MD5;
        ssl_prefer_server_ciphers on;
        ssl_session_cache    shared:SSL:10m;
        ssl_session_timeout  60m;

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
                                                             http_504;
            # NOTE: consider adding 503, if not raising 503 for locks
        }
    }
}
'''

DEFAULT_SUPERVISORD_CONF = '''
[inet_http_server]
port=127.0.0.1:9001
;port=%s:9001
username=admin
password=%s

[rpcinterface:supervisor]
%s

[supervisord]
logfile=%s
pidfile=%s
childlogdir=%s
user=%s
environment=%s
loglevel=debug

[supervisorctl]
serverurl=http://127.0.0.1:9001
;serverurl=http://%s:9001
username=admin
password=%s
history_file=%s

[program:mongodb]
command=metrique mongodb start --nofork
process_name=mongodb
numprocs=1
priority=10
startsecs=60
stopwaitsecs=60

[program:nginx]
command=metrique nginx start --nofork
process_name=nginx
numprocs=1
priority=30
startsecs=30

[program:celeryd]
command=metrique celeryd start --nofork
process_name=celeryd
numprocs=1
priority=40
startsecs=30
autorestart=true

[program:celerybeat]
command=metrique celerybeat start --nofork
process_name=celerybeat
numprocs=1
priority=41
startsecs=30
autorestart=true
'''
###############################################################################


def get_pid(pidfile):
    try:
        return int(''.join(open(pidfile).readlines()).strip())
    except IOError:
        return 0


def makedirs(path, mode=0700):
    if not path.startswith('/'):
        raise OSError("requires absolute path! got %s" % path)
    if not os.path.exists(path):
        os.makedirs(path, mode)
    return path


def move(path, dest, quiet=True):
    if isinstance(path, (list, tuple)):
        [move(p) for p in path]
    else:
        assert isinstance(path, basestring)
        if os.path.exists(path):
            shutil.move(path, dest)
        elif not quiet:
            logger.warn('[move] %s not found' % path)


def remove(path, quiet=True):
    if not path:
        return []
    path = glob.glob(path)
    if isinstance(path, (list, tuple)):
        if len(path) == 1:
            path = path[0]
        else:
            return [remove(p) for p in path]

    assert isinstance(path, basestring)
    if not quiet:
        logger.warn('[remove] deleting %s' % path)
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    elif not quiet:
        logger.warn('[remove] %s not found' % path)
    return path


def system(cmd, fork=False, sig=None, sig_func=None):
    if sig and sig_func:
        signal.signal(sig, sig_func)

    logger.debug("Running: %s" % cmd)
    cmd = cmd.split()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    text = p.stdout.read()
    return text


def run(cmd, cwd, show_stdout):
    logger.info("[%s] Running ...\n`%s`" % (cwd, ' '.join(cmd)))
    try:
        call_subprocess(cmd, cwd=cwd, show_stdout=show_stdout)
    except KeyboardInterrupt:
        logger.warn('CTRL-C killed')
        sys.exit(1)
    except Exception as e:
        raise OSError('[%s] %s' % (e, ' '.join(cmd)))


def call(cmd, cwd=None, show_stdout=True, fork=False, pidfile=None,
         sig=None, sig_func=None):
    cmd = shlex.split(cmd.strip())
    if sig and sig_func:
        signal.signal(sig, sig_func)

    if fork:
        pid = os.fork()
        if pid == 0:
            run(cmd, cwd, show_stdout)
            sys.exit(2)
        elif pidfile:
            with open(pidfile, 'w') as f:
                f.write(str(pid))
    else:
        run(cmd, cwd, show_stdout)
    logger.info(" ... Done!")


def adjust_options(options, args):
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


def firstboot(args, force=False, trash=False, no_auth=False):
    # make sure we have some basic defaults configured in the environment
    trash = getattr(args, 'trash', trash)
    force = getattr(args, 'force', force)
    auth = not getattr(args, 'no_auth', no_auth)
    sys_firstboot(force)
    mongodb_firstboot(force, auth=auth)
    metrique_firstboot(force)
    celery_firstboot(force)
    supervisord_firstboot(force)
    nginx_firstboot(force)


def backup(saveas, path):
    gzip = system('which pigz') or system('which gzip')
    cmd = 'tar -c --use-compress-program=%s -f %s %s' % (gzip, saveas, path)
    system(cmd)


def backup_clean(args, path, prefix):
    keep = args.keep if args.keep != 0 else 3
    path = pjoin(path, prefix) + '*'
    files = sorted(glob.glob(path), reverse=True)
    to_remove = files[keep:]
    logger.debug('Removing %i backups' % len(to_remove))
    [remove(f) for f in to_remove]


def terminate(pidfile, sig=signal.SIGTERM):
    if os.path.exists(pidfile):
        pid = get_pid(pidfile)
        try:
            os.kill(pid, sig)
        except OSError:
            logger.debug("%s not found" % pid)
        else:
            logger.debug("%s killed" % pid)
        remove(pidfile)
    else:
        logger.debug("[terminate] %s does not exist" % pidfile)


def celeryd_terminate(sig=None, frame=None):
    terminate(CELERYD_PIDFILE)


def celeryd_loop(args):
    fork = not args.nofork
    x = 'worker'
    logfile = '--logfile=%s' % CELERY_LOGFILE
    loglvl = '-l INFO'
    pidfile = '--pidfile=%s' % CELERYD_PIDFILE
    app = '-A %s' % args.tasks_mod
    cmd = 'celery %s %s %s %s %s' % (x, logfile, loglvl, pidfile, app)
    call(cmd, fork=fork, sig=signal.SIGTERM, sig_func=celeryd_terminate)


def celeryd_task(args):
    tasks = importlib.import_module(args.tasks_mod)
    task = getattr(tasks, args.task)
    return task.run()


def celeryd(args):
    if args.command == "start":
        celeryd_loop(args)
    elif args.command == "stop":
        terminate(CELERYD_PIDFILE)
    elif args.command == "clean":
        remove(CELERYD_PIDFILE)
    else:
        raise ValueError("unknown command %s" % args.command)


def celerybeat_terminate(sig=None, frame=None):
    terminate(CELERYBEAT_PIDFILE)


def celerybeat_run(args):
    fork = not args.nofork
    x = 'beat'
    logfile = '--logfile=%s' % CELERY_LOGFILE
    loglvl = '-l INFO'
    pidfile = '--pidfile=%s' % CELERYBEAT_PIDFILE
    app = '-A %s' % args.tasks_mod
    cmd = 'celery %s %s %s %s %s' % (x, logfile, loglvl, pidfile, app)
    call(cmd, fork=fork, sig=signal.SIGTERM, sig_func=celerybeat_terminate)


def celerybeat(args):
    if args.command == "start":
        celerybeat_run(args)
    elif args.command == "stop":
        terminate(CELERYBEAT_PIDFILE)
    elif args.command == "clean":
        remove(CELERYBEAT_PIDFILE)
    else:
        raise ValueError("unknown command %s" % args.command)


def supervisord_terminate(sig=None, frame=None):
    terminate(SUPERVISORD_PIDFILE)


def supervisord_run(args):
    cmd = 'supervisord -c %s' % SUPERVISORD_CONF
    call(cmd, fork=True, sig=signal.SIGTERM, sig_func=supervisord_terminate)


def supervisord(args):
    if args.command == "start":
        supervisord_run(args)
    elif args.command == "stop":
        terminate(SUPERVISORD_PIDFILE)
    elif args.command == "clean":
        remove(SUPERVISORD_PIDFILE)
    elif args.command == "reload":
        terminate(SUPERVISORD_PIDFILE, signal.SIGHUP)
    else:
        raise ValueError("unknown command %s" % args.command)


def nginx_terminate(sig=None, frame=None):
    terminate(NGINX_PIDFILE)


def nginx(args):
    fork = not args.nofork
    cmd = 'nginx -c %s' % NGINX_CONF
    if args.command == 'test':
        call('%s -t' % cmd)
    elif args.command == 'start':
        call(cmd, fork=fork, sig=signal.SIGTERM, sig_func=nginx_terminate)
    elif args.command == 'stop':
        call('%s -s stop' % cmd)
    elif args.command == 'restart':
        for cmd in ('stop', 'start'):
            args.command = cmd
            nginx(args)
    elif args.command == 'reload':
        call('%s -s reload' % cmd)
    else:
        raise ValueError("unknown command %s" % args.command)


def mongodb_terminate(sig=None, frame=None):
    terminate(MONGODB_PIDFILE)


def mongodb_start(fork=False, fast=True):
    fork = '--fork' if fork else ''
    if os.path.exists(MONGODB_PIDFILE):
        logger.info('MongoDB pid found not starting...')
        return False
    cmd = 'mongod -f %s %s' % (MONGODB_CONF, fork)
    cmd += ' --noprealloc --nojournal' if fast else ''
    call(cmd, fork=fork, sig=signal.SIGTERM, sig_func=mongodb_terminate)
    return True


def mongodb_stop():
    terminate(MONGODB_PIDFILE)
    mongodb_clean()


def mongodb_clean():
    remove(MONGODB_LOCKFILE)
    remove(MONGODB_PIDFILE)


def mongodb_trash():
    mongodb_stop()
    dest = pjoin(TRASH_DIR, 'mongodb-%s' % NOW)
    move(MONGODB_DIR, dest)
    move(MONGODB_CONF, dest)
    move(MONGODB_JSON, dest)
    move(MONGODB_JS, dest)
    move(MONGODB_KEYFILE, dest)
    remove(MONGODB_FIRSTBOOT_PATH)
    makedirs(MONGODB_DIR)


def mongodb_gen_keyfile():
    call('openssl rand -base64 741 -out %s' % MONGODB_KEYFILE)
    os.chmod(MONGODB_KEYFILE, 0600)


def mongodb_backup(args):
    from metrique.mongodb_api import MongoDBConfig
    config = MongoDBConfig()

    prefix = 'mongodb'
    saveas = '__'.join((prefix, HOSTNAME, NOW))
    out = pjoin(BACKUP_DIR, saveas)

    host = config.host.split(',')[0]  # get the first host (expected primary)
    port = config.port
    p = config.password
    password = '--password %s' % p if p else ''
    username = '--username %s' % config.username if password else ''
    authdb = '--authenticationDatabase admin' if password else ''
    ssl = '--ssl' if config.ssl else ''
    db = '--db %s' if args.db else ''
    collection = '--collection %s' if args.collection else ''

    cmd = ('mongodump', '--host %s' % host, '--port %s' % port,
           ssl, username, password, '--out %s' % out, authdb,
           db, collection)
    cmd = re.sub('\s+', ' ', ' '.join(cmd))
    call(cmd)

    saveas = out + '.tar.gz'
    backup(saveas, out)
    shutil.rmtree(out)

    backup_clean(args, BACKUP_DIR, 'mongodb')

    if args.scp_export:
        user = args.scp_user
        host = args.scp_host
        out_dir = args.scp_out_dir
        cmd = 'scp %s %s@%s:%s' % (saveas, user, host, out_dir)
        call(cmd)


def mongodb(args):
    fork = not args.nofork
    fast = args.fast

    if args.command == 'start':
        mongodb_start(fork, fast)
    elif args.command == 'stop':
        mongodb_stop()
    elif args.command == 'restart':
        mongodb_stop()
        mongodb_start(fork, fast)
    elif args.command == 'clean':
        mongodb_clean()
    elif args.command == 'trash':
        mongodb_trash()
    else:
        raise ValueError("unknown command %s" % args.command)


def rsync(args):
    ssh_user = args.ssh_user
    ssh_host = args.ssh_host
    saveas = re.sub('\W', '_', HOSTNAME)
    compress = '-z' if not args.nocompress else ''
    if not args.targets:
        raise OSError("one or more targets required!")
    targets = ' '.join(args.targets)
    if ssh_host:
        call('rsync -av %s -e ssh %s %s@%s:%s' % (
            compress, targets, ssh_user, ssh_host, saveas),
            show_stdout=True)
    else:
        saveas = pjoin(BACKUP_DIR, saveas)
        call('rsync -av %s %s %s' % (compress, targets, saveas),
             show_stdout=True)


def trash(args=None):
    supervisord_terminate()
    celerybeat_terminate()
    celeryd_terminate()
    nginx_terminate()
    mongodb_terminate()

    dest = pjoin(TRASH_DIR, 'metrique-%s' % NOW)
    for f in [ETC_DIR, PIDS_DIR, LOGS_DIR, CACHE_DIR,
              TMP_DIR, CELERY_DIR, MONGODB_DIR]:
        _dest = os.path.join(dest, os.path.basename(f))
        try:
            shutil.move(f, _dest)
        except (IOError, OSError) as e:
            logger.error(e)
            continue
    firstboot_glob = os.path.join(USER_DIR, '.firstboot*')
    remove(firstboot_glob)


def setup(args, cmd, pip=False):
    if isinstance(cmd, basestring):
        cmd = cmd.strip()
    else:
        cmd = ' '.join([s.strip() for s in cmd])
    if pip and args.slow:
        logger.info(system('pip %s -e .' % cmd))
    elif pip:
        logger.info(system('pip-accel %s -e .' % cmd))
    else:
        logger.info(system('python setup.py %s' % cmd))


def _deploy_virtenv_init(args):
    virtenv = getattr(args, 'virtenv') or ''
    if virtenv:
        # we can't alrady be in a virtenv when running virtualenv.main()
        deactivate()

        # scratch the existing virtenv directory, if requested
        if args.trash:
            remove(virtenv)
            trash()

        # virtualenv.main; pass in only the virtenv path
        sys.argv = sys.argv[0:1] + [virtenv]
        # run the virtualenv script to install the virtenv
        virtualenv.main()

        # activate the newly installed virtenv
        activate(args)
    return virtenv


def _deploy_which_pip(args):
    # install pip-accel and use it instead of pip unless slow install
    if args.slow:
        pip = 'pip'
    else:
        call('pip install pip-accel')
        pip = 'pip-accel'
    return pip


def _deploy_deps(args):
    # optional dependencies; highly recommended! but slow to
    # install if we're not testing
    pip = _deploy_which_pip(args)

    if args.all or args.ipython:
        call('%s install -U ipython' % pip)
    if args.all or args.test or args.pytest:
        call('%s install -U pytest coveralls' % pip)
    if args.all or args.docs:
        call('%s install -U sphinx' % pip)
        # pip-accel fails to install this package...
        call('pip install -U sphinx_bootstrap_theme')
    if args.all or args.supervisord:
        call('%s install -U supervisor' % pip)
    if args.all or args.joblib:
        call('%s install -U joblib' % pip)
    if args.all or args.postgres:
        call('%s install -U psycopg2' % pip)
    if args.all or args.celery:
        call('%s install -U celery' % pip)


def deploy(args):
    virtenv = _deploy_virtenv_init(args)

    # make sure we have some basic defaults configured in the environment
    firstboot(args)

    # make sure we have the installer basics and their up2date
    # pip-accel caches compiled binaries
    call('pip install -U pip setuptools virtualenv')

    _deploy_deps(args)

    cmd = 'install'
    no_pre = getattr(args, 'no_pre', False)
    if not no_pre:
        cmd += ' --pre'
    setup(args, cmd, pip=True)

    if args.develop:
        path = pjoin(virtenv, 'lib/python2.7/site-packages/metrique*')
        remove(path)
        develop(args)

    # run py.test after install
    if args.test:
        running = get_pid(MONGODB_PIDFILE) != 0
        if not running:
            call('metrique mongodb start --fast')
        call('coverage run --source=metrique -m py.test tests')


def build(args):
    cmd = 'build'
    setup(args, cmd)


def sdist(args, upload=None):
    upload = upload or args.upload
    cmd = 'sdist'
    if upload:
        cmd += ' upload'
    setup(args, cmd)


def develop(args):
    cmd = 'develop'
    setup(args, cmd)


def register(args):
    cmd = 'register'
    setup(args, cmd)


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
    logger.info("Installed %s ..." % path)


def mongodb_firstboot(force, auth=True):
    exists = os.path.exists(MONGODB_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return
    makedirs(MONGODB_DIR)
    mongodb_gen_keyfile()

    global DEFAULT_MONGODB_CONF, DEFAULT_MONGODB_JS
    DEFAULT_MONGODB_CONF = DEFAULT_MONGODB_CONF % (
        MONGODB_DIR, MONGODB_LOG, MONGODB_PIDFILE, LOCAL_IP, SSL_PEM,
        MONGODB_KEYFILE)
    DEFAULT_MONGODB_JS = DEFAULT_MONGODB_JS % (PASSWORD)

    default_conf(MONGODB_CONF, DEFAULT_MONGODB_CONF)

    # by installing 'admin' user in Travis-ci we enable
    # authentication; flag here is to disable that
    # for testing
    if auth:
        default_conf(MONGODB_JS, DEFAULT_MONGODB_JS)

    started = mongodb_start(fork=True, fast=True)
    logger.debug('MongoDB forking, sleeping for a moment...')
    time.sleep(1)

    try:
        if auth:
            call('mongo 127.0.0.1/admin %s' % (MONGODB_JS))
    finally:
        if started:
            mongodb_stop()
    with open(MONGODB_FIRSTBOOT_PATH, 'w') as f:
        f.write(NOW)


def metrique_firstboot(force=False):
    exists = os.path.exists(METRIQUE_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return

    global DEFAULT_METRIQUE_JSON

    DEFAULT_METRIQUE_JSON = DEFAULT_METRIQUE_JSON % (
        LOCAL_IP, PASSWORD, SSL_PEM)

    default_conf(METRIQUE_JSON, DEFAULT_METRIQUE_JSON)


def celery_firstboot(force=False):
    exists = os.path.exists(CELERY_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return
    makedirs(CELERY_DIR)
    global DEFAULT_CELERY_JSON
    DEFAULT_CELERY_JSON = DEFAULT_CELERY_JSON % (PASSWORD, PASSWORD, LOCAL_IP)
    default_conf(CELERY_JSON, DEFAULT_CELERY_JSON)


def supervisord_firstboot(force=False):
    exists = os.path.exists(SUPERVISORD_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return

    global DEFAULT_SUPERVISORD_CONF
    ENVIRONMENT = 'VIRTUAL_ENV="%s", METRIQUE_HOME="%s"' % (
        VIRTUAL_ENV, HOME_DIR)
    RPC = ('supervisor.rpcinterface_factory = '
           'supervisor.rpcinterface:make_main_rpcinterface')
    DEFAULT_SUPERVISORD_CONF = DEFAULT_SUPERVISORD_CONF % (
        LOCAL_IP, PASSWORD, RPC, SUPERVISORD_LOGFILE, SUPERVISORD_PIDFILE,
        LOGS_DIR, USER, ENVIRONMENT, LOCAL_IP, PASSWORD,
        SUPERVISORD_HISTORYFILE)

    default_conf(SUPERVISORD_CONF, DEFAULT_SUPERVISORD_CONF)


def nginx_firstboot(force=False):
    exists = os.path.exists(NGINX_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return

    global DEFAULT_NGINX_CONF
    DEFAULT_NGINX_CONF = DEFAULT_NGINX_CONF % (
        USER, NGINX_ERROR_LOG, NGINX_PIDFILE, TMP_DIR,
        TMP_DIR, CACHE_DIR, TMP_DIR, CACHE_DIR, TMP_DIR, CACHE_DIR,
        TMP_DIR, CACHE_DIR, LOCAL_IP, LOCAL_IP, LOCAL_IP, LOCAL_IP,
        NGINX_ERROR_LOG, NGINX_ACCESS_LOG, LOCAL_IP, SSL_CERT, SSL_KEY,
        STATIC_DIR)

    default_conf(NGINX_CONF, DEFAULT_NGINX_CONF)


def sys_firstboot(force=False):
    exists = os.path.exists(SYS_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return

    # create default dirs in advance
    [makedirs(p) for p in (USER_DIR, PIP_CACHE_DIR, PIP_ACCEL_DIR,
                           PIP_EGGS, TRASH_DIR, LOGS_DIR,
                           ETC_DIR, BACKUP_DIR, TMP_DIR, CACHE_DIR,
                           STATIC_DIR, PIDS_DIR)]

    # make sure the the default user python eggs dir is secure
    os.chmod(PIP_EGGS, 0700)

    # generate self-signed ssl certs
    ssl()

    with open(SYS_FIRSTBOOT_PATH, 'w') as f:
        f.write(NOW)


def main():
    import argparse

    cli = argparse.ArgumentParser(description='Metrique Manage CLI')
    cli.add_argument('-V', '--virtenv')
    cli.add_argument('-v', '--verbose')

    _sub = cli.add_subparsers(description='action', dest='action')

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
        '--all', action='store_true', help='install all "extra" dependencies')
    _deploy.add_argument(
        '--ipython', action='store_true', help='install ipython')
    _deploy.add_argument(
        '--pytest', action='store_true', help='install pytest')
    _deploy.add_argument(
        '--docs', action='store_true', help='install doc utils')
    _deploy.add_argument(
        '--supervisord', action='store_true', help='install supervisord')
    _deploy.add_argument(
        '--joblib', action='store_true', help='install joblib')
    _deploy.add_argument(
        '--postgres', action='store_true', help='install postgres')
    _deploy.add_argument(
        '--celery', action='store_true', help='install celery')
    _deploy.add_argument(
        '--trash', action='store_true', help='fresh install (rm old virtenv)')
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

    # Trash existing metrique installation
    _trash = _sub.add_parser('trash')
    _trash.set_defaults(func=trash)

    # Clean-up routines
    _firstboot = _sub.add_parser('firstboot')
    _firstboot.add_argument('-f', '--force', action='store_true')
    _firstboot.add_argument('-A', '--no-auth', action='store_true')
    _firstboot.set_defaults(func=firstboot)

    # rsync
    _rsync = _sub.add_parser('rsync')
    _rsync.add_argument('targets', nargs='*')
    _rsync.add_argument('-m', '--mongodb-host')
    _rsync.add_argument('-Z', '--nocompress', action='store_true')
    _rsync.add_argument('-H', '--ssh-host')
    _rsync.add_argument('-u', '--ssh-user', default='backup')
    _rsync.set_defaults(func=rsync)

    # MongoDB Server
    _mongodb = _sub.add_parser('mongodb')
    _mongodb.add_argument('command',
                          choices=['start', 'stop', 'restart',
                                   'clean', 'trash', 'status'])
    _mongodb.add_argument('-H', '--host', default='127.0.0.1')
    _mongodb.add_argument('-f', '--fast', action='store_true')
    _mongodb.add_argument('-s', '--ssl', action='store_true')
    _mongodb.add_argument('-u', '--user')
    _mongodb.add_argument('-p', '--password')
    _mongodb.add_argument('-F', '--nofork', action='store_true')
    _mongodb.set_defaults(func=mongodb)

    # MongoDB Backup
    _mongodb_backup = _sub.add_parser('mongodb_backup')
    _mongodb_backup.add_argument('-c', '--config-file')
    _mongodb_backup.add_argument('-D', '--db')
    _mongodb_backup.add_argument('-C', '--collection')
    _mongodb_backup.add_argument('-k', '--keep', type=int, default=3)
    _mongodb_backup.add_argument('-x', '--scp-export', action='store_true')
    _mongodb_backup.add_argument('-H', '--scp-host')
    _mongodb_backup.add_argument('-u', '--scp-user', default='backup')
    _mongodb_backup.add_argument('-O', '--scp-out-dir')
    _mongodb_backup.set_defaults(func=mongodb_backup)

    # nginx Server
    _nginx = _sub.add_parser('nginx')
    _nginx.add_argument('command',
                        choices=['start', 'stop', 'reload',
                                 'restart', 'test'])
    _nginx.add_argument('-F', '--nofork', action='store_true')
    _nginx.set_defaults(func=nginx)

    # celeryd task run
    _celeryd_task = _sub.add_parser('celeryd_task')
    _celeryd_task.add_argument('task')
    _celeryd_task.add_argument('--tasks-mod', default='dataservices.tasks')
    _celeryd_task.set_defaults(func=celeryd_task)

    # celeryd server
    _celeryd = _sub.add_parser('celeryd')
    _celeryd.add_argument('command', choices=['start', 'stop', 'clean'])
    _celeryd.add_argument('--tasks-mod', default='dataservices.tasks')
    _celeryd.add_argument('-F', '--nofork', action='store_true')
    _celeryd.set_defaults(func=celeryd)

    # celerybeat server
    _celerybeat = _sub.add_parser('celerybeat')
    _celerybeat.add_argument('command', choices=['start', 'stop', 'clean'])
    _celerybeat.add_argument('--tasks-mod', default='dataservices.tasks')
    _celerybeat.add_argument('-F', '--nofork', action='store_true')
    _celerybeat.set_defaults(func=celerybeat)

    # supervisord server
    _supervisord = _sub.add_parser('supervisord')
    _supervisord.add_argument('command', choices=['start', 'stop',
                                                  'clean', 'reload'])
    _supervisord.set_defaults(func=supervisord)

    # SSL creation
    _ssl = _sub.add_parser('ssl')
    _ssl.set_defaults(func=ssl)

    # parse argv
    args = cli.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.debug('-' * 30)
    logger.debug('Started at  : %s' % NOW)
    logger.debug('Current User: %s' % USER)
    logger.debug('Virtual Env : %s' % VIRTUAL_ENV)
    logger.debug('Hostname    : %s' % HOSTNAME)
    logger.debug('Local IP    : %s' % LOCAL_IP)
    logger.debug('This file   : %s' % __file__)
    logger.debug('Home Path   : %s' % HOME_DIR)
    logger.debug('User Path   : %s' % USER_DIR)
    logger.debug('-' * 30)

    if args.action != 'deploy':
        # Activate the virtual environment in this python session if
        # parent env has one set
        activate(args)

    # run command
    args.func(args)


if __name__ == '__main__':
    main()
