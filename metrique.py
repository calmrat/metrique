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
import re
import signal
import shutil
import socket
import sys
import time
import virtualenv

# find metrique.utils; default is relative to
# this calling file; fallback: global import
try:
    import imp
    utils_tup = imp.find_module('metrique.utils')
    utils = imp.load_module(*utils_tup)
except ImportError:
    import metrique.utils as utils


log_format = "%(message)s"
logger = utils.debug_setup(
    'metrique', level=logging.INFO, log_format=log_format,
    log2file=False, log2stdout=True)

pjoin = os.path.join
env = os.environ

USER = getpass.getuser()
VIRTUAL_ENV = utils.active_virtualenv()
NOW = datetime.datetime.utcnow().strftime('%FT%H%M%S')

HOSTNAME = socket.gethostname()
try:
    # try to get one of the local inet device ip addresses
    LOCAL_IP = socket.gethostbyname(HOSTNAME)
except Exception:
    LOCAL_IP = '127.0.0.1'

PASSWORD = utils.rand_chars(10)
COOKIE_SECRET = utils.rand_chars(50)

HOME_DIR = env.get('METRIQUE_HOME', os.path.expanduser('~/'))
PREFIX_DIR = env.get('METRIQUE_PREFIX', pjoin(HOME_DIR, '.metrique'))

# set cache dir so pip doesn't have to keep downloading over and over
PIP_DIR = pjoin(PREFIX_DIR, '.pip')
PIP_CACHE_DIR = pjoin(PIP_DIR, 'download-cache')
PIP_ACCEL_DIR = pjoin(PREFIX_DIR, '.pip-accel')
PIP_EGGS = pjoin(PREFIX_DIR, '.python-eggs')
env['PIP_DOWNLOAD_CACHE'] = env.get('PIP_DOWNLOAD_CACHE', PIP_CACHE_DIR)
env['PIP_ACCEL_CACHE'] = env.get('PIP_ACCEL_CACHE', PIP_ACCEL_DIR)
env['PYTHON_EGG_CACHE'] = env.get('PYTHON_EGG_CACHE', PIP_EGGS)

TRASH_DIR = env.get('METRIQUE_TRASH', pjoin(PREFIX_DIR, 'trash'))
LOGS_DIR = env.get('METRIQUE_LOGS', pjoin(PREFIX_DIR, 'logs'))
ETC_DIR = env.get('METRIQUE_ETC', pjoin(PREFIX_DIR, 'etc'))
PIDS_DIR = env.get('METRIQUE_PIDS', pjoin(PREFIX_DIR, 'pids'))
BACKUP_DIR = env.get('METRIQUE_BACKUP', pjoin(PREFIX_DIR, 'backup'))
TMP_DIR = env.get('METRIQUE_TMP', pjoin(PREFIX_DIR, 'tmp'))
CACHE_DIR = env.get('METRIQUE_CACHE', pjoin(PREFIX_DIR, 'cache'))
MONGODB_DIR = env.get('METRIQUE_MONGODB', pjoin(PREFIX_DIR, 'mongodb'))
STATIC_DIR = env.get('METRIQUE_STATIC', pjoin(PREFIX_DIR, 'static'))

METRIQUE_FIRSTBOOT_PATH = pjoin(PREFIX_DIR, '.firstboot_metrique')
METRIQUE_JSON = pjoin(ETC_DIR, 'metrique.json')

API_DOCS_PATH = env.get('METRIQUE_API_DOCS', 'docs/build/html/')

SYS_FIRSTBOOT_PATH = pjoin(PREFIX_DIR, '.firstboot_sys')

SSL_CERT = pjoin(ETC_DIR, 'metrique.crt')
SSL_KEY = pjoin(ETC_DIR, 'metrique.key')
SSL_PEM = pjoin(ETC_DIR, 'metrique.pem')

MONGODB_FIRSTBOOT_PATH = pjoin(PREFIX_DIR, '.firstboot_mongodb')
MONGODB_CONF = pjoin(ETC_DIR, 'mongodb.conf')
MONGODB_PIDFILE = pjoin(PIDS_DIR, 'mongodb.pid')
MONGODB_LOCKFILE = pjoin(MONGODB_DIR, 'mongod.lock')
MONGODB_LOG = pjoin(LOGS_DIR, 'mongodb.log')
MONGODB_JSON = pjoin(ETC_DIR, 'mongodb.json')
MONGODB_JS = pjoin(ETC_DIR, 'mongodb_firstboot.js')
MONGODB_KEYFILE = pjoin(ETC_DIR, 'mongodb.key')

CELERY_JSON = pjoin(ETC_DIR, 'celery.json')
CELERYD_PIDFILE = pjoin(PIDS_DIR, 'celeryd.pid')
CELERYBEAT_PIDFILE = pjoin(PIDS_DIR, 'celerybeat.pid')
CELERY_LOGFILE = pjoin(LOGS_DIR, 'celeryd.log')

NGINX_FIRSTBOOT_PATH = pjoin(PREFIX_DIR, '.firstboot_nginx')
NGINX_CONF = pjoin(ETC_DIR, 'nginx.conf')
NGINX_ACCESS_LOG = pjoin(LOGS_DIR, 'nginx_access.log')
NGINX_ERROR_LOG = pjoin(LOGS_DIR, 'nginx_error.log')
NGINX_PIDFILE = pjoin(PIDS_DIR, 'nginx.pid')

SUPERVISORD_FIRSTBOOT_PATH = pjoin(PREFIX_DIR, '.firstboot_supervisord')
SUPERVISORD_CONF = pjoin(ETC_DIR, 'supervisord.conf')
SUPERVISORD_PIDFILE = pjoin(PIDS_DIR, 'supervisord.pid')
SUPERVISORD_LOGFILE = pjoin(LOGS_DIR, 'supervisord.log')
SUPERVISORD_HISTORYFILE = pjoin(TMP_DIR, 'supervisord_history')

POSTGRESQL_FIRSTBOOT_PATH = pjoin(PREFIX_DIR, '.firstboot_postgresql')
_PGDATA = env.get('PGDATA')  # check if it's set in the environment
POSTGRESQL_PGDATA_PATH = _PGDATA or pjoin(PREFIX_DIR, 'postgresql_db')
POSTGRESQL_CONF = pjoin(ETC_DIR, 'pg_hba.conf')
POSTGRESQL_PIDFILE = pjoin(POSTGRESQL_PGDATA_PATH, 'postmaster.pid')
POSTGRESQL_LOGFILE = pjoin(LOGS_DIR, 'postgresql-server.log')


# ############################# DEFAULT CONFS ############################### #
DEFAULT_METRIQUE_JSON = utils.read_file('templates/etc/metrique.json')
DEFAULT_MONGODB_CONF = utils.read_file('templates/etc/mongodb.conf')
DEFAULT_MONGODB_JS = utils.read_file('templates/etc/mongodb_firstboot.js')
DEFAULT_NGINX_CONF = utils.read_file('templates/etc/nginx.conf')
DEFAULT_SUPERVISORD_CONF = utils.read_file('templates/etc/supervisord.conf')


###############################################################################
def adjust_options(options, args):
    options.no_site_packages = True
virtualenv.adjust_options = adjust_options


def backup_clean(args, path, prefix):
    keep = args.keep if args.keep != 0 else 3
    path = pjoin(path, prefix) + '*'
    files = sorted(glob.glob(path), reverse=True)
    to_remove = files[keep:]
    logger.debug('Removing %i backups' % len(to_remove))
    [utils.remove_file(f) for f in to_remove]


def celeryd_terminate(sig=None, frame=None):
    utils.terminate(CELERYD_PIDFILE)


def celeryd_loop(args):
    fork = not args.nofork
    x = 'worker'
    logfile = '--logfile=%s' % CELERY_LOGFILE
    loglvl = '-l INFO'
    pidfile = '--pidfile=%s' % CELERYD_PIDFILE
    app = '-A %s' % args.tasks_mod
    cmd = 'celery %s %s %s %s %s' % (x, logfile, loglvl, pidfile, app)
    utils.sys_call(cmd, fork=fork)


def celeryd_task(args):
    tasks = importlib.import_module(args.tasks_mod)
    task = getattr(tasks, args.task)
    return task.run()


def celeryd(args):
    if args.command == "start":
        celeryd_loop(args)
    elif args.command == "stop":
        utils.terminate(CELERYD_PIDFILE)
    elif args.command == "clean":
        utils.remove_file(CELERYD_PIDFILE)
    else:
        raise ValueError("unknown command %s" % args.command)


def celerybeat_terminate(sig=None, frame=None):
    utils.terminate(CELERYBEAT_PIDFILE)


def celerybeat_run(args):
    fork = not args.nofork
    x = 'beat'
    logfile = '--logfile=%s' % CELERY_LOGFILE
    loglvl = '-l INFO'
    pidfile = '--pidfile=%s' % CELERYBEAT_PIDFILE
    app = '-A %s' % args.tasks_mod
    cmd = 'celery %s %s %s %s %s' % (x, logfile, loglvl, pidfile, app)
    utils.sys_call(cmd, fork=fork)


def celerybeat(args):
    if args.command == "start":
        celerybeat_run(args)
    elif args.command == "stop":
        utils.terminate(CELERYBEAT_PIDFILE)
    elif args.command == "clean":
        utils.remove_file(CELERYBEAT_PIDFILE)
    else:
        raise ValueError("unknown command %s" % args.command)


def supervisord_terminate(sig=None, frame=None):
    utils.terminate(SUPERVISORD_PIDFILE)


def supervisord_run(args):
    cmd = 'supervisord -c %s' % SUPERVISORD_CONF
    utils.sys_call(cmd, fork=True)


def supervisord(args):
    if args.command == "start":
        supervisord_run(args)
    elif args.command == "stop":
        utils.terminate(SUPERVISORD_PIDFILE)
    elif args.command == "clean":
        utils.remove_file(SUPERVISORD_PIDFILE)
    elif args.command == "reload":
        utils.terminate(SUPERVISORD_PIDFILE, signal.SIGHUP)
    else:
        raise ValueError("unknown command %s" % args.command)


def nginx_terminate(sig=None, frame=None):
    utils.terminate(NGINX_PIDFILE)


def nginx(args):
    fork = not args.nofork
    cmd = 'nginx -c %s' % NGINX_CONF
    if args.command == 'test':
        utils.sys_call('%s -t' % cmd)
    elif args.command == 'start':
        utils.sys_call(cmd, fork=fork)
    elif args.command == 'stop':
        utils.sys_call('%s -s stop' % cmd)
    elif args.command == 'restart':
        for cmd in ('stop', 'start'):
            args.command = cmd
            nginx(args)
    elif args.command == 'reload':
        utils.sys_call('%s -s reload' % cmd)
    else:
        raise ValueError("unknown command %s" % args.command)


def mongodb_terminate(sig=None, frame=None):
    utils.terminate(MONGODB_PIDFILE)


def mongodb_start(fork=False, fast=True):
    fork = '--fork' if fork else ''
    if os.path.exists(MONGODB_PIDFILE):
        logger.info('MongoDB pid found not starting...')
        return False
    cmd = 'mongod -f %s %s' % (MONGODB_CONF, fork)
    cmd += ' --noprealloc --nojournal' if fast else ''
    utils.sys_call(cmd, fork=fork, pid_file=MONGODB_PIDFILE)
    return True


def mongodb_stop():
    utils.terminate(MONGODB_PIDFILE)
    mongodb_clean()


def mongodb_clean():
    utils.remove_file(MONGODB_LOCKFILE)
    utils.remove_file(MONGODB_PIDFILE)


def mongodb_trash():
    mongodb_stop()
    dest = pjoin(TRASH_DIR, 'mongodb-%s' % NOW)
    utils.move(MONGODB_DIR, dest)
    utils.move(MONGODB_CONF, dest)
    utils.move(MONGODB_JSON, dest)
    utils.move(MONGODB_JS, dest)
    utils.move(MONGODB_KEYFILE, dest)
    utils.remove_file(MONGODB_FIRSTBOOT_PATH)
    utils.make_dirs(MONGODB_DIR)


def mongodb_gen_keyfile():
    utils.sys_call('openssl rand -base64 741 -out %s' % MONGODB_KEYFILE)
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
    utils.sys_call(cmd)

    saveas = out + '.tar.gz'
    utils.backup(saveas, out)
    shutil.rmtree(out)

    backup_clean(args, BACKUP_DIR, 'mongodb')

    if args.scp_export:
        user = args.scp_user
        host = args.scp_host
        out_dir = args.scp_out_dir
        cmd = 'scp %s %s@%s:%s' % (saveas, user, host, out_dir)
        utils.sys_call(cmd)


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


def postgresql(args):
    # FIXME: disabling fork not possible at this time
    if args.command == 'start':
        postgresql_start()
    elif args.command == 'stop':
        postgresql_stop()
    elif args.command == 'restart':
        postgresql_stop()
        postgresql_start()
    elif args.command == 'clean':
        postgresql_clean()
    elif args.command == 'trash':
        postgresql_trash()
    else:
        raise ValueError("unknown command %s" % args.command)


def postgresql_start():
    if os.path.exists(POSTGRESQL_PIDFILE):
        logger.info('PostgreSQL pid found not starting...')
        return False

    try:
        cmd = 'pg_ctl -D %s -l %s -o \\"-k %s\\" start' % (
            POSTGRESQL_PGDATA_PATH, POSTGRESQL_LOGFILE, PIDS_DIR)
        utils.sys_call(cmd, sig=signal.SIGTERM, sig_func=postgresql_terminate)
    except Exception as e:
        logger.warn(e)
        return False
    else:
        return True


def postgresql_stop(quiet=True):
    try:
        cmd = 'pg_ctl -D %s stop' % (POSTGRESQL_PGDATA_PATH)
        utils.sys_call(cmd)
    except RuntimeError as e:
        if not quiet:
            raise
        else:
            logger.warn('Failed to stop PostgreSQL: %s' % e)
            return False


def postgresql_terminate(sig=None, frame=None):
    utils.terminate(POSTGRESQL_PIDFILE)


def postgresql_clean():
    utils.remove_file(POSTGRESQL_PIDFILE)


def postgresql_trash():
    postgresql_stop()
    dest = pjoin(TRASH_DIR, 'postgresql-%s' % NOW)
    utils.move(POSTGRESQL_PGDATA_PATH, dest)
    utils.remove_file(POSTGRESQL_FIRSTBOOT_PATH)
    utils.make_dirs(POSTGRESQL_PGDATA_PATH)


def rsync(args):
    compress = not args.nocompress
    utils.rsync(args.ssh_host, args.ssh_user, args.targets,
                compress, prefix=HOSTNAME)


def trash(args=None):
    named = getattr(args, 'named')
    named = '%s-%s' % (named[0], NOW) if named else NOW
    supervisord_terminate()
    celerybeat_terminate()
    celeryd_terminate()
    nginx_terminate()
    mongodb_terminate()
    postgresql_stop()

    dest = pjoin(TRASH_DIR, 'metrique-%s' % named)
    logger.warn('Trashing existing .metrique -> %s' % dest)
    for f in [ETC_DIR, PIDS_DIR, LOGS_DIR, CACHE_DIR,
              TMP_DIR, MONGODB_DIR, POSTGRESQL_PGDATA_PATH]:
        _dest = os.path.join(dest, os.path.basename(f))
        try:
            utils.move(f, _dest)
        except (IOError, OSError) as e:
            logger.error(e)
            continue
    firstboot_glob = os.path.join(PREFIX_DIR, '.firstboot*')
    utils.remove_file(firstboot_glob)


def setup(args, cmd, pip=False):
    pre = not getattr(args, 'no_pre', False)
    if pip and not pre:
        cmd += ' --pre'
    if isinstance(cmd, basestring):
        cmd = cmd.strip()
    else:
        cmd = ' '.join([s.strip() for s in cmd])
    if pip:
        out = utils.sys_call('pip %s -e .' % cmd)
    else:
        out = utils.sys_call('python setup.py %s' % cmd)
    logger.info(utils.to_encoding(out))


def _deploy_virtenv_init(args):
    _virtenv = utils.active_virtualenv()
    virtenv = getattr(args, 'virtenv') or _virtenv
    # skip if we're already in the targeted virtenv...
    if virtenv and virtenv != _virtenv:
        # we can't alrady be in a virtenv when running virtualenv.main()
        utils.virtualenv_deactivate()

        # scratch the existing virtenv directory, if requested
        if args.trash:
            utils.remove_file(virtenv, force=True)
            trash()

        # virtualenv.main; pass in only the virtenv path
        sys.argv = sys.argv[0:1] + [virtenv]
        # run the virtualenv script to install the virtenv
        virtualenv.main()

        # activate the newly installed virtenv
        utils.virtualenv_activate(args.virtenv)
    return virtenv


def _deploy_extras(args):
    pip = 'pip'
    _all = args.all
    _ = _all or args.ipython
    utils.sys_call('%s install -U ipython pyzmq jinja2' % pip) if _ else None
    _ = _all or args.test or args.pytest
    utils.sys_call('%s install -U pytest coveralls' % pip) if _ else None
    _ = args.all or args.docs
    utils.sys_call('%s install -U sphinx' % pip) if _ else None
    # pip-accel fails to install this package...
    utils.sys_call('pip install -U sphinx_bootstrap_theme') if _ else None
    _ = _all or args.supervisord
    utils.sys_call('%s install -U supervisor' % pip) if _ else None
    _ = _all or args.joblib
    utils.sys_call('%s install -U joblib' % pip) if _ else None
    _ = _all or args.postgres
    utils.sys_call('%s install -U psycopg2' % pip) if _ else None
    _ = _all or args.celery
    utils.sys_call('%s install -U celery' % pip) if _ else None
    _ = _all or args.sqlalchemy
    utils.sys_call('%s install -U sqlalchemy' % pip) if _ else None
    _ = _all or args.pymongo
    utils.sys_call('%s install -U pymongo pql' % pip) if _ else None
    _ = _all or args.pandas
    utils.sys_call('%s install -U pandas' % pip) if _ else None
    _ = _all or args.matplotlib
    utils.sys_call('%s install -U matplotlib' % pip) if _ else None
    _ = _all or args.dulwich
    utils.sys_call('%s install -U dulwich' % pip) if _ else None
    _ = _all or args.paramiko
    utils.sys_call('%s install -U paramiko' % pip) if _ else None


def deploy(args):
    virtenv = _deploy_virtenv_init(args)

    # make sure we have some basic defaults configured in the environment
    firstboot()

    # make sure we have the installer basics and their up2date
    # pip-accel caches compiled binaries
    utils.sys_call('pip install -U pip setuptools virtualenv')

    cmd = 'install'
    setup(args, cmd, pip=False)

    _deploy_extras(args)

    if args.develop:
        path = pjoin(virtenv, 'lib/python2.7/site-packages/metrique*')
        utils.remove_file(path)
        develop(args)

    # run py.test after install
    if args.test:
        # FIXME: testing mongodb components should be optional
        # and starting up mongodb etc should happen as a byproduct
        # of running the tests!
        #running = utils.get_pid(MONGODB_PIDFILE) != 0
        #if not running:
        #    utils.sys_call('metrique mongodb start --fast')
        utils.sys_call('coverage run --source=metrique -m py.test tests')


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
    utils.sys_call(
        'openssl req -new -x509 -days 365 -nodes '
        '-out %s -keyout %s -batch' % (SSL_CERT, SSL_KEY))
    with open(SSL_PEM, 'w') as pem:
        with open(SSL_CERT) as cert:
            pem.write(''.join(cert.readlines()))
        with open(SSL_KEY) as key:
            pem.write(''.join(key.readlines()))
    os.chmod(SSL_CERT, 0600)
    os.chmod(SSL_KEY, 0600)
    os.chmod(SSL_PEM, 0600)


def default_conf(path, template):
    if os.path.exists(path):
        path = '.'.join([path, 'default'])
    utils.write_file(path, template)
    logger.info("Installed %s ..." % path)


def firstboot(args=None, force=False):
    # make sure we have some basic defaults configured in the environment
    force = getattr(args, 'force', force)
    cmd = getattr(args, 'command', ['metrique'])
    if 'metrique' in cmd:
        sys_firstboot(force)
        pyclient_firstboot(force)
    if 'mongodb' in cmd:
        mongodb_firstboot(force)
    if 'postgresql' in cmd:
        postgresql_firstboot(force)
    if 'supervisord' in cmd:
        supervisord_firstboot(force)
    if 'nginx' in cmd:
        nginx_firstboot(force)
    logger.info('Firstboot complete.')


def postgresql_firstboot(force=False):
    exists = os.path.exists(POSTGRESQL_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return
    utils.make_dirs(POSTGRESQL_PGDATA_PATH)

    cmd = 'pg_ctl -D %s -l %s init' % (POSTGRESQL_PGDATA_PATH,
                                       POSTGRESQL_LOGFILE)
    utils.sys_call(cmd)

    started = False
    try:
        started = postgresql_start()
        time.sleep(1)
        cmd = 'createdb -h 127.0.0.1'
        utils.sys_call(cmd)
        cmd = 'psql -h 127.0.0.1 -c \\"%s\\"'
        P = PASSWORD
        tz = "set timezone TO 'GMT';"
        encoding = "set client_encoding TO 'utf8';"
        admin_db = "CREATE DATABASE admin WITH OWNER admin;"
        admin_user = "CREATE USER admin WITH PASSWORD \\'%s\\' SUPERUSER;" % P
        test_db = "CREATE DATABASE test WITH OWNER test;"
        test_user = "CREATE USER test WITH PASSWORD \\'%s\\' SUPERUSER;" % P
        [utils.sys_call(cmd % sql) for sql in (tz, encoding, admin_user,
                                               admin_db, test_user, test_db)]
    finally:
        if started:
            postgresql_stop()

    utils.write_file(POSTGRESQL_FIRSTBOOT_PATH, '')
    return True


def mongodb_firstboot(force=False):
    exists = os.path.exists(MONGODB_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return
    utils.make_dirs(MONGODB_DIR)
    mongodb_gen_keyfile()

    global DEFAULT_MONGODB_CONF, DEFAULT_MONGODB_JS
    DEFAULT_MONGODB_CONF = DEFAULT_MONGODB_CONF % (
        MONGODB_DIR, MONGODB_LOG, MONGODB_PIDFILE, LOCAL_IP, SSL_PEM,
        MONGODB_KEYFILE)
    DEFAULT_MONGODB_JS = DEFAULT_MONGODB_JS % (PASSWORD)

    default_conf(MONGODB_CONF, DEFAULT_MONGODB_CONF)
    utils.write_file(MONGODB_FIRSTBOOT_PATH, '')


def mongodb_install_admin():
    # by installing 'admin' user in Travis-ci we enable
    # authentication; flag here is to disable that
    # for testing
    default_conf(MONGODB_JS, DEFAULT_MONGODB_JS)
    mongodb_start(fork=True, fast=True)
    logger.debug('MongoDB forking, sleeping for a moment...')
    time.sleep(1)
    try:
        utils.sys_call('mongo 127.0.0.1/admin %s' % (MONGODB_JS))
    finally:
        mongodb_stop()


def pyclient_firstboot(force=False):
    exists = os.path.exists(METRIQUE_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return

    global DEFAULT_METRIQUE_JSON

    DEFAULT_METRIQUE_JSON = DEFAULT_METRIQUE_JSON % (
        LOCAL_IP, PASSWORD, LOCAL_IP, PASSWORD,
        LOCAL_IP, PASSWORD, SSL_PEM,
        PASSWORD, PASSWORD, LOCAL_IP)

    default_conf(METRIQUE_JSON, DEFAULT_METRIQUE_JSON)
    utils.write_file(METRIQUE_FIRSTBOOT_PATH, '')


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
    utils.write_file(SUPERVISORD_FIRSTBOOT_PATH, '')


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
    utils.write_file(NGINX_FIRSTBOOT_PATH, '')


def sys_firstboot(force=False):
    exists = os.path.exists(SYS_FIRSTBOOT_PATH)
    if exists and not force:
        # skip if we have already run this before
        return

    # create default dirs in advance
    [utils.make_dirs(p) for p in (PREFIX_DIR, PIP_CACHE_DIR, PIP_ACCEL_DIR,
                                  PIP_EGGS, TRASH_DIR, LOGS_DIR,
                                  ETC_DIR, BACKUP_DIR, TMP_DIR, CACHE_DIR,
                                  STATIC_DIR, PIDS_DIR)]

    # make sure the the default user python eggs dir is secure
    os.chmod(PIP_EGGS, 0700)

    # generate self-signed ssl certs
    try:
        ssl()
    except Exception as e:
        logger.warn('Failed to create ssl certs: %s' % e)

    utils.write_file(SYS_FIRSTBOOT_PATH, '')


def main():
    import argparse

    cli = argparse.ArgumentParser(description='Metrique Manage CLI')
    cli.add_argument('-V', '--virtenv')
    cli.add_argument('-v', '--verbose')

    _sub = cli.add_subparsers(description='action', dest='action')

    # Automated metrique deployment
    _deploy = _sub.add_parser('deploy')
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
        '--sqlalchemy', action='store_true', help='install sqlalchemy')
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
        '--pymongo', action='store_true', help='install pymongo, pql')
    _deploy.add_argument(
        '--pandas', action='store_true', help='install pandas')
    _deploy.add_argument(
        '--matplotlib', action='store_true', help='install matplotlib')
    _deploy.add_argument(
        '--dulwich', action='store_true', help='install dulwich')
    _deploy.add_argument(
        '--paramiko', action='store_true', help='install paramiko')
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
    _trash.add_argument('named', nargs='*')
    _trash.set_defaults(func=trash)

    # Clean-up routines
    _firstboot = _sub.add_parser('firstboot')
    _firstboot.add_argument('command',
                            nargs='+',
                            choices=['metrique', 'mongodb', 'postgresql',
                                     'celery', 'supervisord', 'nginx'])
    _firstboot.add_argument('-f', '--force', action='store_true')
    #_firstboot.add_argument('-A', '--no-auth', action='store_true')
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

    # postgresql server
    _postgresql = _sub.add_parser('postgresql')
    _postgresql.add_argument('command', choices=['start', 'stop',
                                                 'trash', 'clean', 'reload'])
    _postgresql.add_argument('-F', '--nofork', action='store_true')
    _postgresql.set_defaults(func=postgresql)

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
    logger.debug('User Path   : %s' % PREFIX_DIR)
    logger.debug('-' * 30)

    if args.action != 'deploy':
        # Activate the virtual environment in this python session if
        # parent env has one set
        utils.virtualenv_deactivate()

    # run command
    args.func(args)


if __name__ == '__main__':
    main()
