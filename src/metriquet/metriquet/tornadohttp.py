#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
from functools import partial
import os
import signal
import time
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import Application

from metriqueu.jsonconf import JSONConf

# setup default root logger, but remove default StreamHandler (stderr)
# Handlers will be added upon __init__()
logging.basicConfig()
root_logger = logging.getLogger()
[root_logger.removeHandler(hdlr) for hdlr in root_logger.handlers]
BASIC_FORMAT = "%(name)s.%(process)s:%(asctime)s:%(message)s"
LOG_FORMAT = logging.Formatter(BASIC_FORMAT, "%Y%m%dT%H%M%S")


BASENAME = 'tornado'

LOGIN_URL = '/login'
SECRET = '__DEFAULT_COOKIE_SECRET__'
SSL_CERT = 'mydomain.crt'
SSL_KEY = 'mydomain.key'

USER_DIR = os.path.expanduser('~/.metrique')
ETC_DIR = os.path.join(USER_DIR, 'etc')
CACHE_DIR = os.path.join(USER_DIR, 'cache')
PID_DIR = os.path.join(USER_DIR, 'pids')
LOG_DIR = os.path.join(USER_DIR, 'logs')
STATIC_PATH = os.path.join(USER_DIR, 'static/')
TEMPLATE_PATH = os.path.join(USER_DIR, 'templates/')


class TornadoConfig(JSONConf):
    def __init__(self, config_file=None, name=None, **kwargs):
        self.name = name or BASENAME
        log_file = '%s.log' % self.name

        self.config = {
            'host': '127.0.0.1',
            'port': 8080,
            'debug': True,
            'gzip': True,
            'login_url': LOGIN_URL,
            'cookie_secret': SECRET,
            'xsrf_cookies': False,
            'autoreload': False,
            'ssl': False,
            'ssl_certificate': SSL_CERT,
            'ssl_certificate_key': SSL_KEY,
            'pid_name': self.name,
            'cachedir': CACHE_DIR,
            'piddir': PID_DIR,
            'logdir': LOG_DIR,
            'logstdout': True,
            'log2file': False,
            'logfile': log_file,
            'logrotate': False,
            'logkeep': 3,
            'static_path': STATIC_PATH,
            'template_path': TEMPLATE_PATH,
        }

        # make sure kwargs passed in are set
        for k, v in kwargs.items():
            self.config[k] = v

        # update the config with the args from the config_file
        super(TornadoConfig, self).__init__(config_file=config_file)


class TornadoHTTPServer(object):
    ''' HTTP (Tornado >=3.0) implemntation of MetriqueServer '''
    conf = TornadoConfig()
    parent_pid = None
    child_pid = None
    handlers = []
    name = BASENAME

    def __init__(self, config_file=None, name=None, **kwargs):
        self.name = name or self.name
        self.conf = TornadoConfig(config_file=config_file, **kwargs)

    def _setup_logger(self, logger, propagate=0):
        logdir = os.path.expanduser(self.conf.logdir)
        logfile = os.path.join(logdir, self.conf.logfile)

        if self.conf.logstdout:
            hdlr = logging.StreamHandler()
            hdlr.setFormatter(LOG_FORMAT)
            logger.addHandler(hdlr)

        if self.conf.log2file and logfile:
            if self.conf.logrotate:
                hdlr = logging.handlers.RotatingFileHandler(
                    logfile, backupCount=self.conf.logkeep,
                    maxBytes=self.conf.logrotate)
            else:
                hdlr = logging.FileHandler(logfile)
            hdlr.setFormatter(LOG_FORMAT)
            logger.addHandler(hdlr)

        if self.conf.debug in [-1, False]:
            logger.setLevel(logging.WARN)
        elif self.conf.debug in [0, None]:
            logger.setLevel(logging.INFO)
        elif self.conf.debug in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)

        logger.propagate = propagate

        return logger

    def setup_logger(self):
        # override root logger so all output goes to one place
        self._setup_logger(logging.getLogger())
        # prepare the app logger this instance will use
        return self._setup_logger(logging.getLogger(self.name))

    @property
    def pid(self):
        return os.getpid()

    @property
    def pid_file(self):
        pid_file = '%s.%s.pid' % (self.conf.pid_name, str(self.pid))
        path = os.path.join(self.conf.piddir, pid_file)
        return os.path.expanduser(path)

    def _prepare_web_app(self):
        ''' Config and Views'''
        self.logger.debug("tornado web app setup")

        self._web_app = Application(
            gzip=self.conf.gzip,
            debug=self.conf.debug,
            static_path=self.conf.static_path,
            handlers=self.handlers,
            cookie_secret=self.conf.cookie_secret,
            login_url=self.conf.login_url,
            xsrf_cookies=self.conf.xsrf_cookies,
            template_path=self.conf.template_path,
        )

        if self.conf.debug and not self.conf.autoreload:
            # FIXME hack to disable autoreload when debug is True
            from tornado import autoreload
            autoreload._reload_attempted = True
            autoreload._reload = lambda: None

        if self.conf.ssl:
            ssl_options = dict(
                certfile=os.path.expanduser(self.conf.ssl_certificate),
                keyfile=os.path.expanduser(self.conf.ssl_certificate_key))
            self.server = HTTPServer(self._web_app, ssl_options=ssl_options)
        else:
            self.server = HTTPServer(self._web_app)

    def set_pid(self):
        if os.path.exists(self.pid_file):
            raise RuntimeError(
                "pid (%s) found in (%s)" % (self.pid,
                                            self.pid_file))
        else:
            with open(self.pid_file, 'w') as _file:
                _file.write(str(self.pid))
        signal.signal(signal.SIGTERM, self._inst_terminate_handler)
        signal.signal(signal.SIGINT, self._inst_kill_handler)
        self.logger.debug("PID stored (%s)" % self.pid)

    def remove_pid(self, quiet=False):
        error = None
        try:
            os.remove(self.pid_file)
        except OSError as error:
            if not quiet:
                self.logger.error(
                    'pid file not removed (%s); %s' % (self.pid_file, error))
        else:
            if not quiet:
                self.logger.debug("removed PID file: %s" % self.pid_file)

    def _init_basic_server(self):
        self.logger.debug('======= %s =======' % self.name)
        self.logger.debug(' Conf: %s' % self.conf.config_file)
        self.logger.debug(' Host: %s' % self.uri)
        self.logger.debug('  SSL: %s' % self.conf.ssl)

        self.server.listen(port=self.conf.port, address=self.conf.host)
        IOLoop.instance().start()

    def spawn_instance(self):
        self.logger.debug("spawning tornado %s..." % self.uri)
        self.set_pid()
        self._init_basic_server()

    @property
    def uri(self):
        host = self.conf.host
        ssl = self.conf.ssl
        port = self.conf.port
        uri = 'https://%s' % host if ssl else 'http://%s' % host
        uri += ':%s' % port
        return uri

    def start(self, fork=False):
        ''' Start a new tornado web app '''
        self._prepare_web_app()
        if fork:
            pid = os.fork()
            if pid == 0:
                self.spawn_instance()
        else:
            pid = self.pid
            self.spawn_instance()
        return pid

    def stop(self, delay=None):
        ''' Stop a running tornado web app '''
        if self.child_pid:
            os.kill(self.child_pid, signal.SIGTERM)
        else:
            self.server.stop()  # stop this tornado instance
            delayed_kill = partial(self._inst_delayed_stop, delay)
            IOLoop.instance().add_callback(delayed_kill)

    def _inst_stop(self, sig, delay=None):
        if self.child_pid:
            os.kill(self.child_pid, sig)
        else:
            self.stop(delay=delay)
        self.remove_pid(quiet=True)

    def _inst_terminate_handler(self, sig, frame):
        self.logger.debug("[INST] (%s) recieved TERM signal" % self.pid)
        self._inst_stop(sig)

    def _inst_kill_handler(self, sig, frame):
        self.logger.debug("[INST] (%s) recieved KILL signal" % self.pid)
        self._inst_stop(sig, 0)

    def _inst_delayed_stop(self, delay=None):
        if delay is None:
            if self.conf.debug:
                delay = 0
            else:
                delay = 5
        self.logger.debug("stop ioloop called (%s)... " % self.pid)
        TIMEOUT = float(delay) + time.time()
        self.logger.debug("Shutting down in T-%i seconds ..." % delay)
        IOLoop.instance().add_timeout(TIMEOUT, self._stop_ioloop)

    def _stop_ioloop(self):
        IOLoop.instance().stop()
        self.logger.debug("IOLoop stopped")
