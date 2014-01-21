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
BASIC_FORMAT = "%(name)s:%(asctime)s:%(message)s"
LOG_FORMAT = logging.Formatter(BASIC_FORMAT, "%Y%m%dT%H%M%S")


BASENAME = 'tornado'

STATIC_PATH = 'static/'
LOGIN_URL = '/login'
COOKIE_SECRET = '__DEFAULT_COOKIE_SECRET__'
SSL_CERT = 'mydomain.crt'
SSL_KEY = 'mydomain.key'
PID_DIR = ''
LOG_DIR = ''
LOG_FILE = '%s.log' % BASENAME


class TornadoHTTPServer(object):
    ''' HTTP (Tornado >=3.0) implemntation of MetriqueServer '''
    conf = JSONConf()

    def __init__(self, **kwargs):
        self.parent_pid = None
        self.child_pid = None
        self.handlers = []

        # key aliases (to shorten line <80c)
        cert = 'ssl_certificate'
        key = 'ssl_certificate_key'

        self.conf['host'] = kwargs.pop('host', '127.0.0.1')
        self.conf['port'] = kwargs.pop('port', 8080)
        self.conf['debug'] = kwargs.pop('debug', True)
        self.conf['gzip'] = kwargs.pop('gzip', True)
        self.conf['login_url'] = kwargs.pop('login_url', LOGIN_URL)
        self.conf['static_path'] = kwargs.pop('static_path', STATIC_PATH)
        self.conf['cookie_secret'] = kwargs.pop('cookie_secret', COOKIE_SECRET)
        self.conf['xsrf_cookies'] = kwargs.pop('xsrf_cookies', False)
        self.conf['autoreload'] = kwargs.pop('autoreload', False)
        self.conf['ssl'] = kwargs.pop('ssl', False)
        self.conf['ssl_certificate'] = kwargs.pop(cert, SSL_CERT)
        self.conf['ssl_certificate_key'] = kwargs.pop(key, SSL_KEY)
        self.conf['pid_name'] = kwargs.pop('pid_name', BASENAME)
        self.conf['piddir'] = kwargs.pop('piddir', PID_DIR)
        self.conf['logdir'] = kwargs.pop('logdir', LOG_DIR)
        self.conf['logstdout'] = kwargs.pop('logstdout', True)
        self.conf['log2file'] = kwargs.pop('logstdout', False)
        self.conf['logfile'] = kwargs.pop('logfile', LOG_FILE)
        self.conf['logrotate'] = kwargs.pop('logrotate', False)
        self.conf['logkeep'] = kwargs.pop('logkeep', 3)
        self.conf['logger_propogate'] = kwargs.pop('logkeep', False)
        self.conf['logger_name'] = kwargs.pop('logger_name', BASENAME)

        self.setup_logger()

    @property
    def logger_name(self):
        logger_name = '%s.%s' % (self.conf['logger_name'], self.pid)
        return logger_name

    def setup_logger(self):
        logdir = os.path.expanduser(self.conf.logdir)
        logfile = os.path.join(logdir, self.conf.logfile)

        if self.conf.debug == 2:
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)

        logger = logging.getLogger(self.logger_name)
        logger.propagate = self.conf.logger_propogate

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
        self.logger = logger

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
