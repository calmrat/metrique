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
TEMP_DIR = os.path.join(USER_DIR, 'tmp')
STATIC_PATH = os.path.join(USER_DIR, 'static/')
TEMPLATE_PATH = os.path.join(USER_DIR, 'templates/')


class TornadoConfig(JSONConf):
    name = BASENAME

    def __init__(self, config_file=None, **kwargs):
        log_file = '%s.log' % self.name
        log_requests_file = '%s_access.log' % self.name
        ssl_cert = '%s.crt' % self.name
        ssl_key = '%s.key' % self.name

        config = {
            'autoreload': False,
            'cachedir': CACHE_DIR,
            'configdir':  ETC_DIR,
            'cookie_secret': SECRET,
            'debug': True,
            'gzip': True,
            'host': '127.0.0.1',
            'logdir': LOG_DIR,
            'logstdout': True,
            'log2file': False,
            'logfile': log_file,
            'log_keep': 3,
            'log_rotate': False,
            'log_rotate_bytes': 134217728,  # 128M 'maxBytes' before rotate
            'log_requests_file': log_requests_file,
            'log_requests_level': 100,
            'login_url': LOGIN_URL,
            'pid_name': self.name,
            'piddir': PID_DIR,
            'port': 8080,
            'realm': self.name,
            'ssl': False,
            'ssl_certificate': ssl_cert,
            'ssl_certificate_key': ssl_key,
            'static_path': STATIC_PATH,
            'temp_path': TEMP_DIR,
            'template_path': TEMPLATE_PATH,
            'userdir': USER_DIR,
            'xsrf_cookies': False,
        }
        # apply defaults
        self.config.update(config)
        # update the config with the args from the config_file
        super(TornadoConfig, self).__init__(config_file=config_file)
        # anything passed in explicitly gets precedence
        self.config.update(kwargs)


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

    def _log_handler_get_level(self, level):
        level = level or self.conf.debug
        if level in [-1, False]:
            level = logging.WARN
        elif level is True or level >= 1:
            level = logging.DEBUG
        elif level in [0, None]:
            level = logging.INFO
        else:
            level = int(level)
        return level

    def _log_stream_handler(self, level=None, fmt=LOG_FORMAT):
        hdlr = logging.StreamHandler()
        hdlr.setFormatter(fmt)
        hdlr.setLevel(self._log_handler_get_level(level))
        return hdlr

    def _log_file_handler(self, level=None, logdir=None, logfile=None,
                          rotate=None, rotate_bytes=None, rotate_keep=None,
                          fmt=LOG_FORMAT):
        logdir = logdir or self.conf.logdir
        logdir = os.path.expanduser(logdir)
        logfile = logfile or self.conf.logfile
        logfile = os.path.join(logdir, logfile)
        rotate = rotate or self.conf.log_rotate
        rotate_bytes = rotate_bytes or self.conf.log_rotate_bytes
        rotate_keep = rotate_keep or self.conf.log_keep

        if rotate:
            hdlr = logging.handlers.RotatingFileHandler(
                logfile, backupCount=rotate_keep, maxBytes=rotate_bytes)
        else:
            hdlr = logging.FileHandler(logfile)
        hdlr.setFormatter(fmt)
        hdlr.setLevel(self._log_handler_get_level(level))
        return hdlr

    def _setup_logger(self, logger_name, level=None, logstdout=None,
                      log2file=None, logdir=None, logfile=None, rotate=None,
                      rotate_bytes=None, rotate_keep=None, fmt=None):
        logstdout = logstdout or self.conf.logstdout
        stdout_hdlr = self._log_stream_handler(level) if logstdout else None

        file_hdlr = None
        log2file = log2file or self.conf.log2file
        if log2file:
            file_hdlr = self._log_file_handler(level, logdir, logfile, rotate,
                                               rotate_bytes, rotate_keep)

        logger = logging.getLogger(logger_name)

        if stdout_hdlr:
            logger.addHandler(stdout_hdlr)
        if file_hdlr:
            logger.addHandler(file_hdlr)

        logger.setLevel(self._log_handler_get_level(level))
        logger.propagate = 0
        return logger

    def setup_logger(self):
        # override root logger so all app logging goes to one place
        self._setup_logger(logger_name=None)

        # prepare 'request' logger for storing request details
        logfile = self.conf.log_requests_file
        logger_name = '%s.requests' % self.name
        requests_level = self.conf.log_requests_level
        self.request_logger = self._setup_logger(logger_name=logger_name,
                                                 logfile=logfile,
                                                 level=requests_level)
        # this app's main logger
        app_logger = logging.getLogger(self.name)
        # add request handler output alias
        app_logger.log_request = partial(self.request_logger.log,
                                         requests_level)
        # set expected self.logger instance attr and return
        self.logger = app_logger
        return self.logger

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
