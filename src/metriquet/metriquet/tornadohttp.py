#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriquet.tornadohttp
~~~~~~~~~~~~~~~~~~~~~

Generic, re-usable Tornado server and config classes.

Supports log handling, server startup / shutdown and
configuration loading.
'''

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
STATIC_DIR = os.path.join(USER_DIR, 'static/')
API_DOCS_PATH = os.path.join(STATIC_DIR, 'api_docs/')
TEMPLATE_PATH = os.path.join(USER_DIR, 'templates/')


class TornadoConfig(JSONConf):
    '''
    Tornado default config class. All tornado subclasses should
    derive their config objects from this class to ensure defaults
    values are available.

    Pure defaults assume local, insecure 'test', 'development'
    or 'personal' environment. The defaults are NOT for production
    use!

    To customize local client configuration, add/update
    `~/.metrique/etc/tornado.json` (default).

    This configuration class defines the following overrideable defaults.

    :param config_file:
        path to json config file to load over defaults ($default_config)
    :param autoreload: permit tornado autoreload on file write?
    :param cachedir: path to directory where cache is to be saved
    :param configdir: path to directory where config files are stored
    :param cookie_secret: random key for signing secure cookies
    :param debug: verbosity level
    :param gzip: enable gzip compression of requests?
    :param host: host address to listen on
    :param logdir: path to directory where logs are saved
    :param logstdout: enable logging to stdout?
    :param log2file: enable logging to disk?
    :param logfile: log file name
    :param log_keep: number of logs to save after rotation?
    :param log_rotate: enable automatic log rotation?
    :param log_rotate_bytes: max size of logs before rotation
    :param log_requests_file: filename of 'access log'
    :param log_requests_level: logger level that requests will be queue to
    :param login_url: relative path of login url
    :param pid_name: prefix string to be used for auto-naming pid files
    :param piddir: path to directory where pid files are saved
    :param port: port to list on
    :param realm: authentication realm name
    :param ssl: enable ssl?
    :param ssl_certificate: path to ssl certificate file
    :param ssl_certificate_key: path to ssl certificate key file
    :param static_path: path to where static files are found
    :param temp_path: path to directory where temporary files are saved
    :param template_path: path to directory where template files are found
    :param userdir: path to directory where user files are stored
    :param xsrf_cookies: enable xsrf_cookie form validation?

    :ivar name: name of the tornado instance
    '''
    name = BASENAME

    def __init__(self, config_file=None, **kwargs):
        log_file = '%s.log' % self.name
        log_requests_file = '%s_access.log' % self.name
        ssl_cert = '%s.crt' % self.name
        ssl_key = '%s.key' % self.name

        config = {
            'api_docs': API_DOCS_PATH,
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
            'static_path': STATIC_DIR,
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

    def __init__(self, config_file=None, **kwargs):
        self.config = TornadoConfig(config_file=config_file, **kwargs)

    def _log_handler_get_level(self, level):
        level = level or self.config.debug
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
        logdir = logdir or self.config.logdir
        logdir = os.path.expanduser(logdir)
        logfile = logfile or self.config.logfile
        logfile = os.path.join(logdir, logfile)
        rotate = rotate or self.config.log_rotate
        rotate_bytes = rotate_bytes or self.config.log_rotate_bytes
        rotate_keep = rotate_keep or self.config.log_keep

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
        logstdout = logstdout or self.config.logstdout
        stdout_hdlr = self._log_stream_handler(level) if logstdout else None

        file_hdlr = None
        log2file = log2file or self.config.log2file
        if log2file:
            file_hdlr = self._log_file_handler(level, logdir, logfile, rotate,
                                               rotate_bytes, rotate_keep)

        logger = logging.getLogger(logger_name)

        # clear existing handlers
        [logger.removeHandler(hdlr) for hdlr in logger.handlers]

        if stdout_hdlr:
            logger.addHandler(stdout_hdlr)
        if file_hdlr:
            logger.addHandler(file_hdlr)

        logger.setLevel(self._log_handler_get_level(level))
        logger.propagate = 0
        return logger

    def setup_logger(self):
        '''Setup application logging

        The following loggers will be available:
            * self.logger - main

        By default, .debug, .info, .warn, etc methods are available in
        the main logger.

        Additionally, the following methods are also available via .logger:
            * self.logger.request_logger

        The base logger name is the value of the class attribute `name`.
        '''
        # override root logger so all app logging goes to one place
        self._setup_logger(logger_name=None)

        # prepare 'request' logger for storing request details
        logfile = self.config.log_requests_file
        logger_name = '%s.requests' % self.name
        requests_level = self.config.log_requests_level

        # this app's main logger
        app_logger = self._setup_logger(logger_name=self.name)

        self.request_logger = self._setup_logger(logger_name=logger_name,
                                                 logfile=logfile,
                                                 level=requests_level)
        # add request handler output alias
        app_logger.log_request = partial(self.request_logger.log,
                                         requests_level)
        # set expected self.logger instance attr and return
        self.logger = app_logger
        return self.logger

    @property
    def pid(self):
        '''Wrapper for os.getpid()'''
        return os.getpid()

    @property
    def pid_file(self):
        '''Return back the name of the current instance's pid file on disk'''
        pid_file = '%s.%s.pid' % (self.config.pid_name, str(self.pid))
        path = os.path.join(self.config.piddir, pid_file)
        return os.path.expanduser(path)

    def _prepare_web_app(self):
        ''' Config and Views'''
        self.logger.debug("tornado web app setup")

        self._web_app = Application(
            gzip=self.config.gzip,
            debug=self.config.debug,
            static_path=self.config.static_path,
            handlers=self.handlers,
            cookie_secret=self.config.cookie_secret,
            login_url=self.config.login_url,
            xsrf_cookies=self.config.xsrf_cookies,
            template_path=self.config.template_path,
        )

        if self.config.debug and not self.config.autoreload:
            # FIXME hack to disable autoreload when debug is True
            from tornado import autoreload
            autoreload._reload_attempted = True
            autoreload._reload = lambda: None

        if self.config.ssl:
            ssl_options = dict(
                certfile=os.path.expanduser(self.config.ssl_certificate),
                keyfile=os.path.expanduser(self.config.ssl_certificate_key))
            self.server = HTTPServer(self._web_app, ssl_options=ssl_options)
        else:
            self.server = HTTPServer(self._web_app)

    def set_pid(self):
        '''Store the current instances pid number into a pid file on disk'''
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
        '''Remove existing pid file on disk, if available'''
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
        self.logger.debug(' Conf: %s' % self.config.config_file)
        self.logger.debug(' Host: %s' % self.uri)
        self.logger.debug('  SSL: %s' % self.config.ssl)

        self.server.listen(port=self.config.port, address=self.config.host)
        IOLoop.instance().start()

    def spawn_instance(self):
        '''Spawn a new tornado server instance'''
        self.logger.debug("spawning tornado %s..." % self.uri)
        self.set_pid()
        self._init_basic_server()

    @property
    def uri(self):
        '''Return a uri connection string for the current tornado instance'''
        host = self.config.host
        ssl = self.config.ssl
        port = self.config.port
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
            if self.config.debug:
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
