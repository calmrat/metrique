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

from metriqued.config import metriqued_config, mongodb_config
from metriqued import core_api, cube_api, query_api, user_api

# setup default root logger, but remove default StreamHandler (stderr)
# Handlers will be added upon __init__()
logging.basicConfig()
root_logger = logging.getLogger()
[root_logger.removeHandler(hdlr) for hdlr in root_logger.handlers]
BASIC_FORMAT = "%(name)s:%(asctime)s:%(message)s"

USER_DIR = os.path.expanduser('~/.metrique')
ETC_DIR = os.path.join(USER_DIR, 'etc')
METRIQUED_JSON = os.path.join(ETC_DIR, 'metriqued.json')


def user_cube(value):
    user_cube = r'(\w+)/([-\w]+)'
    path = os.path.join(user_cube, str(value))
    return path


def api_v2(value):
    return os.path.join(r'/api/v2', str(value))


def api_v1(value):
    return os.path.join(r'/api/v1', str(value))


def ucv2(value):
    uc = user_cube(value)
    a2 = api_v2(uc)
    return a2


class TornadoHTTPServer(object):
    ''' HTTP (Tornado >=3.0) implemntation of MetriqueServer '''
    def __init__(self, config_file=None, **kwargs):
        if not config_file:
            config_file = METRIQUED_JSON
        self.mconf = metriqued_config(config_file=config_file)
        self.dbconf = mongodb_config(self.mconf.mongodb_config)

        # update metrique config object with any additional kwargs
        for k, v in kwargs.items():
            if v is not None:
                self.mconf[k] = v

        self.mconf.logdir = os.path.expanduser(self.mconf.logdir)
        if not os.path.exists(self.mconf.logdir):
            os.makedirs(self.mconf.logdir)
        self.mconf.logfile = os.path.join(self.mconf.logdir,
                                          self.mconf.logfile)
        self.debug_set()

        self.parent_pid = None
        self.child_pid = None

        self._prepare_handlers()

        host = self.mconf['host']
        ssl = self.mconf['ssl']
        port = self.mconf['port']
        self.uri = 'https://%s' % host if ssl else 'http://%s' % host
        self.uri += ':%s' % port

        self.logger.debug('======= metrique =======')
        self.logger.debug(' Conf: %s' % self.mconf.config_file)
        self.logger.debug(' Host: %s' % self.uri)
        self.logger.debug('  SSL: %s' % ssl)
        self.logger.debug('======= mongodb ========')
        self.logger.debug(' Conf: %s' % self.dbconf.config_file)
        self.logger.debug(' Host: %s' % self.dbconf.host)
        self.logger.debug('  SSL: %s' % self.dbconf.ssl)
        self.logger.debug(' Port: %s' % self.dbconf.port)

    def debug_set(self):
        '''
        if we get a level of 2, we want to apply the
        debug level to all loggers
        '''
        level = self.mconf['debug']
        logstdout = self.mconf['logstdout']
        logfile = self.mconf['logfile']
        logrotate = self.mconf['logrotate']
        logkeep = self.mconf['logkeep']

        logdir = os.path.expanduser(self.mconf.logdir)
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        logfile = os.path.join(logdir, logfile)

        basic_format = logging.Formatter(BASIC_FORMAT, "%Y%m%dT%H%M%S")

        if level == 2:
            self._logger_name = None
            logger = logging.getLogger('metriqued')
            logger.propagate = 0
        else:
            self._logger_name = 'metriqued.%s' % self.pid
            logger = logging.getLogger(self._logger_name)
            logger.propagate = 0

        # reset handlers
        logger.handlers = []

        if logstdout:
            hdlr = logging.StreamHandler()
            hdlr.setFormatter(basic_format)
            logger.addHandler(hdlr)

        if self.mconf.log2file and logfile:
            logfile = os.path.expanduser(logfile)
            if logrotate:
                hdlr = logging.handlers.RotatingFileHandler(
                    logfile, backupCount=logkeep, maxBytes=logrotate)
            else:
                hdlr = logging.FileHandler(logfile)
            hdlr.setFormatter(basic_format)
            logger.addHandler(hdlr)

        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)
        self.logger = logger

    @property
    def pid(self):
        return os.getpid()

    @property
    def pid_file(self):
        pid_file = 'metriqued.%s.pid' % str(self.pid)
        path = os.path.join(self.mconf['piddir'], pid_file)
        return os.path.expanduser(path)

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

    def _prepare_handlers(self):
        base_handlers = [
            (r"/register", user_api.RegisterHdlr),
            (r"/login", user_api.LoginHdlr),
            (r"/logout", user_api.LogoutHdlr),

            (r"/(\w+)/aboutme", user_api.AboutMeHdlr),
            (r"/(\w+)/passwd", user_api.UpdatePasswordHdlr),
            (r"/(\w+)/remove", user_api.RemoveHdlr),
            (r"/(\w+)/update_profile", user_api.UpdateProfileHdlr),
            (r"/(\w+)/update_group", user_api.UpdateGroupHdlr),
            (r"/(\w+)/update_properties", user_api.UpdatePropertiesHdlr),

            (api_v1(r""), core_api.ObsoleteAPIHdlr),
            (api_v2(r"ping"), core_api.PingHdlr),

            (api_v2(r"(\w+)?/?([-\w]+)?"), cube_api.ListHdlr),
        ]

        user_cube_handlers = [
            (ucv2(r"find"), query_api.FindHdlr),
            (ucv2(r"history"), query_api.HistoryHdlr),
            (ucv2(r"deptree"), query_api.DeptreeHdlr),
            (ucv2(r"count"), query_api.CountHdlr),
            (ucv2(r"aggregate"), query_api.AggregateHdlr),
            (ucv2(r"distinct"), query_api.DistinctHdlr),
            (ucv2(r"sample"), query_api.SampleHdlr),

            (ucv2(r"index"), cube_api.IndexHdlr),
            (ucv2(r"save"), cube_api.SaveObjectsHdlr),
            (ucv2(r"rename"), cube_api.RenameHdlr),
            (ucv2(r"remove"), cube_api.RemoveObjectsHdlr),
            (ucv2(r"export"), cube_api.ExportHdlr),
            (ucv2(r"update_role"), cube_api.UpdateRoleHdlr),
            (ucv2(r"drop"), cube_api.DropHdlr),
            (ucv2(r"stats"), cube_api.StatsHdlr),
            (ucv2(r"register"), cube_api.RegisterHdlr),
        ]

        self._api_v2_handlers = base_handlers + user_cube_handlers

    def _prepare_web_app(self):
        ''' Config and Views'''
        self.logger.debug("tornado web app setup")
        debug = self.mconf.debug == 2
        gzip = self.mconf.gzip
        login_url = self.mconf.login_url
        static_path = self.mconf.static_path

        # pass in metrique and mongodb config to all handlers (init)
        init = dict(metrique_config=self.mconf,
                    mongodb_config=self.dbconf,
                    logger=self.logger)

        handlers = [(h[0], h[1], init) for h in self._api_v2_handlers]

        self._web_app = Application(
            gzip=gzip,
            debug=debug,
            static_path=static_path,
            handlers=handlers,
            cookie_secret=self.mconf.cookie_secret,
            login_url=login_url,
            xsrf_cookies=self.mconf.xsrf_cookies,
        )

        if debug and not self.mconf.autoreload:
            # FIXME hack to disable autoreload when debug is True
            from tornado import autoreload
            autoreload._reload_attempted = True
            autoreload._reload = lambda: None

        ssl = self.mconf.ssl
        if ssl:
            ssl_options = dict(
                certfile=os.path.expanduser(self.mconf.ssl_certificate),
                keyfile=os.path.expanduser(self.mconf.ssl_certificate_key))
            try:
                self.server = HTTPServer(self._web_app,
                                         ssl_options=ssl_options)
            except ValueError:
                raise ValueError(
                    "SSL ERROR (%s); "
                    "try running `metriqued-setup`" % ssl_options)
        else:
            self.server = HTTPServer(self._web_app)

    def _init_basic_server(self):
        host = self.mconf.host
        port = self.mconf.port
        self.server.listen(port=port, address=host)
        IOLoop.instance().start()

    def spawn_instance(self):
        self.logger.debug("spawning tornado %s..." % self.uri)
        self.set_pid()
        self._mongodb_check()
        self._init_basic_server()

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
            if self.mconf.debug:
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

    def _mongodb_check(self):
        # Fail to start if we can't communicate with mongo
        host = self.dbconf.host
        self.logger.debug('testing mongodb connection status (%s) ...' % host)
        try:
            assert self.dbconf.db_metrique_admin.db
            assert self.dbconf.db_timeline_admin.db
            assert self.dbconf.db_metrique_data.db
            assert self.dbconf.db_timeline_data.db
            self.logger.debug('... mongodb connection ok')
        except Exception:
            self.logger.error(
                'failed to communicate with mongodb')
            raise
