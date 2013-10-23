#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)

from functools import partial
import os
import signal
import time
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import Application

from metriqued.config import metrique, mongodb
from metriqued import core_api, cube_api, query_api, user_api

from metriqueu.utils import set_default


def user_cube(value):
    user_cube = r'(\w+)/(\w+)'
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
    def __init__(self, metrique_config_file=None,
                 mongodb_config_file=None, host=None, port=None,
                 ssl=None, async=None, debug=None,
                 pid_file=None, **kwargs):
        self.metrique_config = mconf = metrique(metrique_config_file)
        self.mongodb_config = mbconf = mongodb(mongodb_config_file)

        mconf.debug = debug = set_default(debug, mconf.debug)
        mconf.async = async = set_default(async, mconf.async)
        mconf.ssl = ssl = set_default(ssl, mconf.ssl)
        mconf.host = host = set_default(host, mconf.host)
        mconf.port = port = set_default(port, mconf.port)

        self._pid_file = pid_file = set_default(pid_file, mconf.pid_file)
        self.parent_pid = None
        self.child_pid = None

        self._prepare_handlers()

        self.uri = 'https://%s' % host if ssl else 'http://%s' % host
        self.uri += ':%s' % port

        logger.debug('======= metrique =======')
        logger.debug(' Host: %s' % self.uri)
        logger.debug('  SSL: %s' % ssl)
        logger.debug('Async: %s' % async)
        logger.debug('======= mongodb ========')
        logger.debug(' Host: %s' % mbconf.host)
        logger.debug('  SSL: %s' % mbconf.ssl)
        logger.debug(' Port: %s' % mbconf.port)

    @property
    def pid(self):
        return os.getpid()

    @property
    def pid_file(self):
        return self._pid_file

    def set_pid(self):
        if os.path.exists(self.pid_file):
            raise RuntimeError(
                "pid (%s) found in (%s)" % (self.pid,
                                            self.metrique_config.pid_file))
        else:
            with open(self.pid_file, 'w') as _file:
                _file.write(str(self.pid))
        logger.debug("PID stored (%s)" % self.pid)

    def _remove_pid(self):
        error = None
        try:
            os.remove(self.pid_file)
        except OSError as error:
            logger.error(
                'pid file not removed (%s); %s' % (self.pid_file, error))
        else:
            logger.debug("removed PID file: %s" % self.pid_file)

    def _prepare_handlers(self):
        base_handlers = [
            (r"/register", user_api.RegisterHdlr),
            (r"/login", user_api.LoginHdlr),
            (r"/logout", user_api.LogoutHdlr),

            (r"/(\w+)/aboutme", user_api.AboutMeHdlr),
            (r"/(\w+)/passwd", user_api.UpdatePasswordHdlr),
            (r"/(\w+)/update_profile", user_api.UpdateProfileHdlr),
            (r"/(\w+)/update_group", user_api.UpdateGroupHdlr),
            (r"/(\w+)/update_properties", user_api.UpdatePropertiesHdlr),

            (api_v1(r""), core_api.ObsoleteAPIHdlr),
            (api_v2(r"ping"), core_api.PingHdlr),

            (api_v2(r"(\w+)?/?(\w+)?"), cube_api.ListHdlr),
        ]

        user_cube_handlers = [
            (ucv2(r"find"), query_api.FindHdlr),
            (ucv2(r"history"), query_api.HistoryHdlr),
            (ucv2(r"deptree"), query_api.DeptreeHdlr),
            (ucv2(r"count"), query_api.CountHdlr),
            (ucv2(r"aggregate"), query_api.AggregateHdlr),
            (ucv2(r"fetch"), query_api.FetchHdlr),
            (ucv2(r"distinct"), query_api.DistinctHdlr),
            (ucv2(r"sample"), query_api.SampleHdlr),

            (ucv2(r"index"), cube_api.IndexHdlr),
            (ucv2(r"save"), cube_api.SaveObjectsHdlr),
            (ucv2(r"remove"), cube_api.RemoveObjectsHdlr),
            (ucv2(r"update_role"), cube_api.UpdateRoleHdlr),
            (ucv2(r"drop"), cube_api.DropHdlr),
            (ucv2(r"stats"), cube_api.StatsHdlr),
            (ucv2(r"register"), cube_api.RegisterHdlr),
        ]

        self._api_v2_handlers = base_handlers + user_cube_handlers

    def _prepare_web_app(self):
        ''' Config and Views'''
        logger.debug("tornado web app setup")
        debug = self.metrique_config.debug == 2
        gzip = self.metrique_config.gzip
        login_url = self.metrique_config.login_url
        static_path = self.metrique_config.static_path

        # pass in metrique and mongodb config to all handlers (init)
        init = dict(metrique_config=self.metrique_config,
                    mongodb_config=self.mongodb_config)

        handlers = [(h[0], h[1], init) for h in self._api_v2_handlers]

        self._web_app = Application(
            gzip=gzip,
            debug=debug,
            static_path=static_path,
            handlers=handlers,
            cookie_secret=self.metrique_config.cookie_secret,
            login_url=login_url,
            xsrf_cookies=self.metrique_config.xsrf_cookies,
        )

        if debug and not self.metrique_config.autoreload:
            # FIXME hack to disable autoreload when debug is True
            from tornado import autoreload
            autoreload._reload_attempted = True
            autoreload._reload = lambda: None

        ssl = self.metrique_config.ssl
        if ssl:
            ssl_options = dict(
                certfile=self.metrique_config.ssl_certificate,
                keyfile=self.metrique_config.ssl_certificate_key)
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
        host = self.metrique_config.host
        port = self.metrique_config.port
        self.server.listen(port=port, address=host)
        IOLoop.instance().start()

    def spawn_instance(self):
        logger.debug("spawning new tornado webapp instance...")

        self._mongodb_check()

        self.set_pid()

        signal.signal(signal.SIGTERM, self._inst_terminate_handler)
        signal.signal(signal.SIGINT, self._inst_kill_handler)

        self._init_basic_server()

    def start(self, fork=False):
        ''' Start a new tornado web app '''
        self._prepare_web_app()
        if fork:
            pid = os.fork()
            self.parent_pid = self.pid
            if pid == 0:
                self.spawn_instance()
            else:
                time.sleep(0.5)  # give child a moment to start
                self.child_pid = pid
        else:
            pid = self.pid
            self.spawn_instance()
        logger.debug("tornado listening on %s" % self.uri)
        return pid

    def stop(self, delay=None):
        ''' Stop a running tornado web app '''
        if self.child_pid:
            os.kill(self.child_pid, signal.SIGTERM)
            self._remove_pid()
        else:
            self.server.stop()  # stop this tornado instance
            delayed_kill = partial(self._inst_delayed_stop, delay)
            IOLoop.instance().add_callback(delayed_kill)

    def _inst_stop(self, sig, delay=None):
        if self.child_pid:
            os.kill(self.child_pid, sig)
        else:
            self.stop(delay=delay)

    def _inst_terminate_handler(self, sig, frame):
        logger.debug("[INST] (%s) recieved TERM signal" % self.pid)
        self._inst_stop(sig)

    def _inst_kill_handler(self, sig, frame):
        logger.debug("[INST] (%s) recieved KILL signal" % self.pid)
        self._inst_stop(sig, 0)

    def _inst_delayed_stop(self, delay=None):
        if delay is None:
            if self.metrique_config.debug:
                delay = 0
            else:
                delay = 5
        logger.debug("stop ioloop called (%s)... " % self.pid)
        TIMEOUT = float(delay) + time.time()
        logger.debug("Shutting down in T-%i seconds ..." % delay)
        IOLoop.instance().add_timeout(TIMEOUT, self._stop_ioloop)

    def _stop_ioloop(self):
        IOLoop.instance().stop()
        logger.debug("IOLoop stopped")

    def _mongodb_check(self):
        # Fail to start if we can't communicate with mongo
        host = self.mongodb_config.host
        logger.debug('testing mongodb connection status (%s) ...' % host)
        try:
            assert self.mongodb_config.db_metrique_admin.db
            assert self.mongodb_config.db_metrique_data.db
            assert self.mongodb_config.db_timeline_admin.db
            assert self.mongodb_config.db_timeline_data.db
            logger.debug('... mongodb connection ok')
        except Exception:
            logger.error(
                'failed to communicate with mongodb')
            raise
