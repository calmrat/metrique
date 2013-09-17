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
from metriqued.config import DEFAULT_METRIQUE_CONF
from metriqued.config import DEFAULT_MONGODB_CONF
from metriqued import core_api, cube_api, query_api, user_api

from metriqueu.utils import set_default


def user_cube(value):
    user_cube = r'(\w+)/(\w+)'
    value = str(value)
    path = os.path.join(user_cube, value)
    return path


def api_v2(value):
    return os.path.join(r'/api/v2', value)


def api_v1(value):
    return os.path.join(r'/api/v1', value)


def ucv2(value):
    uc = user_cube(value)
    a2 = api_v2(uc)
    return a2


class TornadoHTTPServer(object):
    ''' HTTP (Tornado >=3.0) implemntation of MetriqueServer '''
    def __init__(self, metrique_config_file=None,
                 mongodb_config_file=None, host=None, port=None,
                 ssl=None, auth=False, async=True, debug=None,
                 pid_file=None, **kwargs):
        if not metrique_config_file:
            metrique_config_file = DEFAULT_METRIQUE_CONF
        if not mongodb_config_file:
            mongodb_config_file = DEFAULT_MONGODB_CONF

        self._metrique_config_file = metrique_config_file
        self.metrique_config = mconf = metrique(metrique_config_file)

        self._mongodb_config_file = metrique_config_file
        self.mongodb_config = mbconf = mongodb(mongodb_config_file)

        mconf.debug = debug = set_default(debug, mconf.debug)
        mconf.async = async = set_default(async, mconf.async)
        mconf.host = host = set_default(host, mconf.http_host)
        mconf.port = port = set_default(port, mconf.http_port)
        mconf.ssl = ssl = set_default(ssl, mconf.ssl, True)
        mconf.auth = auth = set_default(auth, mconf.auth, True)

        self._pid_file = pid_file = set_default(pid_file, mconf.pid_file)

        self._child_pids = []

        self._prepare_handlers()

        self.address = 'https://%s' % host if ssl else 'http://%s' % host

        logger.debug('======= metrique =======')
        logger.debug(' Host: %s' % self.address)
        logger.debug('  SSL: %s' % ssl)
        logger.debug(' Port: %s' % port)
        logger.debug('Async: %s' % async)
        logger.debug(' Auth: %s' % auth)
        logger.debug('======= mongodb ========')
        logger.debug(' Host: %s' % mbconf.host)
        logger.debug(' Port: %s' % mbconf.port)

    @property
    def pid(self):
        return os.getpid()

    @property
    def pid_file(self):
        return self._pid_file

    def _set_pid(self, child=False):
        if os.path.exists(self.pid_file):
            raise RuntimeError(
                "pid (%s) found in (%s)" % (self.pid,
                                            self.metrique_config.pid_file))
        with open(self.pid_file, 'w') as file:
            file.write(str(self.pid))
        logger.debug("PID stored (%s)" % self.pid)
        return self.pid

    def _remove_pid(self):
        try:
            os.remove(self.pid_file)
        except OSError as e:
            logger.error(
                'pid file not removed (%s); %e' % (self.pid_file, e))

    def _prepare_handlers(self):
        mongodb = self.mongodb_config.db_timeline_data.db
        init_timeline_db = {'mongodb': mongodb}
        base_handlers = [
            (r"/register", user_api.RegisterHdlr),
            (r"/login", user_api.LoginHdlr),
            (r"/logout", user_api.LogoutHdlr),

            (r"/(\w+)/aboutme", user_api.AboutMeHdlr),
            (r"/(\w+)/passwd", user_api.UpdatePasswordHdlr),
            (r"/(\w+)/update_profile", user_api.UpdateProfileHdlr),
            (r"/(\w+)/update_properties", user_api.UpdatePropertiesHdlr),

            (api_v1(r""), core_api.ObsoleteAPIHdlr),
            (api_v2(r"ping"), core_api.PingHdlr),

            (api_v2(r"(\w+)?/?(\w+)?"), cube_api.ListHdlr, init_timeline_db),
        ]

        user_cube_handlers = [
            (ucv2(r"find"), query_api.FindHdlr),
            (ucv2(r"deptree"), query_api.DeptreeHdlr),
            (ucv2(r"count"), query_api.CountHdlr),
            (ucv2(r"aggregate"), query_api.AggregateHdlr),
            (ucv2(r"fetch"), query_api.FetchHdlr),
            (ucv2(r"distinct"), query_api.DistinctHdlr),
            (ucv2(r"sample"), query_api.SampleHdlr),

            (ucv2(r"index"), cube_api.IndexHdlr),
            (ucv2(r"save_objects"), cube_api.SaveObjectsHdlr),
            (ucv2(r"remove_objects"), cube_api.RemoveObjectsHdlr),
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

        # pass in metrique and mongodb config
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

        if debug:
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

    def spawn_instance(self):
        logger.debug("spawning new tornado webapp instance...")
        self._parent_pid = ppid = os.getpid()
        pid = os.fork()
        if pid == 0:
            real_pid = self._set_pid()
            logger.debug(" ... child (%s)" % real_pid)
            signal.signal(signal.SIGUSR1, self._inst_start_handler)
            signal.signal(signal.SIGTERM, self._inst_terminate_handler)
            signal.signal(signal.SIGINT, self._inst_kill_handler)
            self._pid = real_pid
            signal.pause()
        else:
            time.sleep(.5)  # give the child a bit more time to startup
            child_pid = pid
            logger.debug(
                " ... parent (%s); child (%s)" % (ppid, child_pid))
            os.kill(child_pid, signal.SIGUSR1)

    def start(self):
        ''' Start a new tornado web app '''
        self._mongodb_check()
        self.spawn_instance()

    def stop(self, pid=None, sig=signal.SIGTERM):
        ''' Stop a running tornado web app '''
        os.kill(self.pid, signal.SIGTERM)
        self._remove_pid()
        logger.debug("removed PID file")

    def _inst_start_handler(self, sig, fram):
        host = self.metrique_config.host
        port = self.metrique_config.port
        self._prepare_web_app()
        logger.debug("tornado listening on %s:%s" % (self.address, port))
        self.server.listen(port=port, address=host)
        self.ioloop = IOLoop.instance()
        self.ioloop.start()

        # FIXME: try... if it fails, bump port up by one
        #proc_k = self.metrique_config.max_processes
        #self.server.bind(port=port, address=host)
        #self.server.start(proc_k)  # fork some sub-processes
        #self.ioloop = IOLoop.instance()
        #self.ioloop.start()

    def _inst_terminate_handler(self, sig, frame):
        logger.debug("[INST] (%s) recieved TERM signal" % self.pid)
        self.ioloop.add_callback(self._inst_delayed_stop)
        self.server.stop()  # stop this tornado instance

    def _inst_kill_handler(self, sig, frame):
        logger.debug("[INST] (%s) recieved KILL signal" % self.pid)
        kill_now = partial(self._inst_delayed_stop, 0)
        self.ioloop.add_callback(kill_now)
        self.server.stop()  # stop this tornado instance

    def _inst_delayed_stop(self, delay=None):
        if delay is None:
            if self.metrique_config.debug:
                delay = 0
            else:
                delay = 5

        logger.debug("stop ioloop called (%s)... " % self.pid)
        TIMEOUT = float(delay) + time.time()
        logger.debug("Shutting down in T-%i seconds ..." % delay)
        self.ioloop.add_timeout(TIMEOUT, self._stop_ioloop)

    def _stop_ioloop(self):
        self.ioloop.stop()
        logger.debug("IOLoop stopped")

    def _mongodb_check(self):
        # Fail to start if we can't communicate with mongo
        logger.warn('testing mongodb connection status...')
        try:
            assert self.mongodb_config.db_metrique_admin.db
        except Exception as e:
            host = self.mongodb_config.host
            raise RuntimeError(
                '%s\nFailed to communicate with MongoDB (%s)' % (e, host))
