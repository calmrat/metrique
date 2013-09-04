#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import os
import tornado.ioloop
import tornado.web

from metriqued.metriqueserver import MetriqueServer

from handlers import PingHandler
from handlers import QueryAggregateHandler, QueryFindHandler
from handlers import QueryDeptreeHandler
from handlers import QueryFetchHandler, QueryCountHandler
from handlers import QueryDistinctHandler, QuerySampleHandler
from handlers import UsersAddHandler
from handlers import ETLIndexHandler
from handlers import ETLActivityImportHandler
from handlers import ETLSaveObjects, ETLRemoveObjects, ETLCubeDrop
from handlers import CubeHandler


class HTTPServer(MetriqueServer):
    ''' HTTP (Tornado >=3.0) implemntation of MetriqueServer '''
    def __init__(self, host=None, port=None, **kwargs):
        super(HTTPServer, self).__init__(**kwargs)
        if host:
            self.metrique_config.http_host = host
        if port:
            self.metrique_config.http_port = port

    def _setup_webapp(self):
        ''' Config and Views'''
        logger.debug("Tornado: Web App setup")
        init = dict(proxy=self)
        debug = self.metrique_config.debug == 2
        gzip = self.metrique_config.gzip
        static_path = self.metrique_config.static_path
        self._web_app = tornado.web.Application(
            gzip=gzip,
            debug=debug,
            static_path=static_path,
            handlers=[
                (r"/api/v1/ping/?", PingHandler, init),
                (r"/api/v1/query/find", QueryFindHandler, init),
                (r"/api/v1/query/deptree", QueryDeptreeHandler, init),
                (r"/api/v1/query/count", QueryCountHandler, init),
                (r"/api/v1/query/aggregate", QueryAggregateHandler, init),
                (r"/api/v1/query/fetch", QueryFetchHandler, init),
                (r"/api/v1/query/distinct", QueryDistinctHandler, init),
                (r"/api/v1/query/sample", QuerySampleHandler, init),
                (r"/api/v1/admin/users/add", UsersAddHandler, init),
                (r"/api/v1/admin/etl/index", ETLIndexHandler, init),
                (r"/api/v1/admin/etl/activityimport",
                    ETLActivityImportHandler, init),
                (r"/api/v1/admin/etl/saveobjects", ETLSaveObjects, init),
                (r"/api/v1/admin/etl/removeobjects", ETLRemoveObjects, init),
                (r"/api/v1/admin/etl/cube/drop", ETLCubeDrop, init),
                (r"/api/v1/cube", CubeHandler, init),
            ],
        )
        if debug:
            # FIXME hack to disable autoreload when debug is True
            from tornado import autoreload
            autoreload._reload_attempted = True
            autoreload._reload = lambda: None
        port = self.metrique_config.http_port
        address = self.metrique_config.http_host
        if self.metrique_config.ssl:
            ssl_options = dict(
                certfile=self.metrique_config.ssl_certificate,
                keyfile=self.metrique_config.ssl_certificate_key)
            try:
                self._web_app.listen(port=port, address=address,
                                     ssl_options=ssl_options)
            except ValueError:
                raise ValueError(
                    "SSL Cert missing, perhaps? (%s)." % ssl_options)
        else:
            self._web_app.listen(port=port, address=address)

        logger.debug("Tornado: listening on %s:%s" % (port, address))

    def start(self):
        ''' Start a new tornado web app '''
        pid = os.fork()
        if pid == 0:
            logger.debug("Tornado: Start")
            super(HTTPServer, self).start()
            self._setup_webapp()
            ioloop = tornado.ioloop.IOLoop.instance()
            ioloop.start()

    def stop(self):
        ''' Stop a run tornado web app '''
        # FIXME: THIS ISN"T WORKING!
        # should catch sigkill/sigterm and shutdown properly
        super(HTTPServer, self).stop()
        logger.debug("Tornado: Stop")
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.stop()
        if hasattr(self, '_web_app'):
            del self._web_app
