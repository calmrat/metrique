#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import os
import tornado.ioloop
import tornado.web

from metrique.server.metriqueserver import MetriqueServer

from handlers import PingHandler
from handlers import JobStatusHandler
from handlers import QueryAggregateHandler, QueryFindHandler
from handlers import QueryFetchHandler, QueryCountHandler
from handlers import UsersAddHandler
from handlers import LogTailHandler
from handlers import ETLIndexWarehouseHandler
from handlers import ETLExtractHandler, ETLSnapshotHandler, CubesHandler
from handlers import ETLActivityImportHandler
from handlers import ETLSaveObject


class HTTPServer(MetriqueServer):
    '''
    '''
    def __init__(self, host=None, port=None, **kwargs):
        super(HTTPServer, self).__init__(**kwargs)
        if host:
            self.metrique_config.http_host = host
        if port:
            self.metrique_config.http_port = port

    def _setup_webapp(self):
        logger.debug("Tornado: Web App setup")
        init = dict(proxy=self)
        self._web_app = tornado.web.Application([
            (r"/api/v1/ping/?", PingHandler, init),
            (r"/api/v1/job/status/(\w+)", JobStatusHandler, init),
            (r"/api/v1/query/find", QueryFindHandler, init),
            (r"/api/v1/query/count", QueryCountHandler, init),
            (r"/api/v1/query/aggregate", QueryAggregateHandler, init),
            (r"/api/v1/query/fetch", QueryFetchHandler, init),
            (r"/api/v1/admin/users/add", UsersAddHandler, init),
            (r"/api/v1/admin/log/tail", LogTailHandler, init),
            (r"/api/v1/admin/etl/extract", ETLExtractHandler, init),
            (r"/api/v1/admin/etl/index/warehouse", ETLIndexWarehouseHandler, init),
            (r"/api/v1/admin/etl/snapshot", ETLSnapshotHandler, init),
            (r"/api/v1/admin/etl/activityimport",
             ETLActivityImportHandler, init),
            (r"/api/v1/admin/etl/saveobject", ETLSaveObject, init),
            (r"/api/v1/cubes", CubesHandler, init),
        ], gzip=True)
        # FIXME: set gzip as metrique_config property, default True
        port = self.metrique_config.http_port
        address = self.metrique_config.http_host
        if self.metrique_config.ssl:
            ssl_options = dict(certfile=self.metrique_config.ssl_certificate,
                               keyfile=self.metrique_config.ssl_certificate_key)
            try:
                self._web_app.listen(port=port, address=address,
                                     ssl_options=ssl_options)
            except ValueError:
                raise ValueError("SSL Cert missing, perhaps? (%s)." % ssl_options)
        else:
            self._web_app.listen(port=port, address=address)

        logger.debug("Tornado: listening on %s:%s" % (port, address))

    def start(self):
        pid = os.fork()
        if pid == 0:
            logger.debug("Tornado: Start")
            super(HTTPServer, self).start()
            self._setup_webapp()
            ioloop = tornado.ioloop.IOLoop.instance()
            ioloop.start()

    def stop(self):
        # FIXME: THIS ISN"T WORKING! should catch sigkill/sigterm and shutdown properly
        super(HTTPServer, self).stop()
        logger.debug("Tornado: Stop")
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.stop()
        if hasattr(self, '_web_app'):
            del self._web_app
