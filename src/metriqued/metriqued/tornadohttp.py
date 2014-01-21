#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metriquet.tornadohttp import TornadoHTTPServer

from metriqued.config import metriqued_config, mongodb_config
from metriqued import core_api, cube_api, query_api, user_api

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


class MetriqueHTTP(TornadoHTTPServer):
    ''' HTTP (Tornado >=3.0) implemntation of MetriqueServer '''
    def __init__(self, config_file=None, **kwargs):
        super(MetriqueHTTP, self).__init__(**kwargs)

        config_file = config_file or METRIQUED_JSON

        self.conf = metriqued_config(config_file=config_file,
                                     defaults=self.conf)

        # make sure kwargs passed in are set
        for k, v in kwargs.items():
            self.conf[k] = v

        self.dbconf = mongodb_config(self.conf.mongodb_config)

        self.handlers = self._prepare_handlers()

        # call setup_logger() once more to ensure we've adjusted
        # for local config changes
        self.setup_logger()

        self.logger.debug('======= metrique =======')
        self.logger.debug(' Conf: %s' % self.conf.config_file)
        self.logger.debug(' Host: %s' % self.uri)
        self.logger.debug('  SSL: %s' % self.conf.ssl)
        self.logger.debug('======= mongodb ========')
        self.logger.debug(' Conf: %s' % self.dbconf.config_file)
        self.logger.debug(' Host: %s' % self.dbconf.host)
        self.logger.debug('  SSL: %s' % self.dbconf.ssl)
        self.logger.debug(' Port: %s' % self.dbconf.port)

        self._mongodb_check()

    def _prepare_handlers(self):
        # pass in metrique and mongodb config to all handlers (init)
        init = dict(metrique_config=self.conf,
                    mongodb_config=self.dbconf,
                    logger=self.logger)

        base_handlers = [
            (r"/register", user_api.RegisterHdlr, init),
            (r"/login", user_api.LoginHdlr, init),
            (r"/logout", user_api.LogoutHdlr, init),

            (r"/(\w+)/aboutme", user_api.AboutMeHdlr, init),
            (r"/(\w+)/passwd", user_api.UpdatePasswordHdlr, init),
            (r"/(\w+)/remove", user_api.RemoveHdlr, init),
            (r"/(\w+)/update_profile", user_api.UpdateProfileHdlr, init),
            (r"/(\w+)/update_group", user_api.UpdateGroupHdlr, init),
            (r"/(\w+)/update_properties", user_api.UpdatePropertiesHdlr, init),

            (api_v1(r""), core_api.ObsoleteAPIHdlr, init),
            (api_v2(r"ping"), core_api.PingHdlr, init),

            (api_v2(r"(\w+)?/?([-\w]+)?"), cube_api.ListHdlr, init),
        ]

        user_cube_handlers = [
            (ucv2(r"find"), query_api.FindHdlr, init),
            (ucv2(r"history"), query_api.HistoryHdlr, init),
            (ucv2(r"deptree"), query_api.DeptreeHdlr, init),
            (ucv2(r"count"), query_api.CountHdlr, init),
            (ucv2(r"aggregate"), query_api.AggregateHdlr, init),
            (ucv2(r"distinct"), query_api.DistinctHdlr, init),
            (ucv2(r"sample"), query_api.SampleHdlr, init),

            (ucv2(r"index"), cube_api.IndexHdlr, init),
            (ucv2(r"save"), cube_api.SaveObjectsHdlr, init),
            (ucv2(r"rename"), cube_api.RenameHdlr, init),
            (ucv2(r"remove"), cube_api.RemoveObjectsHdlr, init),
            (ucv2(r"export"), cube_api.ExportHdlr, init),
            (ucv2(r"update_role"), cube_api.UpdateRoleHdlr, init),
            (ucv2(r"drop"), cube_api.DropHdlr, init),
            (ucv2(r"stats"), cube_api.StatsHdlr, init),
            (ucv2(r"register"), cube_api.RegisterHdlr, init),
        ]

        return base_handlers + user_cube_handlers

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
