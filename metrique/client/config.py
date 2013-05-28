#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import os

from metrique.tools.jsonconfig import JSONConfig
from metrique.tools.defaults import METRIQUE_HTTP_PORT, METRIQUE_HTTP_HOST

API_VERSION = 'v1'
API_REL_PATH = 'api'
API_SSL = False


class Config(JSONConfig):
    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)
        self.debug = None

    @property
    def metrique_api_ssl(self):
        return self._default('metrique_api_ssl', API_SSL)

    @property
    def metrique_api_version(self):
        return self._default('metrique_api_version', API_VERSION)

    @property
    def metrique_api_rel_path(self):
        def_rel_path = os.path.join(API_REL_PATH, self.metrique_api_version)
        return self._default('metrique_api_rel_path', def_rel_path)

    @property
    def metrique_api_url(self):
        if self.metrique_api_ssl:
            proto = 'https://'
        else:
            proto = 'http://'
        host_port = '%s:%s' % (self.metrique_http_host, self.metrique_http_port)
        api_url = os.path.join(proto, host_port, self.metrique_api_rel_path)
        return api_url

    @property
    def metrique_http_port(self):
        return self._default('metrique_http_port', METRIQUE_HTTP_PORT)

    @metrique_http_port.setter
    def metrique_http_port(self, value):
        if not isinstance(value, basestring):
            raise TypeError("metrique_http_port must be string")
        self._config['metrique_http_port'] = value

    @property
    def metrique_http_host(self):
        return self._default('metrique_http_host', METRIQUE_HTTP_HOST)

    @metrique_http_host.setter
    def metrique_http_host(self, value):
        if not isinstance(value, basestring):
            raise TypeError("metrique_http_host must be string")
        self._config['metrique_http_host'] = value


    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, bool_=None):
        if bool_ is False:
            logger.setLevel(logging.WARN)
        elif bool_ is True:
            logger.setLevel(logging.DEBUG)
            logger.debug('DEBUG: ON')
        else:
            logging.disable(logging.INFO)
