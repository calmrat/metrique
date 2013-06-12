#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import os
import re

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
    def api_ssl(self):
        if re.match('https://', self.api_host):
            return True
        else:
            return False

    @property
    def api_version(self):
        return self._default('api_version', API_VERSION)

    @property
    def api_rel_path(self):
        def_rel_path = os.path.join(API_REL_PATH, self.api_version)
        return self._default('api_rel_path', def_rel_path)

    @property
    def api_url(self):
        if not re.match('http', self.api_host):
            host = '%s%s' % ('http://', self.api_host)
        else:
            host = self.api_host

        if not re.match('https?://', host):
            raise ValueError("Invalid schema (%s). "
                             "Expected: %s" % (host, 'http(s)?'))
        host_port = '%s:%s' % (host, self.api_port)
        api_url = os.path.join(host_port, self.api_rel_path)
        return api_url

    @property
    def api_port(self):
        return self._default('api_port', METRIQUE_HTTP_PORT)

    @api_port.setter
    def api_port(self, value):
        if not isinstance(value, basestring):
            raise TypeError("api_port must be string")
        self._config['api_port'] = value

    @property
    def api_host(self):
        return self._default('api_host', METRIQUE_HTTP_HOST)

    @api_host.setter
    def api_host(self, value):
        if not isinstance(value, basestring):
            raise TypeError("api_host must be string")
        self._config['api_host'] = value

    @property
    def api_username(self):
        return self._default('api_username')

    @api_username.setter
    def api_username(self, value):
        self._config['api_username'] = value

    @property
    def api_password(self):
        return self._default('api_password')

    @api_password.setter
    def api_password(self, value):
        self._config['api_password'] = value

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
