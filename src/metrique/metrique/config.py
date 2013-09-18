#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
config.py contains the main configuration object for
metrique client applications, which includes built-in
defaults.

Defaults assume local, insecure 'test', 'development'
or 'personal' environment.

To customize local client configuration, add/update
`~/.metrique/http_api.json` (default).

Paths are UNIX compatible only.
'''

import logging
import os
import re

from jsonconf import JSONConf

from metriqueu.defaults import DEFAULT_CONFIG_DIR
from metriqueu.defaults import DEFAULT_METRIQUE_HTTP_HOST
from metriqueu.defaults import DEFAULT_METRIQUE_HTTP_PORT
from metriqueu.defaults import DEFAULT_CLIENT_CUBES_PATH
from metriqueu.defaults import DEFAULT_API_REL_PATH

DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_DIR, 'http_api')
DEFAULT_BATCH_SIZE = -1

API_VERSION = 'v2'


class Config(JSONConf):
    ''' Client config (property) class '''
    def __init__(self, config_file, force=True, *args, **kwargs):
        config_file = config_file or DEFAULT_CONFIG_FILE
        super(Config, self).__init__(config_file=config_file, force=force,
                                     *args, **kwargs)

    @property
    def api_auto_login(self):
        ''' Current api version in use '''
        return self._default('api_auto_login', True)

    @api_auto_login.setter
    def api_auto_login(self, value):
        ''' Set and save in config a metrique api port to use '''
        self.config['api_port'] = value

    @property
    def api_host(self):
        ''' The hostname/url/ip to connect to metrique api '''
        return self._default('api_host', DEFAULT_METRIQUE_HTTP_HOST)

    @api_host.setter
    def api_host(self, value):
        ''' Set and save in config a metrique api host to use

            If you're connection to a metrique server with
            auth = True, it's highly likely it's ssl = True
            too, so be sure to add https:// to the api_host!
        '''
        if not isinstance(value, basestring):
            raise TypeError("api_host must be string")
        self.config['api_host'] = value

    @property
    def api_port(self):
        ''' Port need to connect to metrique api '''
        return self._default('api_port', DEFAULT_METRIQUE_HTTP_PORT)

    @api_port.setter
    def api_port(self, value):
        ''' Set and save in config a metrique api port to use '''
        if not isinstance(value, basestring):
            raise TypeError("api_port must be string")
        self.config['api_port'] = value

    @property
    def api_password(self):
        ''' The password to connect to metrique api with (OPTIONAL)'''
        return self._default('api_password')

    @api_password.setter
    def api_password(self, value):
        ''' Set and save the password to connect to metrique api with '''
        self.config['api_password'] = value

    @property
    def api_rel_path(self):
        ''' Reletive paths from url.root needed
            to trigger/access the metrique http api
        '''
        def_rel_path = os.path.join(DEFAULT_API_REL_PATH, self.api_version)
        return self._default('api_rel_path', def_rel_path)

    @property
    def api_ssl(self):
        ''' Determine if ssl schema used in a given host string'''
        return bool(re.match('https://', self.api_host))

    @property
    def api_ssl_verify(self):
        ''' Current api version in use '''
        return self._default('api_ssl_verify', True)

    @api_ssl_verify.setter
    def api_ssl_verify(self, value):
        ''' Set and save in config a metrique api port to use '''
        self.config['api_ssl_verify'] = value

    @property
    def api_url(self):
        ''' Url and schema - http(s)? needed to call metrique api

            If you're connection to a metrique server with
            auth = True, it's highly likely it's ssl = True
            too, so be sure to add https:// to the api_host!
        '''
        return os.path.join(self.host_port, self.api_rel_path)

    @property
    def api_username(self):
        ''' The username to connect to metrique api with (OPTIONAL)'''
        return self._default('api_username', os.getenv('USER'))

    @api_username.setter
    def api_username(self, value):
        ''' Set and save the username to connect to metrique api with '''
        self.config['api_username'] = value

    @property
    def api_version(self):
        ''' Current api version in use '''
        return self._default('api_version', API_VERSION)

    @property
    def async(self):
        ''' Turn on/off async (parallel) multiprocessing (where supported) '''
        return self._default('async', True)

    @async.setter
    def async(self, value):
        self.config['async'] = value

    @property
    def batch_size(self):
        ''' The number of objs to push save_objects at a time'''
        return self._default('batch_size', DEFAULT_BATCH_SIZE)

    @batch_size.setter
    def batch_size(self, value):
        self.config['batch_size'] = value

    @property
    def cubes_path(self):
        ''' Path to client modules '''
        return self._default('cubes_path', DEFAULT_CLIENT_CUBES_PATH)

    @property
    def debug(self):
        ''' Reflect whether debug is enabled or not '''
        return self._default('debug', -1)

    @debug.setter
    def debug(self, value):
        ''' Update logger settings '''
        if isinstance(value, (tuple, list)):
            logger, value = value
            self._set_debug(value, logger)
        else:
            try:
                logger = self.logger
            except AttributeError:
                self._set_debug(value)
            else:
                self._set_debug(value, logger)
        self.config['debug'] = value

    @property
    def host_port(self):
        ''' Url and schema - http(s)? needed to call metrique api

            If you're connection to a metrique server with
            auth = True, it's highly likely it's ssl = True
            too, so be sure to add https:// to the api_host!
        '''
        if not re.match('http', self.api_host):
            host = '%s%s' % ('http://', self.api_host)
        else:
            host = self.api_host

        if not re.match('https?://', host):
            raise ValueError("Invalid schema (%s). "
                             "Expected: %s" % (host, 'http(s)?'))
        host_port = '%s:%s' % (host, self.api_port)
        return host_port

    def _set_debug(self, level, logger=None):
        '''
        if we get a level of 2, we want to apply the
        debug level to all loggers
        '''
        logger = logger or logging.getLogger()

        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)
            rlogger = logging.getLogger()
            rlogger.setLevel(logging.DEBUG)

    @property
    def sql_delta_batch_size(self):
        ''' The number of objects to query at a time in sql.id_delta '''
        return self._default('sql_delta_batch_size', 1000)

    @property
    def sql_delta_batch_retries(self):
        return self._default('sql_delta_batch_retries', 3)
