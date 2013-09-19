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

from metriqueu.defaults import CONFIG_DIR, CLIENT_CUBES_PATH, API_REL_PATH
from metriqueu.defaults import METRIQUE_HTTP_HOST, METRIQUE_HTTP_PORT

CONFIG_FILE = os.path.join(CONFIG_DIR, 'http_api')
BATCH_SIZE = -1

API_VERSION = 'v2'


class Config(JSONConf):
    ''' Client config (property) class '''
    def __init__(self, config_file, force=True, *args, **kwargs):
        config_file = config_file or CONFIG_FILE
        super(Config, self).__init__(config_file=config_file, force=force,
                                     *args, **kwargs)

    @property
    def api_rel_path(self):
        ''' Reletive paths from url.root needed
            to trigger/access the metrique http api
        '''
        def_rel_path = os.path.join(API_REL_PATH, self.api_version)
        return self._default('api_rel_path', def_rel_path)

    @property
    def api_url(self):
        ''' Url and schema - http(s)? needed to call metrique api

            If you're connection to a metrique server with
            auth = True, it's highly likely it's ssl = True
            too, so be sure to add https:// to the host!
        '''
        return os.path.join(self.host_port, self.api_rel_path)

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
    def auto_login(self):
        ''' Current api version in use '''
        return self._default('auto_login', True)

    @auto_login.setter
    def auto_login(self, value):
        ''' Set and save in config a metrique api port to use '''
        self.config['port'] = value

    @property
    def batch_size(self):
        ''' The number of objs to push save_objects at a time'''
        return self._default('batch_size', BATCH_SIZE)

    @batch_size.setter
    def batch_size(self, value):
        self.config['batch_size'] = value

    @property
    def cubes_path(self):
        ''' Path to client modules '''
        return self._default('cubes_path', CLIENT_CUBES_PATH)

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
    def host(self):
        ''' The hostname/url/ip to connect to metrique api '''
        return self._default('host', METRIQUE_HTTP_HOST)

    @host.setter
    def host(self, value):
        ''' Set and save in config a metrique api host to use

            If you're connection to a metrique server with
            auth = True, it's highly likely it's ssl = True
            too, so be sure to add https:// to the host!
        '''
        if not isinstance(value, basestring):
            raise TypeError("host must be string")
        self.config['host'] = value

    @property
    def host_port(self):
        ''' Url and schema - http(s)? needed to call metrique api

            If you're connection to a metrique server with
            auth = True, it's highly likely it's ssl = True
            too, so be sure to add https:// to the host!
        '''
        if not re.match('http', self.host):
            host = '%s%s' % ('http://', self.host)
        else:
            host = self.host

        if not re.match('https?://', host):
            raise ValueError("Invalid schema (%s). "
                             "Expected: %s" % (host, 'http(s)?'))
        host_port = '%s:%s' % (host, self.port)
        return host_port

    @property
    def port(self):
        ''' Port need to connect to metrique api '''
        return self._default('port', METRIQUE_HTTP_PORT)

    @port.setter
    def port(self, value):
        ''' Set and save in config a metrique api port to use '''
        if not isinstance(value, basestring):
            raise TypeError("port must be string")
        self.config['port'] = value

    @property
    def password(self):
        ''' The password to connect to metrique api with (OPTIONAL)'''
        return self._default('password')

    @password.setter
    def password(self, value):
        ''' Set and save the password to connect to metrique api with '''
        self.config['password'] = value

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
    @property
    def ssl(self):
        ''' Determine if ssl schema used in a given host string'''
        return bool(re.match('https://', self.host))

    @property
    def ssl_verify(self):
        ''' Current api version in use '''
        return self._default('ssl_verify', True)

    @ssl_verify.setter
    def ssl_verify(self, value):
        ''' Set and save in config a metrique api port to use '''
        self.config['ssl_verify'] = value

    @property
    def username(self):
        ''' The username to connect to metrique api with (OPTIONAL)'''
        return self._default('username', os.getenv('USER'))

    @username.setter
    def username(self, value):
        ''' Set and save the username to connect to metrique api with '''
        self.config['username'] = value

