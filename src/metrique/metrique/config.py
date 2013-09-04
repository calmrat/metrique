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

from distutils.sysconfig import get_python_lib
import logging
import os
import re

from jsonconf import JSONConf

DEFAULT_CONFIG_DIR = '~/.metrique'
DEFAULT_CONFIG_FILE = 'http_api'

DEFAULT_CLIENT_CUBES_BASE_PATH = 'cubes/'
DEFAULT_CLIENT_CUBES_PATH = os.path.join(
    DEFAULT_CONFIG_DIR, DEFAULT_CLIENT_CUBES_BASE_PATH)


DEFAULT_SYS_CUBES_BASE_PATH = 'metriquec/'
DEFAULT_SYSTEM_CUBES_PATH = os.path.join(
    get_python_lib(), DEFAULT_SYS_CUBES_BASE_PATH)

API_VERSION = 'v1'
API_REL_PATH = 'api'
API_SSL = False

METRIQUE_HTTP_HOST = '127.0.0.1'
METRIQUE_HTTP_PORT = 8080


class Config(JSONConf):
    ''' Client config (property) class '''
    def __init__(self, config_file, config_dir, force=True,
                 *args, **kwargs):
        if not config_file:
            config_file = DEFAULT_CONFIG_FILE
        if not config_dir:
            config_dir = DEFAULT_CONFIG_DIR
        super(Config, self).__init__(config_file=config_file,
                                     config_dir=config_dir,
                                     force=force, *args, **kwargs)

    @property
    def cubes_path(self):
        ''' Path to client modules '''
        return self._default('cubes_path', DEFAULT_CLIENT_CUBES_PATH)

    @property
    def api_ssl(self):
        ''' Determine if ssl schema used in a given host string'''
        if re.match('https://', self.api_host):
            return True
        else:
            return False

    @property
    def api_version(self):
        ''' Current api version in use '''
        return self._default('api_version', API_VERSION)

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
        api_url = os.path.join(host_port, self.api_rel_path)
        return api_url

    @property
    def api_port(self):
        ''' Port need to connect to metrique api '''
        return self._default('api_port', METRIQUE_HTTP_PORT)

    @api_port.setter
    def api_port(self, value):
        ''' Set and save in config a metrique api port to use '''
        if not isinstance(value, basestring):
            raise TypeError("api_port must be string")
        self.config['api_port'] = value

    @property
    def api_host(self):
        ''' The hostname/url/ip to connect to metrique api '''
        return self._default('api_host', METRIQUE_HTTP_HOST)

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
    def api_username(self):
        ''' The username to connect to metrique api with (OPTIONAL)'''
        return self._default('api_username')

    @api_username.setter
    def api_username(self, value):
        ''' Set and save the username to connect to metrique api with '''
        self.config['api_username'] = value

    @property
    def api_password(self):
        ''' The password to connect to metrique api with (OPTIONAL)'''
        return self._default('api_password')

    @api_password.setter
    def api_password(self, value):
        ''' Set and save the password to connect to metrique api with '''
        self.config['api_password'] = value

    @property
    def async(self):
        ''' Turn on/off async (parallel) multiprocessing (where supported) '''
        return self._default('async', True)

    @async.setter
    def async(self, value):
        self.config['async'] = value

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

    def _set_debug(self, level, logger=None):
        '''
        if we get a level of 2, we want to apply the
        debug level to all loggers
        '''
        if not logger:
            logger = logging.getLogger()

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
        ''' The number of tries to make when running a query when it fails '''
        return self._default('sql_delta_batch_retries', 3)
