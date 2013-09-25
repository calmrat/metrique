#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
config.py contains the main configuration object for
metrique client applications, which includes built-in
defaults.

Pure defaults assume local, insecure 'test', 'development'
or 'personal' environment. The defaults are not meant for
production use.

To customize local client configuration, add/update
`~/.metrique/http_api.json` (default).

Paths are UNIX compatible only.
'''

import logging
import os
import re
from distutils.sysconfig import get_python_lib

from metriqueu.jsonconf import JSONConf

METRIQUE_HTTP_HOST = '127.0.0.1'
METRIQUE_HTTP_PORT = 5420

CONFIG_DIR = '~/.metrique'
CONFIG_FILE = os.path.join(CONFIG_DIR, 'http_api')
CLIENT_CUBES_PATH = os.path.join(CONFIG_DIR, 'cubes/')
SYSTEM_CUBES_PATH = os.path.join(get_python_lib(), 'metriquec/')

BATCH_SIZE = -1

API_VERSION = 'v2'


class Config(JSONConf):
    ''' Client config (property) class '''
    def __init__(self, config_file, force=True, *args, **kwargs):
        config_file = config_file or CONFIG_FILE
        super(Config, self).__init__(config_file=config_file, force=force,
                                     *args, **kwargs)

    '''
    api_verison: Current api version in use
    async: Turn on/off async (parallel) multiprocessing (where supported)
    auto_login: ...
    batch_size: The number of objs to push save_objects at a time
    cubes_path: Path to client modules
    username: The username to connect to metrique api with (OPTIONAL)
    password: The password to connect to metrique api with (OPTIONAL)
    ssl: Connect with SSL (https)
    ssl_verify: ...
    sql_delta_batch_size:
        The number of objects to query at a time in sql.id_delta
    sql_delta_batch_retries: ...
    '''
    defaults = {'api_version': API_VERSION,
                'async': True,
                'auto_login': True,
                'batch_size': BATCH_SIZE,
                'cubes_path': CLIENT_CUBES_PATH,
                'logfile': None,
                'password': None,
                'ssl': False,
                'ssl_verify': True,
                'sql_delta_batch_size': 1000,
                'sql_delta_batch_retries': 3,
                'username': os.getenv('USER'),
                }

    @property
    def api_rel_path(self):
        ''' Reletive paths from url.root needed
            to trigger/access the metrique http api
        '''
        def_rel_path = os.path.join('api', self.api_version)
        return self._default('api_rel_path', def_rel_path)

    @property
    def api_url(self):
        ''' Url and schema - http(s)? needed to call metrique api '''
        return os.path.join(self.host_port, self.api_rel_path)

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
        if not logger or level == 2:
            logger = logging.getLogger()

        if self.logfile:
            logfile = os.path.expanduser(self.logfile)
            for hdlr in logger.handlers:
                logger.removeHandler(hdlr)
            logger.removeHandler
            hdlr = logging.FileHandler(logfile)
            logger.addHandler(hdlr)

        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)

    @property
    def host(self):
        ''' The hostname/url/ip to connect to metrique api '''
        return self._default('host', METRIQUE_HTTP_HOST)

    @host.setter
    def host(self, value):
        ''' Set and save in config a metrique api host to use '''
        if not isinstance(value, basestring):
            raise TypeError("host must be string")
        self.config['host'] = value

    @property
    def host_port(self):
        ''' Url and schema - http(s)? needed to call metrique api '''
        if self.ssl:
            protocol = 'https://'
        else:
            protocol = 'http://'

        if not re.match('http', self.host):
            host = '%s%s' % (protocol, self.host)
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
