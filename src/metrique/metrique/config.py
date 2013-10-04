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

from metriqueu.jsonconf import JSONConf


class Config(JSONConf):
    ''' Client config (property) class

    DEFAULTS::
        api_verison: Current api version in use
        async: Turn on/off async (parallel) multiprocessing (where supported)
        auto_login: ...
        batch_size: The number of objs to push save_objects at a time
        cubes_path: Path to client modules
        host: Metrique Server host
        username: The username to connect to metrique api with (OPTIONAL)
        password: The password to connect to metrique api with (OPTIONAL)
        port: Metrique server port
        ssl: Connect with SSL (https)
        ssl_verify: ...
        sql_delta_batch_size:
            The number of objects to query at a time in sql.id_delta
        sql_delta_batch_retries: ...
    '''
    defaults = {
        'api_version': 'v2',
        'api_rel_path': 'api/v2',
        'async': True,
        'auto_login': True,
        'batch_size': -1,
        'cubes_path': '~/.metrique/cubes',
        'host': '127.0.0.1',
        'logfile': None,
        'password': None,
        'port': 5420,
        'ssl': False,
        'ssl_verify': True,
        'sql_delta_batch_size': 1000,
        'sql_delta_batch_retries': 3,
        'username': os.getenv('USER'),
    }

    def __init__(self, config_file='~/.metrique/http_api', force=True,
                 *args, **kwargs):
        super(Config, self).__init__(config_file=config_file, force=force,
                                     *args, **kwargs)

    @property
    def api_url(self):
        ''' Url and schema - http(s)? needed to call metrique api '''
        return os.path.join(self.host_port, self.api_rel_path)

    @property
    def debug(self):
        ''' Reflect whether debug is enabled or not '''
        return self._default('debug', False)

    @debug.setter
    def debug(self, value):
        ''' Update logger settings '''
        if isinstance(value, (tuple, list)):
            logger, value = value
            self._debug_set(value, logger)
        else:
            try:
                logger = self.logger
            except AttributeError:
                self._debug_set(value)
            else:
                self._debug_set(value, logger)
        self.config['debug'] = value

    def _debug_set(self, level, logger=None):
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
    def host_port(self):
        ''' Url and schema - http(s)? needed to call metrique api '''
        protocol = 'https://' if self.ssl else 'http://'

        if not re.match('https?://', self.host):
            host = '%s%s' % (protocol, self.host)
        else:
            host = self.host

        host_port = '%s:%s' % (host, self.port)
        return host_port
