#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.config
~~~~~~~~~~~~~~~

This module contains the main configuration object for
metrique client applications, which includes built-in
defaults.

Pure defaults assume local, insecure 'test', 'development'
or 'personal' environment. The defaults are NOT for production
use!

To customize local client configuration, add/update
`~/.metrique/etc/metrique.json` (default).
'''

import logging
import multiprocessing
import os

from metrique.jsonconf import JSONConf

logger = logging.getLogger(__name__)

# if HOME environment variable is set, use that
# useful when running 'as user' with root (supervisord)
ETC_DIR = os.environ.get('METRIQUE_ETC')
DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metrique.json')


class Config(JSONConf):
    '''
    Client default config class. All metrique clients should subclass
    from their config objects from this class to ensure defaults
    values are available.

    This configuration class defines the following overrideable defaults.

    :param batch_size: The number of objs save at a time (5000)
    :param cube_pkgs: list of package names where to search for cubes ('cubes')
    :param cube_paths: Additional paths to search for client cubes (None)
    :param debug: turn on debug mode logging (level: INFO)
    :param logfile: filename for logs ('metrique.log')
    :param log2file: boolean - log output to file? (False)
    :param logstout: boolean - log output to stdout? (True)
    :param max_workers: number of workers for threaded operations (#cpus)
    :param password: the password to connect to metriqued with (None)
    :param sql_retries: number of attempts to run sql queries before excepting
    :param sql_batch_size: number of objects to sql query for at a time (1000)
    :param username: the username to connect to metriqued with ($USERNAME)
    '''
    default_config = DEFAULT_CONFIG
    default_config_dir = ETC_DIR

    def __init__(self, config_file=None, **kwargs):
        config = {
            'batch_size': 1000,
            'cube_pkgs': ['cubes'],
            'cube_paths': [],
            'debug': True,
            'logfile': 'metrique.log',
            'log2file': True,
            'logstdout': False,
            'max_workers': multiprocessing.cpu_count(),
            'sql_retries': 1,
            'sql_batch_size': 500
        }
        # apply defaults
        self.config.update(config)
        # update the config with the args from the config_file
        super(Config, self).__init__(config_file=config_file)
        # anything passed in explicitly gets precedence
        self.config.update(kwargs)
