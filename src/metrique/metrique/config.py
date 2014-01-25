#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
config.py contains the main configuration object for
metrique client applications, which includes built-in
defaults.

Pure defaults assume local, insecure 'test', 'development'
or 'personal' environment. The defaults are NOT for production
use!

To customize local client configuration, add/update
`~/.metrique/etc/metrique.json` (default).
'''

import multiprocessing
import os
import re

from metriqueu.jsonconf import JSONConf

USER_DIR = os.path.expanduser('~/.metrique')
CONFIG_DIR = os.path.join(USER_DIR, 'etc')
LOG_DIR = os.path.join(USER_DIR, 'logs')
TMP_DIR = os.path.join(USER_DIR, 'tmp')
GNUPG_DIR = os.path.expanduser('~/.gnupg')
COOKIEJAR = os.path.join(USER_DIR, '.cookiejar')
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, 'metrique')


class Config(JSONConf):
    ''' Client config (property) class

    DEFAULTS::
        api_verison: metriqued api version in use
        api_rel_path: metriqued api uri prefix
        auto_login: automatically attempt to log-in to metriqued host (False)
        batch_size: The number of objs save at a time (5000)
        cookiejar: path to file for storing cookies (~/.metrique/.cookiejar)
        configdir: path to where config files are located (~/.metrique/etc)
        cube_autoregister: automatically attempt to register non-existant cubes
        cube_pkgs: list of package names where to search for cubes ('cubes')
        cube_paths: Additional paths to search for client cubes
        debug: turn on debug mode logging (level: INFO)
        gnupg_dir: path to where user gnupg data directory
        gnupg_fingerprint: gpnupg fingerprint to sign/verify with
        host: metriqued server host (multiple hosts separated with ',')
        logdir: path to where log files are stored (~/.metrique/logs)
        logfile: filename for logs ('metrique.log')
        log2file: boolean - log output to file? (False)
        logstout: boolean - log output to stdout? (True)
        max_workers: number of workers to use during threaded operations
        password: the password to connect to metriqued with
        port: metriqued server port
        sql_batch_size: number of objects to sql query for at a time (1000)
        ssl: connect to metriqued with SSL (https)
        ssl_verify: verify ssl certificate
        tmpdir: path to temporary data storage (~/.metrique/tmp)
        username: the username to connect to metriqued with
        userdir: path to metrique user directory (~/.metrique)
    '''
    default_config = DEFAULT_CONFIG
    default_config_dir = CONFIG_DIR

    def __init__(self, config_file=None, **kwargs):
        super(Config, self).__init__(config_file=config_file, **kwargs)
        self.config.update({
            'api_version': 'v2',
            'api_rel_path': 'api/v2',
            'auto_login': False,
            'batch_size': 5000,
            'cookiejar': COOKIEJAR,
            'configdir': self.default_config_dir,
            'cube_autoregister': False,
            'cube_pkgs': ['cubes'],
            'cube_paths': [],
            'debug': None,
            'gnupg_dir': GNUPG_DIR,
            'gnupg_fingerprint': None,
            'host': '127.0.0.1',
            'logdir': LOG_DIR,
            'logfile': 'metrique.log',
            'log2file': True,
            'logstdout': True,
            'max_workers': multiprocessing.cpu_count(),
            'password': None,
            'port': 5420,
            'sql_batch_size': 1000,
            'ssl': False,
            'ssl_verify': False,
            'tmpdir': TMP_DIR,
            'username': os.getenv('USER'),
            'userdir': USER_DIR,
        })

    @property
    def gnupg(self):
        if hasattr(self, '_gnupg'):
            gpg = self._gnupg
        else:
            # avoid exception in py2.6
            # workaround until
            # https://github.com/isislovecruft/python-gnupg/pull/36 is resolved
            try:
                from gnupg import GPG
            except (ImportError, AttributeError):
                gpg = None
            else:
                gpg = GPG(homedir=os.path.expanduser(self['gnupg_dir']))
        return gpg

    @property
    def gnupg_pubkey(self):
        if self.gnupg:
            return self.gnupg.export_keys(self['gnupg_fingerprint'])
        else:
            return ''

    @property
    def api_uris(self):
        ''' Url and schema - http(s)? needed to call metrique api '''
        return [os.path.join(uri, self.api_rel_path) for uri in self.uris]

    @property
    def uris(self):
        ''' Url and schema - http(s)? needed to call metrique api '''
        protocol = 'https://' if self.ssl else 'http://'

        uris = []
        hosts = [x.strip() for x in self.host.split(',')]
        for host in hosts:
            if not re.match('https?://', host):
                host = '%s%s' % (protocol, host)
            uris.append('%s:%s' % (host, self.port))
        return uris
