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
`~/.metrique/metrique.json` (default).

Paths are UNIX compatible only.
'''

# FIXME: requires psutil
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
    '''
    default_config = DEFAULT_CONFIG

    def __init__(self, config_file=None, *args, **kwargs):
        self.defaults = {
            'api_version': 'v2',
            'api_rel_path': 'api/v2',
            'auto_login': False,
            'batch_size': 5000,
            'cookiejar': COOKIEJAR,
            'configdir': CONFIG_DIR,
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
            'retries': 1,
            'sort': -1,
            'sql_batch_size': 1000,
            'ssl': False,
            'ssl_verify': False,
            'tmpdir': TMP_DIR,
            'username': os.getenv('USER'),
            'userdir': USER_DIR,
        }
        super(Config, self).__init__(config_file=config_file, *args, **kwargs)

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
    def api_url(self):
        ''' Url and schema - http(s)? needed to call metrique api '''
        return os.path.join(self.host_port, self.api_rel_path)

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
