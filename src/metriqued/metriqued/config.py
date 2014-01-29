#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriqued.config
~~~~~~~~~~~~~~~~

This module contains the main configuration object for
metriqued server applications, which includes built-in
defaults.

Pure defaults assume local, insecure 'test', 'development'
or 'personal' environment. The defaults are NOT for production
use!

To customize local client configuration, add/update
`~/.metrique/etc/metriqued.json` (default).
'''

from gnupg import GPG
import os

from metriqueu.jsonconf import JSONConf
from metriquet.tornadohttp import TornadoConfig
from metriqued.basemongodb import BaseMongoDB

USER_DIR = os.path.expanduser('~/.metrique')
ETC_DIR = os.path.join(USER_DIR, 'etc')
GNUPG_DIR = os.path.join(USER_DIR, 'gnupg')

DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metriqued')
MONGODB_CONFIG = os.path.join(ETC_DIR, 'mongodb')

SSL_CERT = os.path.join(ETC_DIR, 'metrique.cert')
SSL_KEY = os.path.join(ETC_DIR, 'metrique.key')
SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

here = os.path.dirname(os.path.abspath(__file__))
STATIC_PATH = os.path.join(here, 'static/')


class metriqued_config(TornadoConfig):
    '''
    metriqued default config class.

    This configuration class defines the following overrideable defaults.

    :param user_cube_quota: max number of cubes a user can own
    :param gnupg_dir: path to gnupg data directory
    :param gnupg_fingerprint: key fingerprint for gpg signing/verification
    :param krb_auth: enable kerberos authentication
    :param log2mongodb: enable passing log events to mongodb?
    :param log_mongodb_level: logger level to listen on and pass to mongodb
    :param mongodb_config: path to mongodb config json
    :param port: port to listen on
    :param superusers: list of usernames that have root access
    '''
    default_config = DEFAULT_CONFIG
    name = 'metriqued'

    def __init__(self, config_file=None, **kwargs):
        config = {
            'user_cube_quota': 3,
            'gnupg_dir': GNUPG_DIR,
            'gnupg_fingerprint': None,
            'krb_auth': False,
            'log2mongodb': False,
            'log_mongodb_level': 100,
            'mongodb_config': None,
            'port': 5420,
            'superusers': ["admin"],
        }
        # apply defaults
        self.config.update(config)
        # update the config with the args from the config_file
        super(metriqued_config, self).__init__(config_file=config_file)
        # anything passed in explicitly gets precedence
        self.config.update(kwargs)

    @property
    def gnupg(self):
        '''Shortcut for dynamically loading gpg module'''
        if hasattr(self, '_gnupg'):
            gpg = self._gnupg
        else:
            gpg = GPG(homedir=os.path.expanduser(self['gnupg_dir']))
        return gpg

    @property
    def gnupg_pubkey(self):
        '''Shortcut for to get gnupg public key for config'd fingerprint'''
        if self.gnupg:
            return self.gnupg.export_keys(self['gnupg_fingerprint'])
        else:
            return ''


class mongodb_config(JSONConf):
    '''
    mongodb default config class.

    This configuration class defines the following overrideable defaults.

    :param auth: enable mongodb authentication
    :param admin_password: admin user password
    :param admin_user: admin username
    :param data_password: data (read only) password
    :param data_user: data (read only) username
    :param db_metrique: metrique db name
    :param db_timeline: timeline db name
    :param collection_cube_profile: cube profile collection name
    :param collection_user_profile: user profile collection name
    :param collection_logs: logs collection name
    :param fsync: sync writes to disk before return?
    :param host: mongodb host(s) to connect to
    :param journal: enable write journal before return?
    :param port: mongodb port to connect to
    :param mongoexport: path to mongoexport command
    :param read_preference: default - SECONDARY_PREFERRED
    :param replica_set: name of replica set, if any
    :param ssl: enable ssl
    :param ssl_certificate: path to ssl certificate file
    :param ssl_certificate_key: path to ssl certificate key file
    :param tz_aware: return back tz_aware dates?
    :param write_concern: what level of write assurance before returning
    '''
    default_config = MONGODB_CONFIG
    name = 'mongodb'

    def __init__(self, config_file=None, **kwargs):
        config = {
            'auth': False,
            'admin_password': None,
            'admin_user': 'admin',
            'data_password': None,
            'data_user': 'metrique',
            'db_metrique': 'metrique',
            'db_timeline': 'timeline',
            'collection_cube_profile': 'cube_profile',
            'collection_user_profile': 'user_profile',
            'collection_logs': 'logs',
            'fsync': False,
            'host': '127.0.0.1',
            'journal': True,
            'port': 27017,
            'mongoexport': '/usr/bin/mongoexport',
            'read_preference': 'SECONDARY_PREFERRED',
            'replica_set': None,
            'ssl': False,
            'ssl_certificate': SSL_PEM,
            'ssl_certificate_key': None,
            'tz_aware': True,
            'write_concern': 1,  # primary; add X for X replicas
        }
        # apply defaults
        self.config.update(config)
        # update the config with the args from the config_file
        super(mongodb_config, self).__init__(config_file=config_file)
        # anything passed in explicitly gets precedence
        self.config.update(kwargs)

    @property
    def db_readonly(self):
        '''Wrapper for a readonly DB proxy connection'''
        if not hasattr(self, '_db_readonly'):
            user = self.data_user
            pwd = self.data_password
            self._db_readonly = BaseMongoDB(
                host=self.host, port=self.port, auth=self.auth,
                user=user, password=pwd,
                ssl=self.ssl, ssl_certfile=self.ssl_certificate,
                ssl_keyfile=self.ssl_certificate_key, tz_aware=self.tz_aware,
                replica_set=self.replica_set,
                read_preference=self.read_preference)
        return self._db_readonly

    @property
    def db_admin(self):
        '''Wrapper for a read/write DB proxy connection'''
        if not hasattr(self, '_db_admin'):
            user = self.admin_user
            pwd = self.admin_password
            self._db_admin = BaseMongoDB(
                host=self.host, port=self.port, auth=self.auth,
                user=user, password=pwd,
                ssl=self.ssl, ssl_certfile=self.ssl_certificate,
                ssl_keyfile=self.ssl_certificate_key, tz_aware=self.tz_aware,
                replica_set=self.replica_set,
                read_preference=self.read_preference)
        return self._db_admin

    @property
    def db_metrique_data(self):
        '''Wrapper for a read only 'metrique' DB proxy'''
        return self.db_readonly[self.db_metrique]

    @property
    def db_timeline_data(self):
        '''Wrapper for a read only 'timeline' DB proxy'''
        return self.db_readonly[self.db_timeline]

    @property
    def db_metrique_admin(self):
        '''Wrapper for a read/write 'metrique' DB proxy'''
        return self.db_admin[self.db_metrique]

    @property
    def db_timeline_admin(self):
        '''Wrapper for a read/write 'timeline' DB proxy'''
        return self.db_admin[self.db_timeline]

    @property
    def c_user_profile_data(self):
        '''Wrapper for a read only 'user profile' collection proxy'''
        return self.db_metrique_data[self.collection_user_profile]

    @property
    def c_user_profile_admin(self):
        '''Wrapper for a read/write 'user profile' collection proxy'''
        return self.db_metrique_admin[self.collection_user_profile]

    @property
    def c_cube_profile_data(self):
        '''Wrapper for a read only 'cube profile' collection proxy'''
        return self.db_metrique_data[self.collection_cube_profile]

    @property
    def c_cube_profile_admin(self):
        '''Wrapper for a read/write 'cube profile' collection proxy'''
        return self.db_metrique_admin[self.collection_cube_profile]

    @property
    def c_logs_admin(self):
        '''Wrapper for a read/write 'logs' collection proxy'''
        return self.db_metrique_admin[self.collection_logs]
