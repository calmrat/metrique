#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metriqueu.jsonconf import JSONConf
from metriqued.basemongodb import BaseMongoDB

USER_DIR = os.path.expanduser('~/.metrique')
CONFIG_DIR = os.path.join(USER_DIR, 'etc')
LOG_DIR = os.path.join(USER_DIR, 'logs')

DEFAULT_CONFIG = os.path.join(CONFIG_DIR, 'metriqued')

for path in [USER_DIR, CONFIG_DIR, LOG_DIR]:
    if not os.path.exists(path):
        os.makedirs(path)

SSL_CERT_FILE = os.path.join(CONFIG_DIR, 'cert.pem')
SSL_KEY_FILE = os.path.join(CONFIG_DIR, 'pkey.pem')

here = os.path.dirname(os.path.abspath(__file__))
STATIC_PATH = os.path.join(here, 'static/')


class metriqued_config(JSONConf):
    def __init__(self, config_file=None):
        self.default_config = DEFAULT_CONFIG
        self.defaults = {
            'async': True,
            'autoreload': False,
            'cookie_secret': '____UPDATE_COOKIE_SECRET_CONFIG____',
            'configdir':  CONFIG_DIR,
            'debug': None,
            'gzip': True,
            'host': '127.0.0.1',
            'krb_auth': False,
            'logdir': LOG_DIR,
            'logfile': 'metriqued.log',
            'log2file': True,
            'logstdout': False,
            'login_url': '/login',
            'max_processes': 0,
            'mongodb_config': None,
            'port': 5420,
            'realm': 'metrique',
            'ssl': False,
            'ssl_certificate': SSL_CERT_FILE,
            'ssl_certificate_key': SSL_KEY_FILE,
            'static_path': STATIC_PATH,
            'superusers': ['admin'],
            'userdir': USER_DIR,
            'xsrf_cookies': False,
        }
        super(metriqued_config, self).__init__(config_file=config_file)


class mongodb_config(JSONConf):
    def __init__(self, config_file=None):
        self.default_config = os.path.join(CONFIG_DIR, 'mongodb')
        self.defaults = {
            'auth': False,
            'admin_password': None,
            'admin_user': 'admin',
            'data_password': None,
            'data_user': 'metrique',
            'db_metrique': 'metrique',
            'db_timeline': 'timeline',
            'collection_cube_profile': 'cube_profile',
            'collection_user_profile': 'user_profile',
            'host': '127.0.0.1',
            'port': 27017,
            'mongoexport': '/usr/bin/mongoexport',
            'ssl': False,
            'ssl_certificate': SSL_CERT_FILE,
            'ssl_certificate_key': SSL_KEY_FILE,
            'write_concern': 1,
        }
        super(mongodb_config, self).__init__(config_file=config_file)

    @property
    def db_metrique_data(self):
        return BaseMongoDB(host=self.host, db=self.db_metrique,
                           auth=self.auth,
                           user=self.data_user,
                           password=self.data_password,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern)

    @property
    def db_metrique_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_metrique,
                           auth=self.auth,
                           user=self.admin_user,
                           password=self.admin_password,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern)

    @property
    def db_timeline_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           auth=self.auth,
                           user=self.admin_user,
                           password=self.admin_password,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern)

    @property
    def db_timeline_data(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           auth=self.auth,
                           user=self.data_user,
                           password=self.data_password,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern)

    @property
    def c_user_profile_data(self):
        return self.db_metrique_data[self.collection_user_profile]

    @property
    def c_user_profile_admin(self):
        return self.db_metrique_admin[self.collection_user_profile]

    @property
    def c_cube_profile_data(self):
        return self.db_metrique_data[self.collection_cube_profile]

    @property
    def c_cube_profile_admin(self):
        return self.db_metrique_admin[self.collection_cube_profile]
