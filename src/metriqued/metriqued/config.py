#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metriqueu.jsonconf import JSONConf
from metriqued.basemongodb import BaseMongoDB

USER_DIR = os.path.expanduser('~/.metrique')
ETC_DIR = os.path.join(USER_DIR, 'etc')
PID_DIR = os.path.join(USER_DIR, 'pids')
LOG_DIR = os.path.join(USER_DIR, 'logs')
GNUPG_DIR = os.path.join(USER_DIR, 'gnupg')

DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metriqued')

for path in [USER_DIR, ETC_DIR, LOG_DIR, PID_DIR]:
    if not os.path.exists(path):
        os.makedirs(path)

SSL_CERT = os.path.join(ETC_DIR, 'metrique.cert')
SSL_KEY = os.path.join(ETC_DIR, 'metrique.key')
SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

here = os.path.dirname(os.path.abspath(__file__))
STATIC_PATH = os.path.join(here, 'static/')


class metriqued_config(JSONConf):
    def __init__(self, config_file=None):
        self.default_config = DEFAULT_CONFIG
        self.defaults = {
            'autoreload': False,
            'cookie_secret': '____UPDATE_COOKIE_SECRET_CONFIG____',
            'configdir':  ETC_DIR,
            'user_cube_quota': 3,
            'debug': None,
            'gnupg_dir': GNUPG_DIR,
            'gnupg_fingerprint': None,
            'gzip': True,
            'host': '127.0.0.1',
            'krb_auth': False,
            'logdir': LOG_DIR,
            'logfile': 'metriqued.log',
            'log2file': True,
            'logstdout': False,
            'logrotate': 134217728,  # 128M 'maxBytes' before rotate
            'logkeep': 20,
            'login_url': '/login',
            'mongodb_config': None,
            'piddir': PID_DIR,
            'port': 5420,
            'realm': 'metrique',
            'ssl': False,
            'ssl_certificate': SSL_CERT,
            'ssl_certificate_key': SSL_KEY,
            'static_path': STATIC_PATH,
            'superusers': ["admin"],
            'userdir': USER_DIR,
            'xsrf_cookies': False,
        }
        super(metriqued_config, self).__init__(config_file=config_file)

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


class mongodb_config(JSONConf):
    def __init__(self, config_file=None):
        self.default_config = os.path.join(ETC_DIR, 'mongodb')
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
            'fsync': False,
            'host': '127.0.0.1',
            'journal': True,
            'port': 27017,
            'mongoexport': '/usr/bin/mongoexport',
            'tz_aware': True,
            'ssl': True,
            'ssl_certificate': SSL_PEM,
            'ssl_certificate_key': None,
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
                           tz_aware=self.tz_aware)

    @property
    def db_metrique_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_metrique,
                           auth=self.auth,
                           user=self.admin_user,
                           password=self.admin_password,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern,
                           fsync=self.fsync,
                           journal=self.journal,
                           tz_aware=self.tz_aware)

    @property
    def db_timeline_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           auth=self.auth,
                           user=self.admin_user,
                           password=self.admin_password,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern,
                           fsync=self.fsync,
                           journal=self.journal,
                           tz_aware=self.tz_aware)

    @property
    def db_timeline_data(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           auth=self.auth,
                           user=self.data_user,
                           password=self.data_password,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           tz_aware=self.tz_aware)

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
