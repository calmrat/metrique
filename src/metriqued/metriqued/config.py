#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import os
from jsonconf import JSONConf

from metriqued.mongodb.basemongodb import BaseMongoDB
from metriqued.utils import new_cookie_secret

CONFIG_DIR = '~/.metrique'
METRIQUE_CONF = os.path.join(CONFIG_DIR, 'metrique_config')

METRIQUE_HTTP_HOST = '127.0.0.1'
METRIQUE_HTTP_PORT = 5420

MONGODB_HOST = METRIQUE_HTTP_HOST
MONGODB_PORT = 27017
MONGODB_CONF = os.path.join(CONFIG_DIR, 'mongodb_config')

PID_FILE = os.path.join(CONFIG_DIR, 'server.pid')
SSL = False
SSL_CERT_FILE = os.path.join(CONFIG_DIR, 'cert.pem')
SSL_KEY_FILE = os.path.join(CONFIG_DIR, 'pkey.pem')
WRITE_CONCERN = 1

ADMIN_USER = 'admin'
DATA_USER = 'metrique'

METRIQUE_DB = 'metrique'
TIMELINE_DB = 'timeline'

USER_PROFILE_COLLECTION = 'user_profile'
CUBE_PROFILE_COLLECTION = 'cube_profile'

ROOT_LOGGER = 'metrique'
DATE_FORMAT = '%Y%m%dT%H:%M:%S'
LOG_FORMAT = u'%(processName)s:%(message)s'
LOG_FORMATTER = logging.Formatter(LOG_FORMAT,
                                  DATE_FORMAT)

LOGDIR_SAVEAS = '%s/metriqued.log' % CONFIG_DIR

CUBE_QUOTA = None


class metrique(JSONConf):
    def __init__(self, config_file, force=True):
        if not config_file:
            config_file = METRIQUE_CONF
        super(metrique, self).__init__(config_file=config_file,
                                       force=force)

    defaults = {'async': True,
                'autoreload': False,
                'gzip': True,
                'http_host': METRIQUE_HTTP_HOST,
                'http_port': METRIQUE_HTTP_PORT,
                'krb_auth': False,
                'realm': 'metrique',
                'log_formatter': LOG_FORMATTER,
                'login_url': '/login',
                'max_processes': 0,
                'ssl': False,
                'xsrf_cookies': False,
                }

    @property
    def superusers(self):
        return self._default('superusers', [ADMIN_USER])

    @superusers.setter
    def superusers(self, value):
        if isinstance(value, basestring):
            value = [value]
        if not isinstance(value, list):
            raise TypeError(
                "superuser must be a single or list of strings")
        self.config['superusers'] = value

    @property
    def cookie_secret(self):
        if self.config.get('cookie_secret'):
            return self.config['cookie_secret']
        else:
            # automatically generate a new cookie secret
            # NOTE A NEW SECRET WILL INVALIDATE ALL PREVIOUS
            # COOKIES; IN PRODUCTION, MAKE SURE TO HARDCODE
            # THE COOKIE SECRETE IN metrique_config.json
            return self._default('cookie_secret', new_cookie_secret())

    @property
    def debug(self):
        ''' Reflect whether debug is enabled or not '''
        return self._default('debug', -1)

    @debug.setter
    def debug(self, n):
        logger = logging.getLogger(ROOT_LOGGER)
        if n == -1:
            level = logging.WARN
        elif n == 0:
            level = logging.INFO
        elif n == 1:
            level = logging.DEBUG
            logger.debug("Debug: metrique")
        elif n == 2:
            logger = logging.getLogger()
            level = logging.DEBUG
            logger.debug("Debug: metrique, tornado")
        logger.setLevel(level)
        self.config['debug'] = n

    @property
    def log_file_path(self):
        return os.path.expanduser(
            self._default('log_file_path', LOGDIR_SAVEAS))

    @property
    def pid_file(self):
        pid_file = os.path.expanduser(
            self._default('pid_file', PID_FILE))
        if not pid_file.endswith(".pid"):
            raise ValueError('pid file must end with .pid')
        return pid_file

    @property
    def static_path(self):
        abspath = os.path.dirname(os.path.abspath(__file__))
        static_path = os.path.join(abspath, 'static/')
        return self._default('static_path', static_path)

    @property
    def ssl_certificate(self):
        return os.path.expanduser(
            self._default('ssl_certificate', SSL_CERT_FILE))

    @ssl_certificate.setter
    def ssl_certificate(self, value):
        self.config['ssl_certificate'] = value

    @property
    def ssl_certificate_key(self):
        return os.path.expanduser(
            self._default('ssl_certificate_key', SSL_KEY_FILE))

    @ssl_certificate_key.setter
    def ssl_certificate_key(self, value):
        self.config['ssl_certificate_key'] = value


class mongodb(JSONConf):
    def __init__(self, config_file=None, force=True, *args, **kwargs):
        if not config_file:
            config_file = MONGODB_CONF
        super(mongodb, self).__init__(config_file, force=force,
                                      *args, **kwargs)

    defaults = {'admin_password': None,
                'data_password': None,
                'data_user': DATA_USER,
                'db_metrique': METRIQUE_DB,
                'db_timeline': TIMELINE_DB,
                'collection_cube_profile': CUBE_PROFILE_COLLECTION,
                'collection_user_profile': USER_PROFILE_COLLECTION,
                'host': MONGODB_HOST,
                'port': MONGODB_PORT,
                'ssl': SSL,
                'write_concern': WRITE_CONCERN,
                }

    @property
    def db_metrique_data(self):
        return BaseMongoDB(host=self.host, db=self.db_metrique,
                           user=self.admin_user,
                           password=self.admin_password,
                           admin=False,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern)

    @property
    def db_metrique_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_metrique,
                           user=self.admin_user,
                           password=self.admin_password,
                           admin=True,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern)

    @property
    def db_timeline_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           user=self.admin_user,
                           password=self.admin_password,
                           admin=True,
                           ssl=self.ssl,
                           ssl_certfile=self.ssl_certificate,
                           ssl_keyfile=self.ssl_certificate_key,
                           write_concern=self.write_concern)

    @property
    def db_timeline_data(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           user=self.data_user,
                           password=self.data_password,
                           ssl=self.ssl)

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

    @property
    def ssl_certificate(self):
        return os.path.expanduser(
            self._default('ssl_certificate', SSL_CERT_FILE))

    @ssl_certificate.setter
    def ssl_certificate(self, value):
        self.config['ssl_certificate'] = value

    @property
    def ssl_certificate_key(self):
        return os.path.expanduser(
            self._default('ssl_certificate_key', SSL_KEY_FILE))

    @ssl_certificate_key.setter
    def ssl_certificate_key(self, value):
        self.config['ssl_certificate_key'] = value
