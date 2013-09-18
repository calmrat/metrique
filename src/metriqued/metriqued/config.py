#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import os
from jsonconf import JSONConf

from metriqued.mongodb.basemongodb import BaseMongoDB
from metriqued.utils import new_cookie_secret

from metriqueu.defaults import CONFIG_DIR
from metriqueu.defaults import METRIQUE_HTTP_HOST
from metriqueu.defaults import METRIQUE_HTTP_PORT
from metriqueu.defaults import METRIQUE_LOGIN_URL

pjoin = os.path.join

METRIQUE_CONF = pjoin(CONFIG_DIR, 'metrique_config')

MONGODB_HOST = METRIQUE_HTTP_HOST
MONGODB_PORT = 27017
MONGODB_CONF = pjoin(CONFIG_DIR, 'mongodb_config')

PID_FILE = pjoin(CONFIG_DIR, 'server.pid')
SSL = False
SSL_CERT_FILE = pjoin(CONFIG_DIR, 'cert.pem')
SSL_KEY_FILE = pjoin(CONFIG_DIR, 'pkey.pem')
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
        self._properties = {}

    @property
    def admin_user(self):
        return self._default('admin_user', ADMIN_USER)

    @property
    def admin_password(self):
        return self._default('admin_password', None)

    @admin_password.setter
    def admin_password(self, value):
        self.config['admin_password'] = value

    @property
    def async(self):
        return self._default('async', True)

    @async.setter
    def async(self, bool_):
        self.config['async'] = bool_

    @property
    def autoreload(self):
        return self._default('autoreload', False)

    @autoreload.setter
    def autoreload(self, value):
        self.config['autoreload'] = value

    @property
    def cookie_secret(self):
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
    def gzip(self):
        return self._default('gzip', True)

    @gzip.setter
    def gzip(self, value):
        self.config['gzip'] = value

    @property
    def http_host(self):
        return self._default('http_host', METRIQUE_HTTP_HOST)

    @http_host.setter
    def http_host(self, value):
        self.config['http_host'] = value

    @property
    def http_port(self):
        return self._default('http_port', METRIQUE_HTTP_PORT)

    @http_port.setter
    def http_port(self, value):
        self.config['http_port'] = value

    @property
    def krb_auth(self):
        return self._default('krb_auth', False)

    @krb_auth.setter
    def krb_auth(self, value):
        self.config['krb_auth'] = value

    @property
    def realm(self):
        return self._default('realm', 'metrique')

    @property
    def log_formatter(self):
        return self._property_default('log_formatter', LOG_FORMATTER)

    @property
    def log_file_path(self):
        return os.path.expanduser(
            self._default('log_file_path', LOGDIR_SAVEAS))

    @property
    def login_url(self):
        return self._default('login_url', METRIQUE_LOGIN_URL)

    @property
    def pid_file(self):
        pid_file = os.path.expanduser(
            self._default('pid_file', PID_FILE))
        if not pid_file.endswith(".pid"):
            raise ValueError('pid file must end with .pid')
        return pid_file

    @property
    def max_processes(self):
        return self._default('max_processes', 0)

    @max_processes.setter
    def max_processes(self, value):
        return self._default('max_processes', value)

    @property
    def static_path(self):
        abspath = os.path.dirname(os.path.abspath(__file__))
        static_path = os.path.join(abspath, 'static/')
        return self._default('static_path', static_path)

    @property
    def ssl(self):
        return self._default('ssl', False)

    @ssl.setter
    def ssl(self, value):
        self.config['ssl'] = value

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

    @property
    def xsrf_cookies(self):
        return self._default('xsrf_cookies', False)

    @xsrf_cookies.setter
    def xsrf_cookies(self, value):
        self.config['xsrf_cookies'] = value


class mongodb(JSONConf):
    def __init__(self, config_file=None, force=True, *args, **kwargs):
        if not config_file:
            config_file = MONGODB_CONF
        super(mongodb, self).__init__(config_file, force=force,
                                      *args, **kwargs)

    @property
    def admin_password(self):
        return self._default('admin_password', None)

    @admin_password.setter
    def admin_password(self, value):
        self.config['admin_password'] = value

    @property
    def admin_user(self):
        return self._default('admin_user', ADMIN_USER)

    @property
    def data_password(self):
        return self._default('data_password', None)

    @data_password.setter
    def data_password(self, value):
        self.config['data_password'] = value

    @property
    def data_user(self):
        return self._default('data_user', DATA_USER)

    @property
    def db_metrique(self):
        return self._default('db_metrique', METRIQUE_DB)

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
    def db_timeline(self):
        return self._default('db_timeline', TIMELINE_DB)

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
    def collection_cube_profile(self):
        return self._default('collection_cube_profile',
                             CUBE_PROFILE_COLLECTION)

    @property
    def collection_user_profile(self):
        return self._default('collection_user_profile',
                             USER_PROFILE_COLLECTION)

    @property
    def host(self):
        return self._default('host', MONGODB_HOST)

    @host.setter
    def host(self, value):
        self.config['host'] = value

    @property
    def port(self):
        return self._default('port', MONGODB_PORT)

    @port.setter
    def port(self, value):
        self.config['port'] = value

    @property
    def ssl(self):
        return self._default('ssl', SSL)

    @ssl.setter
    def ssl(self, value):
        self.config['ssl'] = value

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

    @property
    def write_concern(self):
        return self._default('write_concern', WRITE_CONCERN)

    @write_concern.setter
    def write_concern(self, value):
        self.config['write_concern'] = value
