#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
import os
from jsonconf import JSONConf
from tornado.web import HTTPError

from metriqued.mongodb.basemongodb import BaseMongoDB

from metriqueu.defaults import DEFAULT_CONFIG_DIR
from metriqueu.defaults import DEFAULT_METRIQUE_HTTP_HOST
from metriqueu.defaults import DEFAULT_METRIQUE_HTTP_PORT
from metriqueu.defaults import DEFAULT_METRIQUE_LOGIN_URL

DEFAULT_METRIQUE_CONF = os.path.join(DEFAULT_CONFIG_DIR, 'metrique_config')

DEFAULT_MONGODB_HOST = DEFAULT_METRIQUE_HTTP_HOST
DEFAULT_MONGODB_CONF = os.path.join(DEFAULT_CONFIG_DIR, 'mongodb_config')

DEFAULT_PID_FILE = '%s/server.pid' % DEFAULT_CONFIG_DIR
DEFAULT_SSL_CERT_KEY = '%s/pkey.pem' % DEFAULT_CONFIG_DIR
DEFAULT_SSL_CERT = '%s/cert.pem' % DEFAULT_CONFIG_DIR

VALID_ROLES = set(('__read__', '__write__', '__admin__'))
VALID_GROUPS = set(('admin', ))
VALID_CUBE_ROLE_ACTIONS = set(('pull', 'push'))
IMMUTABLE_DOC_ID_PREFIX = '__'

METRIQUE_DB = 'metrique'
TIMELINE_DB = 'timeline'
ADMIN_DB = 'admin'

ADMIN_USER = 'admin'
DATA_USER = 'metrique'

ETL_ACTIVITY_COLLECTION = 'etl_activity'
JOB_ACTIVITY_COLLECTION = 'job_activity'
AUTH_KEYS_COLLECTION = 'auth_keys'

DATE_FORMAT = '%Y%m%dT%H:%M:%S'
LOG_FORMAT = u'%(processName)s:%(message)s'
LOG_FORMATTER = logging.Formatter(LOG_FORMAT,
                                  DATE_FORMAT)

LOGDIR_SAVEAS = '%s/metriqued.log' % DEFAULT_CONFIG_DIR

DEFAULT_CUBE_QUOTA = -1


def group_is_valid(role):
    if role not in VALID_GROUPS:
        raise HTTPError(400, "Invalid user group. "
                        "Got (%s). Expected: %s" % (role, VALID_GROUPS))


def role_is_valid(role):
    if role not in VALID_ROLES:
        raise HTTPError(400, "Invalid cube role. "
                        "Got (%s). Expected: %s" % (role, VALID_ROLES))


def action_is_valid(action):
    if action not in VALID_CUBE_ROLE_ACTIONS:
        raise HTTPError(400, "Invalid cube role. "
                        "Got (%s). "
                        "Expected: %s" % (action, VALID_CUBE_ROLE_ACTIONS))


class metrique(JSONConf):
    def __init__(self, config_file, force=True, *args, **kwargs):
        if not config_file:
            config_file = DEFAULT_METRIQUE_CONF
        super(metrique, self).__init__(config_file, force=force,
                                       *args, **kwargs)
        self._properties = {}

    @property
    def pid_file(self):
        _pf = os.path.expanduser(DEFAULT_PID_FILE)
        return self._default('pid_file', _pf)

    @property
    def server_thread_count(self):
        from multiprocessing import cpu_count
        return self._default('server_thread_count', cpu_count() * 10)

    @property
    def async(self):
        return self._default('async', True)

    @async.setter
    def async(self, bool_):
        self.config['async'] = bool_

    @property
    def debug(self):
        ''' Reflect whether debug is enabled or not '''
        return self._default('debug', -1)

    @debug.setter
    def debug(self, n):
        logger = logging.getLogger('metrique')
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
    def admin_user(self):
        return self._default('admin_user', ADMIN_USER)

    @property
    def admin_password(self):
        return self._default('admin_password', None)

    @admin_password.setter
    def admin_password(self, value):
        self.config['admin_password'] = value

    @property
    def auth(self):
        return self._default('auth', False)

    @auth.setter
    def auth(self, value):
        self.config['auth'] = value

    @property
    def gzip(self):
        return self._default('gzip', True)

    @gzip.setter
    def gzip(self, value):
        self.config['gzip'] = value

    @property
    def http_host(self):
        return self._default('http_host', DEFAULT_METRIQUE_HTTP_HOST)

    @http_host.setter
    def http_host(self, value):
        self.config['http_host'] = value

    @property
    def http_port(self):
        return self._default('http_port', DEFAULT_METRIQUE_HTTP_PORT)

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
    def krb_realm(self):
        return self._default('krb_realm', '')

    @property
    def login_url(self):
        return self._default('login_url', DEFAULT_METRIQUE_LOGIN_URL)

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
    def ssl_certificate_key(self):
        return self._default(
            'ssl_certificate_key', os.path.expanduser(DEFAULT_SSL_CERT_KEY))

    @ssl_certificate_key.setter
    def ssl_certificate_key(self, value):
        self.config['ssl_certificate_key'] = value

    @property
    def ssl_certificate(self):
        return self._default(
            'ssl_certificate', os.path.expanduser(DEFAULT_SSL_CERT))

    @ssl_certificate.setter
    def ssl_certificate(self, value):
        self.config['ssl_certificate'] = value

    @property
    def log_formatter(self):
        return self._property_default('log_formatter', LOG_FORMATTER)

    @property
    def log_file_path(self):
        return os.path.expanduser(
            self._default('log_file_path', LOGDIR_SAVEAS))


class mongodb(JSONConf):
    def __init__(self, config_file=None, force=True, *args, **kwargs):
        if not config_file:
            config_file = DEFAULT_MONGODB_CONF
        super(mongodb, self).__init__(config_file, force=force,
                                      *args, **kwargs)

    @property
    def host(self):
        return self._default('host', DEFAULT_MONGODB_HOST)

    @host.setter
    def host(self, value):
        self.config['host'] = value

    @property
    def ssl(self):
        return self._default('ssl', False)

    @host.setter
    def host(self, value):
        self.config['host'] = value

    @property
    def admin_password(self):
        return self._default('admin_password', None)

    @admin_password.setter
    def admin_password(self, value):
        self.config['admin_password'] = value

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
    def admin_user(self):
        return self._default('admin_user', ADMIN_USER)

    @property
    def db_admin(self):
        return self._default('db_admin', ADMIN_DB)

    @property
    def db_metrique(self):
        return self._default('db_metrique', METRIQUE_DB)

    @property
    def db_timeline(self):
        return self._default('db_timeline', TIMELINE_DB)

    @property
    def db_metrique_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_metrique,
                           user=self.admin_user,
                           password=self.admin_password,
                           admin_db=self.db_admin,
                           ssl=self.ssl)

    @property
    def db_timeline_admin(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           user=self.admin_user,
                           password=self.admin_password,
                           admin_db=self.db_admin,
                           ssl=self.ssl)

    @property
    def db_timeline_data(self):
        return BaseMongoDB(host=self.host, db=self.db_timeline,
                           user=self.data_user,
                           password=self.data_password,
                           ssl=self.ssl)

    @property
    def collection_etl(self):
        return self._default('collection_etl', ETL_ACTIVITY_COLLECTION)

    @property
    def collection_jobs(self):
        return self._default('collection_jobs', JOB_ACTIVITY_COLLECTION)

    @property
    def collection_auth_keys(self):
        return self._default('collection_auth_keys', AUTH_KEYS_COLLECTION)

    @property
    def c_job_activity(self):
        return self.db_metrique_admin[self.collection_jobs]

    @property
    def c_logs(self):
        return self.db_metrique_admin[self.collection_logs]

    @property
    def c_etl_activity(self):
        return self.db_metrique_admin[self.collection_etl]

    @property
    def c_auth_keys(self):
        return self.db_metrique_admin[self.collection_auth_keys]
