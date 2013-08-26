#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
import os

from defaults import LOGDIR_SAVEAS
from defaults import ADMIN_DB, METRIQUE_DB
from defaults import JOB_ACTIVITY_COLLECTION
from defaults import LOGS_COLLECTION, AUTH_KEYS_COLLECTION
from defaults import LOG_FORMATTER
from defaults import ETL_ACTIVITY_COLLECTION, MAX_SIZE, MAX_DOCS
from defaults import DATA_USER, ADMIN_USER, TIMELINE_DB
from defaults import PID_FILE
from defaults import SSL_CERT, SSL_CERT_KEY
from defaults import MONGODB_CONF
from defaults import DEFAULT_CONFIG_DIR
from defaults import MONGODB_HOST
from defaults import METRIQUE_HTTP_HOST
from defaults import METRIQUE_HTTP_PORT

from jsonconf import JSONConf

from metrique.server.mongodb.basemongodb import BaseMongoDB


class metrique(JSONConf):
    def __init__(self, config_file, config_dir=None,
                 force=True, *args, **kwargs):
        if not config_dir:
            config_dir = DEFAULT_CONFIG_DIR
        super(metrique, self).__init__(config_file, config_dir,
                                       force=force, *args, **kwargs)
        self._properties = {}

    @property
    def pid_file(self):
        _pf = os.path.expanduser(PID_FILE)
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
    def krb_realm(self):
        return self._default('krb_realm', '')

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
            'ssl_certificate_key', os.path.expanduser(SSL_CERT_KEY))

    @ssl_certificate_key.setter
    def ssl_certificate_key(self, value):
        self.config['ssl_certificate_key'] = value

    @property
    def ssl_certificate(self):
        return self._default('ssl_certificate', os.path.expanduser(SSL_CERT))

    @ssl_certificate.setter
    def ssl_certificate(self, value):
        self.config['ssl_certificate'] = value

    @property
    def logs_max_size(self):
        return self._default('logs_max_size', MAX_SIZE)

    @property
    def logs_max_docs(self):
        return self._default('logs_max_docs', MAX_DOCS)

    @property
    def log_formatter(self):
        return self._property_default('log_formatter', LOG_FORMATTER)

    @property
    def log_to_stdout(self):
        return self._default('log_to_stdout', 1)

    @property
    def log_file_path(self):
        return os.path.expanduser(
            self._default('log_file_path', LOGDIR_SAVEAS))

    @log_file_path.setter
    def log_file_path(self, value):
        self.config['log_file_path'] = value

    @property
    def log_to_file(self):
        return self._default('log_to_file', 0)


class mongodb(JSONConf):
    def __init__(self, config_file=MONGODB_CONF, config_dir=None,
                 force=True, *args, **kwargs):
        super(mongodb, self).__init__(config_file, config_dir,
                                      force=force, *args, **kwargs)

    @property
    def host(self):
        return self._default('host', MONGODB_HOST)

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
    def collection_logs(self):
        return self._default('collection_logs', LOGS_COLLECTION)

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
