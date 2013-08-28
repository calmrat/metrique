#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging

DEFAULT_CONFIG_DIR = '~/.metrique'
MONGODB_HOST = '127.0.0.1'
METRIQUE_HTTP_HOST = '127.0.0.1'
METRIQUE_HTTP_PORT = 8080

METRIQUE_CONF = 'metrique_config'
MONGODB_CONF = 'mongodb_config'
SERVER_CONFIG_PATH = DEFAULT_CONFIG_DIR
PID_FILE = '%s/server.pid' % DEFAULT_CONFIG_DIR
SSL_CERT_KEY = '%s/.metrique/pkey.pem' % DEFAULT_CONFIG_DIR
SSL_CERT = '%s/.metrique/cert.pem' % DEFAULT_CONFIG_DIR

VALID_PERMISSIONS = (None, 'r', 'rw', 'admin')

METRIQUE_DB = 'metrique'
TIMELINE_DB = 'timeline'
ADMIN_DB = 'admin'

ADMIN_USER = 'admin'
DATA_USER = 'metrique'

ETL_ACTIVITY_COLLECTION = 'etl_activity'
JOB_ACTIVITY_COLLECTION = 'job_activity'
AUTH_KEYS_COLLECTION = 'auth_keys'

CLIENTS_DB = 'clients'

LOGS_DB = 'logs'
LOGS_COLLECTION = 'logs'
MAX_SIZE = 1000000000
MAX_DOCS = 100000

DATE_FORMAT = '%Y%m%dT%H:%M:%S'
LOG_FORMAT = u'%(processName)s:%(message)s'
LOG_FORMATTER = logging.Formatter(LOG_FORMAT,
                                  DATE_FORMAT)

LOGDIR_SAVEAS = '%s/logs/metriqued.log' % DEFAULT_CONFIG_DIR
BACKUP_COUNT = 5
MAX_BYTES = 1.049e+7
