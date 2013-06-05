#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging

METRIQUE_CONF = 'metrique_config'
MONGODB_CONF = 'mongodb_config'
SERVER_CONFIG_PATH = '~/.metrique/'
PID_FILE = '~/.metrique/server.pid'

METRIQUE_DB = 'metrique'
WAREHOUSE_DB = 'warehouse'
TIMELINE_DB = 'timeline'
ADMIN_DB = 'admin'

ADMIN_USER = 'admin'
DATA_USER = 'metrique'

ETL_ACTIVITY_COLLECTION = 'etl_activity'
JOB_ACTIVITY_COLLECTION = 'job_activity'

CLIENTS_DB = 'clients'

LOGS_DB = 'logs'
LOGS_COLLECTION = 'logs'
MAX_SIZE = 1000000000
MAX_DOCS = 100000

DATE_FORMAT = '%Y%m%dT%H:%M:%S'
LOG_FORMAT = u'%(processName)s:%(message)s'
LOG_FORMATTER = logging.Formatter(LOG_FORMAT,
                                  DATE_FORMAT)

LOGDIR_SAVEAS = '~/.metrique/logs/metriqued.log'
BACKUP_COUNT = 5
MAX_BYTES = 1.049e+7
