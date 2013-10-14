#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import os

from metriqueu.jsonconf import JSONConf
from metriqued.basemongodb import BaseMongoDB
from metriqued.utils import new_cookie_secret

CONFIG_DIR = '~/.metrique'

PID_FILE = os.path.join(CONFIG_DIR, 'server.pid')
SSL_CERT_FILE = os.path.join(CONFIG_DIR, 'cert.pem')
SSL_KEY_FILE = os.path.join(CONFIG_DIR, 'pkey.pem')

ADMIN_USER = 'admin'

DATE_FORMAT = '%Y%m%dT%H:%M:%S'
LOG_FORMAT = u'%(processName)s:%(message)s'
LOG_FORMATTER = logging.Formatter(LOG_FORMAT,
                                  DATE_FORMAT)

CUBE_QUOTA = None


class metrique(JSONConf):

    def __init__(self, config_file=None):
        self.default_config = os.path.join(CONFIG_DIR, 'metrique_config')
        self.defaults = {
            'async': True,
            'autoreload': False,
            'gzip': True,
            'host': '127.0.0.1',
            'krb_auth': False,
            'log_formatter': LOG_FORMATTER,
            'logfile': None,
            'login_url': '/login',
            'max_processes': 0,
            'port': 5420,
            'realm': 'metrique',
            'ssl': False,
            'xsrf_cookies': False,
        }
        super(metrique, self).__init__(config_file=config_file)

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
        return self._default('debug', False)

    @debug.setter
    def debug(self, value):
        ''' Update logger settings '''
        if isinstance(value, (tuple, list)):
            logger, value = value
            self._debug_set(value, logger)
        else:
            try:
                logger = self.logger
            except AttributeError:
                self._debug_set(value)
            else:
                self._debug_set(value, logger)
        self.config['debug'] = value

    def _debug_set(self, level, logger=None):
        '''
        if we get a level of 2, we want to apply the
        debug level to all loggers
        '''
        if not logger or level == 2:
            logger = logging.getLogger()

        if self.logfile:
            logfile = os.path.expanduser(self.logfile)
            for hdlr in logger.handlers:
                logger.removeHandler(hdlr)
            logger.removeHandler
            hdlr = logging.FileHandler(logfile)
            logger.addHandler(hdlr)

        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)

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

    def __init__(self, config_file=None, *args, **kwargs):
        self.default_config = os.path.join(CONFIG_DIR, 'mongodb_config')
        self.defaults = {
            'auth': False,
            'admin_password': None,
            'admin_user': ADMIN_USER,
            'data_password': None,
            'data_user': 'metrique',
            'db_metrique': 'metrique',
            'db_timeline': 'timeline',
            'collection_cube_profile': 'cube_profile',
            'collection_user_profile': 'user_profile',
            'host': '127.0.0.1',
            'port': 27017,
            'ssl': False,
            'write_concern': 1,
        }
        super(mongodb, self).__init__(config_file, *args, **kwargs)

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
