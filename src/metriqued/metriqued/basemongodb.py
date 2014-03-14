#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metriqued.basemongodb
~~~~~~~~~~~~~~~~~~~~~

This module contains a convenience wrapper around PyMongo,
providing methods for simple connection and authentication
workflows and default configuration and setup.
'''

import logging
import os
try:
    from pymongo import MongoClient, MongoReplicaSetClient
except ImportError:
    raise ImportError("Pymongo 2.6+ required!")
#from pymongo.errors import ConnectionFailure
from pymongo.read_preferences import ReadPreference

from metriqueu.jsonconf import JSONConf

logger = logging.getLogger(__name__)

# if HOME environment variable is set, use that
# useful when running 'as user' with root (supervisord)
HOME = os.environ.get('HOME')
if HOME:
    USER_DIR = os.path.join(HOME, '.metrique')
else:
    USER_DIR = os.path.expanduser('~/.metrique')
ETC_DIR = os.path.join(USER_DIR, 'etc')

MONGODB_CONFIG = os.path.join(ETC_DIR, 'mongodb.json')

# FIXME: create the in mongodb firstboot!
# FIXME: make 'firstboot' a method of this class
SSL_CERT = os.path.join(ETC_DIR, 'metrique.cert')
SSL_KEY = os.path.join(ETC_DIR, 'metrique.key')
SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

READ_PREFERENCE = {
    'PRIMARY_PREFERRED': ReadPreference.PRIMARY,
    'PRIMARY': ReadPreference.PRIMARY,
    'SECONDARY': ReadPreference.SECONDARY,
    'SECONDARY_PREFERRED': ReadPreference.SECONDARY_PREFERRED,
    'NEAREST': ReadPreference.NEAREST,
}


class MongoDBConfig(JSONConf):
    '''
    mongodb default config class.

    This configuration class defines the following overrideable defaults.

    :param auth: enable mongodb authentication
    :param password: admin user password
    :param user: admin username
    :param fsync: sync writes to disk before return?
    :param host: mongodb host(s) to connect to
    :param journal: enable write journal before return?
    :param port: mongodb port to connect to
    :param read_preference: default - NEAREST
    :param replica_set: name of replica set, if any
    :param ssl: enable ssl
    :param ssl_certificate: path to ssl certificate file
    :param ssl_certificate_key: path to ssl certificate key file
    :param tz_aware: return back tz_aware dates?
    :param write_concern: what level of write assurance before returning
    '''
    default_config = MONGODB_CONFIG
    name = 'mongodb'

    # FIXME: move metrique specific configs back to metriqued.config
    # eg, 'collection logs'... that's it. and below
    #@property
    #def c_logs_admin(self):
    #    '''Wrapper for a read/write 'logs' collection proxy'''

    def __init__(self, config_file=None, **kwargs):
        config = {
            'auth': False,
            'autoconnect': True,
            'password': None,
            'user': None,
            'collection_logs': 'logs',
            'fsync': False,
            'host': '127.0.0.1',
            'journal': True,
            'port': 27017,
            'read_preference': 'NEAREST',
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
        super(MongoDBConfig, self).__init__(config_file=config_file)
        # anything passed in explicitly gets precedence
        self.config.update(kwargs)


class MongoDBClient(object):
    '''
    Generic wrapper for MongoDB connection configuration and handling.

    Connections are cached and reused, until destruction or connection
    error is encountered.

    Requires Mongodb 2.4+ and pymongo 2.6+!
    '''
    def __init__(self, config_file=None, **kwargs):
        self.set_config(config_file, **kwargs)

    def alive(self):
        try:
            return self._proxy.alive()
        except AttributeError:
            return False

    def close(self):
        '''Close the existing cached mongodb proxy connection'''
        try:
            self._proxy.close()
        except AttributeError:
            pass
        finally:
            if hasattr(self, '_proxy'):
                del self._proxy

    @property
    def proxy(self):
        if not (hasattr(self, '_proxy') and self._proxy):
            if self.c.autoconnect:
                return self.get_proxy(force=True)
            else:
                raise RuntimeError("Not connected to any mongodb instance!")
        else:
            return self._proxy

    def set_config(self, config_file, **kwargs):
        self.c = self.config = MongoDBConfig(config_file, **kwargs)

    def get_proxy(self, force=False):
        if force or not (hasattr(self, '_proxy') and self._proxy):
            kwargs = {}
            if self.c.ssl:
                if self.c.ssl_keyfile:
                    # include ssl keyfile only if its defined
                    # otherwise, certfile must be crt+key pem combined
                    kwargs.update({'ssl_keyfile': self.c.ssl_keyfile})

                # include ssl options only if it's enabled
                kwargs.update(dict(ssl=self.c.ssl,
                                   ssl_certfile=self.c.ssl_certfile))
            if self.c.replica_set:
                self._load_mongo_replica_client(**kwargs)
            else:
                self._load_mongo_client(**kwargs)
            _proxy = self._auth_db()
        else:
            _proxy = self._proxy
        return _proxy

    def _auth_db(self):
        if self.c.auth:
            if not self['admin'].authenticate(self.c.user, self.c.password):
                raise RuntimeError(
                    "MongoDB failed to authenticate user (%s)" % self.c.user)
        return self._proxy

    def __getitem__(self, key):
        return self.proxy[key]

    def __getattr__(self, attr):
        return getattr(self.proxy, attr)

    def _load_mongo_client(self, **kwargs):
        logger.debug('Loading new MongoClient connection')
        self._proxy = MongoClient(
            self.c.host, self.c.port, tz_aware=self.c.tz_aware,
            w=self.c.write_concern, j=self.c.journal, fsync=self.c.fsync,
            **kwargs)
        return self._proxy

    def _load_mongo_replica_client(self, **kwargs):
        logger.debug('Loading new MongoReplicaSetClient connection')
        read_preference = READ_PREFERENCE[self.c.read_preference]
        self._proxy = MongoReplicaSetClient(
            self.c.host, self.c.port, tz_aware=self.c.tz_aware,
            w=self.c.write_concern, j=self.c.journal,
            fsync=self.c.fsync, replicaSet=self.c.replica_set,
            read_preference=read_preference, **kwargs)
        return self._proxy

    #    return self.db_metrique_admin[self.collection_logs]
