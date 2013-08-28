#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from pymongo import Connection, errors

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 27017
DEFAULT_SSL = False
DEFAULT_TIMEOUT = None
DEFAULT_WRITE_CONCERN = 1


class BaseMongoDB(object):
    def __init__(self, db, host=DEFAULT_HOST, user=None, password=None,
                 admin_db=None, ssl=DEFAULT_SSL, port=DEFAULT_PORT,
                 timeout=DEFAULT_TIMEOUT,
                 write_concern=DEFAULT_WRITE_CONCERN):
        self._host = host
        self._password = password
        self._user = user
        self._db = db
        self._admin_db = admin_db

        self._ssl = ssl
        self._port = port
        self._timeout = timeout
        self._write_concern = write_concern

    @property
    def db(self):
        if not hasattr(self, '_db_proxy'):
            try:
                self._proxy = Connection(self._host, self._port,
                                         ssl=self._ssl,
                                         tz_aware=True,
                                         network_timeout=self._timeout)
            except errors.ConnectionFailure as e:
                raise errors.ConnectionFailure(
                    "MongoDB Failed to connect (%s): %s" % (self._host, e))
            self._db_proxy = self._auth_db()
        # return the connected, authenticated database object
        return self._db_proxy

    def close(self):
        if hasattr(self, '_proxy'):
            self._proxy.close()

    def _auth_db(self):
        '''
        by default the default user only has read-only access to db
        '''
        if not self._password:
            pass
        elif self._admin_db:
            admin_db = self._proxy[self._admin_db]
            if not admin_db.authenticate(self._user, self._password):
                raise RuntimeError(
                    "MongoDB failed to authenticate user (%s)" % self._user)
        else:
            if not self._proxy[self._db].authenticate(self._user,
                                                      self._password):
                raise RuntimeError(
                    "MongoDB failed to authenticate user (%s)" % self._user)
        return self._proxy[self._db]

    def set_collection(self, collection):
        self.collection = collection

    def __getitem__(self, collection):
        return self.db[collection]
