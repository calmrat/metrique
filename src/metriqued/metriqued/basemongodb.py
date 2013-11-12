#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
try:
    from pymongo import MongoClient
    mongo_client_support = True
except ImportError:
    from pymongo import Connection
    mongo_client_support = False
from pymongo.errors import ConnectionFailure


class BaseMongoDB(object):
    def __init__(self, db, host, user=None, password=None, auth=False,
                 port=None, ssl=None, ssl_keyfile=None,
                 ssl_certfile=None, write_concern=0):

        self.auth = auth
        self.host = host
        self.user = user
        self.password = password
        self._db = db

        self.ssl = ssl
        self.ssl_keyfile = os.path.expanduser(ssl_keyfile)
        self.ssl_certfile = os.path.expanduser(ssl_certfile)
        self.port = port
        self.write_concern = write_concern

    def _auth_db(self):
        '''
        by default the default user only has read-only access to db
        '''
        if not self.auth:
            pass
        elif not self.password:
            raise ValueError("no mongo authentication password provided")
        else:
            admin_db = self._proxy['admin']
            if not admin_db.authenticate(self.user, self.password):
                raise RuntimeError(
                    "MongoDB failed to authenticate user (%s)" % self.user)
        return self._proxy[self._db]

    def close(self):
        if hasattr(self, '_proxy'):
            self._proxy.close()

    @property
    def db(self):
        # return the connected, authenticated database object
        return self._load_db_proxy()

    def __getitem__(self, collection):
        return self.db[collection]

    def _load_mongo_client(self, **kwargs):
        self._proxy = MongoClient(self.host, self.port,
                                  tz_aware=True,
                                  w=self.write_concern,
                                  **kwargs)

    def _load_mongo_connection(self, **kwargs):
            self._proxy = Connection(self.host, self.port,
                                     ssl=self.ssl,
                                     ssl_keyfile=self.ssl_keyfile,
                                     ssl_certfile=self.ssl_certfile,
                                     tz_aware=True,
                                     w=self.write_concern,
                                     **kwargs)

    def _load_db_proxy(self):
        if not hasattr(self, '_db_proxy'):
            kwargs = {}
            if self.ssl:
                # include ssl options only if it's enabled
                kwargs.update(dict(ssl=self.ssl,
                                   ssl_keyfile=self.ssl_keyfile,
                                   ssl_certfile=self.ssl_certfile))
            try:
                if mongo_client_support:
                    self._load_mongo_client(**kwargs)
                else:
                    self._load_mongo_connection(**kwargs)
            except ConnectionFailure as e:
                raise ConnectionFailure(
                    "MongoDB Failed to connect (%s): %s" % (self.host, e))
            else:
                self._db_proxy = self._auth_db()
        return self._db_proxy

    def set_collection(self, collection):
        self.collection = collection
