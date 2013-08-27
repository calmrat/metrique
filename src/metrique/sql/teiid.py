#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import psycopg2
import re
from metrique.sql.basesql import BaseSql


class TEIID(BaseSql):
    '''
    Connection object for Teiid database. Uses psycopg2 postgres.

    Only difference from basesql is self.vdb and connection
    method. To connect, TEIID (psycopg2) expects a single string with
    all the required arguments included.
    '''
    def __init__(self, vdb, db, host, username, password, port,
                 **kwargs):
        super(TEIID, self).__init__(**kwargs)
        self.vdb = vdb
        self.db = db
        self.host = host
        self.username = username
        self.password = password
        self.port = port

    @property
    def connect_str(self):
        return "dbname=%s user=%s password=%s host=%s port=%s" % (
            self.vdb, self.username, self.password, self.host, self.port)

    @property
    def proxy(self):
        '''
        Connect to TEIID, using psycopg2; run some teiid
        specific calls to get ready for querying and return
        the proxy.
        '''
        self.logger.debug(
            ' TEIID Config: %s' % re.sub(
                'password=[^ ]+', 'password=*****', self.connect_str))

        if not (hasattr(self, '_proxy') and self._proxy.status):
            # FIXME: set a timeout??
            self._proxy = psycopg2.connect(self.connect_str)
            self.logger.debug(' ... Connected (New)')
            # Teiid does not support setting this value at all and unless we
            # specify ISOLATION_LEVEL_AUTOCOMMIT (zero), psycopg2 will send a
            # SET command the teiid server doesn't understand.
            try:
                self._proxy.set_isolation_level(0)
            except Exception:
                # This only seems to be necessary on early versions of
                # psycopg2 though
                # So in the case that we hit an exception, just ignore them.
                pass
        else:
            self.logger.debug(' ... Connected (Cached)')
        return self._proxy
