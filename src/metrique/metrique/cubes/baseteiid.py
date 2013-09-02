#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Base cube for extracting data from SQL TEIID interface
'''

from psycopg2 import DatabaseError
from metrique.cubes.basesql import BaseSql
from metrique.sql.teiid import TEIID

DEFAULT_CONFIG = {
    "host":         "",
    "port":         "",
    "vdb":          "",
    "username":     "",
    "password":     "",
    "row_limit":    300000
}


class BaseTEIID(BaseSql):
    '''
    Driver which adds support for TEIID based sql cubes.
    '''
    def __init__(self, db, host=None, vdb=None, port=None,
                 username=None, password=None, **kwargs):
        self.host = self.setdefault(host, DEFAULT_CONFIG['host'])
        self.vdb = self.setdefault(vdb, DEFAULT_CONFIG['vdb'])
        self.port = self.setdefault(port, DEFAULT_CONFIG['port'])
        self.username = self.setdefault(username, DEFAULT_CONFIG['username'])
        self.password = self.setdefault(password, DEFAULT_CONFIG['password'])
        super(BaseTEIID, self).__init__(host=self.host, port=self.port,
                                        db=self.db, **kwargs)
        self.retry_on_error = DatabaseError

    @property
    def proxy(self):
        self._proxy = TEIID(vdb=self.vdb, db=self.db, host=self.host,
                            port=self.port, username=self.username,
                            password=self.password, logger=self.logger)
        return self._proxy
