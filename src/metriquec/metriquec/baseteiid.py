#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Base cube for extracting data from SQL TEIID interface
'''

from metriquec.basesql import BaseSql
from metriquec.sql.teiid import TEIID


class BaseTEIID(BaseSql):
    '''
    Driver which adds support for TEIID based sql cubes.
    '''
    def __init__(self, db, sql_host, sql_port, vdb,
                 sql_username, sql_password, **kwargs):

        try:
            from psycopg2 import DatabaseError
        except ImportError:
            raise ImportError("pip install psycopg2")

        self.host = sql_host
        self.db = db
        self.vdb = vdb
        self.port = sql_port
        self.username = sql_username
        self.password = sql_password
        super(BaseTEIID, self).__init__(sql_host=self.host,
                                        sql_port=self.port,
                                        db=self.db, **kwargs)
        self.retry_on_error = DatabaseError

    @property
    def proxy(self):
        self._proxy = TEIID(vdb=self.vdb, db=self.db, host=self.host,
                            port=self.port, username=self.username,
                            password=self.password, logger=self.logger)
        return self._proxy
