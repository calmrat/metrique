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
    def __init__(self, sql_db, sql_host, sql_port, sql_vdb,
                 sql_username, sql_password, **kwargs):

        try:
            from psycopg2 import DatabaseError
        except ImportError:
            raise ImportError("pip install psycopg2")

        super(BaseTEIID, self).__init__(sql_host=sql_host,
                                        sql_port=sql_port,
                                        sql_db=sql_db,
                                        **kwargs)
        self.config['sql_vdb'] = sql_vdb
        self.config['sql_username'] = sql_username
        self.config['sql_password'] = sql_password
        self.retry_on_error = DatabaseError

    @property
    def proxy(self):
        if not hasattr(self, '_teiid'):
            self.logger.debug("TEIID proxy (NEW)")
            self._teiid = TEIID(vdb=self.config['sql_vdb'],
                                db=self.config['sql_db'],
                                host=self.config['sql_host'],
                                port=self.config['sql_port'],
                                username=self.config['sql_username'],
                                password=self.config['sql_password'],
                                logger=self.logger)
        return self._teiid
