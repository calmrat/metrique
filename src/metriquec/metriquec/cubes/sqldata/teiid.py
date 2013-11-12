#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from metrique.utils import get_cube
sqldata_generic = get_cube('sqldata_generic')

from metriquec.sql.teiid import TEIID


class Teiid(sqldata_generic):
    '''
    Driver which adds support for TEIID based sql cubes.
    '''
    def __init__(self, sql_host=None, sql_port=None, sql_vdb=None,
                 sql_username=None, sql_password=None, **kwargs):
        try:
            from psycopg2 import DatabaseError
        except ImportError:
            raise ImportError("pip install psycopg2")

        super(Teiid, self).__init__(sql_host=sql_host,
                                    sql_port=sql_port,
                                    **kwargs)
        if sql_vdb:
            self.config['sql_vdb'] = sql_vdb
        if sql_username:
            self.config['sql_username'] = sql_username
        if sql_password:
            self.config['sql_password'] = sql_password

        self.retry_on_error = DatabaseError

    @property
    def proxy(self):
        if not hasattr(self, '_teiid'):
            for arg in ('sql_vdb', 'sql_host', 'sql_port',
                        'sql_username', 'sql_password'):
                if arg not in self.config:
                    raise RuntimeError("%s argument is not set!" % arg)

            self.logger.debug("TEIID proxy (NEW)")
            self._teiid = TEIID(vdb=self.config['sql_vdb'],
                                host=self.config['sql_host'],
                                port=self.config['sql_port'],
                                username=self.config['sql_username'],
                                password=self.config['sql_password'],
                                logger=self.logger)
        return self._teiid
