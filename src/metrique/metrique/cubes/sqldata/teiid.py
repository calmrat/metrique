#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metrique.cubes.sqldata.teiid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the cube methods for extracting
data from SQL TEIID data sources.
'''
import logging

from metrique.utils import get_cube
sqldata_generic = get_cube('sqldata_generic')

from metrique.sql.teiid import TEIID

logger = logging.getLogger(__name__)


class Teiid(sqldata_generic):
    '''
    Driver which adds support for TEIID based sql cubes.

    :param sql_host: teiid hostname
    :param sql_port: teiid port
    :param sql_vdb: teiid virtual database name
    :param sql_username: teiid username
    :param sql_password: teiid password
    '''
    def __init__(self, sql_host=None, sql_port=None, sql_vdb=None,
                 sql_username=None, sql_password=None, **kwargs):

        super(Teiid, self).__init__(sql_host=sql_host,
                                    sql_port=sql_port,
                                    **kwargs)
        try:
            from psycopg2 import DatabaseError
        except ImportError:
            raise ImportError("pip install psycopg2")

        if sql_vdb:
            self.config['sql_vdb'] = sql_vdb
        if sql_username:
            self.config['sql_username'] = sql_username
        if sql_password:
            self.config['sql_password'] = sql_password

        self.retry_on_error = DatabaseError
        logger.debug("New TEIID proxy initialized")

    @property
    def sql_proxy(self):
        '''
        Connect, authenticate and cache TEIID db connection and return it
        to caller.
        '''
        if not hasattr(self, '_teiid'):
            for arg in ('sql_vdb', 'sql_host', 'sql_port',
                        'sql_username', 'sql_password'):
                if arg not in self.config:
                    raise RuntimeError("%s argument is not set!" % arg)
            self._teiid = TEIID(vdb=self.config['sql_vdb'],
                                host=self.config['sql_host'],
                                port=self.config['sql_port'],
                                username=self.config['sql_username'],
                                password=self.config['sql_password'])
        return self._teiid
