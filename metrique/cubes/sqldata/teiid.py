#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metrique.cubes.sqldata.teiid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the cube methods for extracting
data from SQL TEIID data sources.
'''
import logging
logger = logging.getLogger(__name__)

try:
    import psycopg2
    from psycopg2.extensions import TRANSACTION_STATUS_UNKNOWN
    from psycopg2.extensions import TRANSACTION_STATUS_INERROR
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from psycopg2 import DatabaseError
    DatabaseError  # avoid 'imported but not used' pyflakes warning
    TRANS_ERROR = [TRANSACTION_STATUS_UNKNOWN, TRANSACTION_STATUS_INERROR]
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    logger.warn("psycopg2 package not found!")
    TRANS_ERROR = []
import re

from metrique.utils import get_cube
sqldata_generic = get_cube('sqldata_generic')

from metrique.sql.teiid import TEIID, DatabaseError


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
        super(Teiid, self).__init__(**kwargs)
        if not HAS_POSTGRES:
            raise ImportError('`pip install psycopg2` required')
        if sql_vdb:
            self.config['sql_vdb'] = sql_vdb
        if sql_username:
            self.config['sql_username'] = sql_username
        if sql_password:
            self.config['sql_password'] = sql_password
        if sql_host:
            self.config['sql_host'] = sql_host
        if sql_port:
            self.config['sql_port'] = sql_port

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

    @property
    def connect_str(self):
        '''
        TEIID (psycopg2) expects a single string with all the required
        arguments included in-line.

        eg, connect_str = "dbname=%s user=%s password=%s host=%s port=%s"
        '''
        connect_str = "dbname=%s user=%s password=%s host=%s port=%s" % (
            self.vdb, self.username, self.password, self.host, self.port)
        logger.debug('TEIID Config: %s' % re.sub(
            'password=[^ ]+', 'password=*****', connect_str))
        return connect_str

    def get_sql_proxy(self, cached=True):
        '''
        Connect to TEIID, using psycopg2; run some teiid
        specific calls to get ready for querying and return
        the proxy.

        Some versions of TEIID do not support 'set' command at all; so
        unless we specify ISOLATION_LEVEL_AUTOCOMMIT (zero), psycopg2
        will send a SET command the teiid server doesn't understand.

        As we're only making reads against the db, we set autocommit=True
        to ensure cursors are properly flushed/closed after read.

        Connections are cached and reused by default and reconnections only
        occur if we encouter some sort of transaction error that indicates
        our connection is some how invalid.
        '''
        err_state = False
        if cached and hasattr(self, '_proxy'):
            trans_status = self._proxy.get_transaction_status()
            if trans_status in TRANS_ERROR:
                err_state = True
                logger.error('Transaction Error: %s' % trans_status)
            elif self._proxy.closed == 1:
                err_state = True
                logger.error('Connection Error: CLOSED')
        else:
            err_state = True

        if err_state or not (cached and hasattr(self, '_proxy')):
            proxy = psycopg2.connect(self.connect_str)
            logger.debug(' ... Connected (New)')
            # Teiid does not support 'set' command at all; so unless we
            # specify ISOLATION_LEVEL_AUTOCOMMIT (zero), psycopg2 will send a
            # SET command the teiid server doesn't understand.
            proxy.autocommit = True
            try:
                proxy.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            except Exception:
                # This only seems to be necessary on early versions of
                # psycopg2 though; ignore exception if it occurs
                pass
            finally:
                self._proxy = proxy
        return self._proxy
