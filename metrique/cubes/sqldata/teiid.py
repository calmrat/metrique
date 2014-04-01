#!/usr/bin/env pyehon
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.cubes.sqldata.teiid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the cube methods for extracting
data from SQL TEIID data sources.
'''

from __future__ import unicode_literals

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


class Teiid(sqldata_generic):
    '''
    Driver which adds support for TEIID based sql cubes.

    :param sql_vdb: teiid virtual database name

    Also expects arguments as defined in `sqldata_generic` class
    '''
    def __init__(self, sql_vdb=None, *args, **kwargs):
        super(Teiid, self).__init__(*args, **kwargs)
        if not HAS_POSTGRES:
            raise ImportError('`pip install psycopg2` required')
        options = dict(vdb=sql_vdb)
        defaults = dict(vdb=None)
        self.configure('sql', options, defaults)
        self.retry_on_error = DatabaseError

    @property
    def sql_proxy(self):
        '''
        Connect, authenticate and cache TEIID db connection and return it
        to caller.
        '''
        if not hasattr(self, '_teiid'):
            for arg in ('vdb', 'host', 'port',
                        'username', 'password'):
                if arg not in self.config['sql']:
                    raise RuntimeError("%s argument is not set!" % arg)
            self._teiid = self.get_sql_proxy()
        return self._teiid

    @property
    def connect_str(self):
        '''
        TEIID (psycopg2) expects a single string with all the required
        arguments included in-line.

        eg, connect_str = "dbname=%s user=%s password=%s host=%s port=%s"
        '''
        vdb = self.config['sql'].get('vdb')
        username = self.config['sql'].get('username')
        password = self.config['sql'].get('password')
        host = self.config['sql'].get('host')
        port = self.config['sql'].get('port')
        connect_str = "dbname=%s user=%s password=%s host=%s port=%s" % (
            vdb, username, password, host, port)
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
        if cached and hasattr(self, '_sql_proxy'):
            trans_status = self._sql_proxy.get_transaction_status()
            if trans_status in TRANS_ERROR:
                err_state = True
                logger.error('Transaction Error: %s' % trans_status)
            elif self._sql_proxy.closed == 1:
                err_state = True
                logger.error('Connection Error: CLOSED')
        else:
            err_state = True

        if err_state or not (cached and hasattr(self, '_sql_proxy')):
            proxy = psycopg2.connect(self.connect_str)
            logger.debug(' ... TEIID Connected (New)')
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
                self._sql_proxy = proxy
        return self._sql_proxy
