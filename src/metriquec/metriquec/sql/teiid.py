#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriquec.sql.teiid
~~~~~~~~~~~~~~~~~~~

This module contains a convenience wrapper for connecting
to TEIID databases using postgres (psycopg2).
'''

import logging
import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_UNKNOWN
from psycopg2.extensions import TRANSACTION_STATUS_INERROR
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re

from metriquec.sql.basesql import BaseSql

logger = logging.getLogger(__name__)

TRANS_ERROR = [TRANSACTION_STATUS_UNKNOWN, TRANSACTION_STATUS_INERROR]


class TEIID(BaseSql):
    '''
    Connection object for Teiid database. Uses psycopg2 postgres.

    Only difference from metriquec.sql.basesql is self.vdb and connection
    method. To connect,
    '''
    def __init__(self, vdb, host, username, password, port, **kwargs):
        super(TEIID, self).__init__()
        self.vdb = vdb
        self.host = host
        self.username = username
        self.password = password
        self.port = port

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

    def get_proxy(self, cached=True):
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
