#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriquec.sql.basesql
~~~~~~~~~~~~~~~~~~~~~

This module contains a generic, reuable SQL connection
and authentication wrapper for connecting to databases.
'''

import logging
import re

logger = logging.getLogger(__name__)


class BaseSql(object):
    '''
    Baseclass for SQL connectors; contains generic methods for
     * consistently executing SQL
     * preparing select statements for extraction of object
       column/field values

    It is expected that database specific drivers will subclass
    this class to add connection and authentication methods
    required to obtain a cursor for querying.
    '''
    def __init__(self):
        self._auto_reconnect_attempted = False

    def get_proxy(self, **kwargs):
        '''
        Database specific drivers must implemented this method.

        It is expected that by calling this method, the instance
        will set ._proxy with a auhenticated connection, which is
        also returned to the caller.
        '''
        raise NotImplementedError(
            "Driver has not provided a get_proxy method!")

    def fetchall(self, sql, cached=True):
        '''
        Shortcut for getting a cursor, cleaning the sql a bit,
        adding the LIMIT clause, executing the sql, fetching
        all the results

        If certain failures occur, this method will authomatically
        attempt to reconnect and rerun.

        :param sql: sql string to execute
        :param cached: flag for using a chaced proxy or not
        '''
        logger.debug('Fetching rows...')
        proxy = self.get_proxy(cached=cached)
        k = proxy.cursor()
        sql = re.sub('\s+', ' ', sql).strip().encode('utf-8')
        logger.debug('SQL:\n %s' % sql.decode('utf-8'))
        rows = None
        try:
            k.execute(sql)
            rows = k.fetchall()
        except Exception as e:
            if re.search('Transaction is not active', str(e)):
                if not self._auto_reconnect_attempted:
                    logger.warn('Transaction failure; reconnecting')
                    self.fetchall(sql, cached=False)
            logger.error('%s\n%s\n%s' % ('*' * 100, e, sql))
            raise
        else:
            if self._auto_reconnect_attempted:
                # in the case we've attempted to reconnect and
                # the transaction succeeded, reset this flag
                self._auto_reconnect_attempted = False
        finally:
            k.close()
            del k
        logger.debug('... fetched (%i)' % len(rows))
        return rows
