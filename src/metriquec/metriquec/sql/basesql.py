#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
import re


class BaseSql(object):
    '''
    Metrique SQL obj; contains helper methods for
     * executing SQL
     * preparing select statements for extraction of object
       column/field values
    '''

    def __init__(self, logger=None):
        if not logger:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._auto_reconnect_attempted = False

    def get_proxy(self, **kwargs):
        raise NotImplementedError(
            "Driver has not provided a get_proxy method!")

    def fetchall(self, sql, cached=True):
        '''
        Shortcut for getting a cursor, cleaning the sql a bit,
        adding the LIMIT clause, executing the sql, fetching
        all the results
        '''
        self.logger.debug('Fetching rows...')
        proxy = self.get_proxy(cached=cached)
        k = proxy.cursor()
        sql = re.sub('\s+', ' ', sql).strip().encode('utf-8')
        self.logger.info('SQL:\n %s' % sql.decode('utf-8'))
        rows = None
        try:
            k.execute(sql)
            rows = k.fetchall()
        except Exception as e:
            if re.search('Transaction is not active', str(e)):
                if not self._auto_reconnect_attempted:
                    self.logger.error('Transaction failure; reconnecting')
                    self.fetchall(sql, cached=False)
            self.logger.error('%s\n%s\n%s' % ('*' * 100, e, sql))
            raise
        else:
            if self._auto_reconnect_attempted:
                # in the case we've attempted to reconnect and
                # the transaction succeeded, reset this flag
                self._auto_reconnect_attempted = False
        finally:
            k.close()
            del k
        self.logger.debug('... fetched (%i)' % len(rows))
        return rows
