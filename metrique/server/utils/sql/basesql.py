#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import re


class BaseSql(object):
    '''
    Metrique SQL obj; contains helper methods for
     * executing SQL
     * preparing simple select statements
     * Automated caching
    '''

    def __init__(self):
        self.enabled = True

    @property
    def proxy(self):
        raise NotImplementedError("Driver has not provided a proxy method!")

    @property
    def cursor(self):
        return self.proxy.cursor()

        raise NotImplementedError("Driver has not yet provided a configure moethod!")

    def reconnect(self):
        # force the new connection by deleting the exiting one
        del self._proxy

    def fetchall(self, sql, row_limit, start=0):
        '''
        '''
        logger.debug('Getting new sql cursor')
        cursor = self.proxy.cursor()
        logger.debug('... got a cursor!')

        sql = re.sub('\s+', ' ', sql).strip().encode('utf-8')

        if row_limit > 0:
            sql = re.sub('$', ' LIMIT %i,%i' % (start, row_limit), sql)

        logger.debug('UPDATED SQL:\n %s' % sql.decode('utf-8'))
        try:
            logger.debug("Using existing connection")
            cursor.execute(sql)
        except Exception as e:
            #####
            # FIXME: we need each subclass to be able to catch specific
            # errors.... if there is a Programming Error for example...
            # no need to reconnect, we know there's a syntax error
            #####
            # let's try to reconnect and run it again
            logger.warn('Got an exception: %s' % e)
            logger.warn('Trying to reconnect and execute(sql) again')
            self.reconnect()
            logger.debug('Getting new sql cursor')
            cursor = self.proxy.cursor()
            logger.debug('... got a cursor!')
            logger.debug("Rerunning execute(sql)")
            cursor.execute(sql)

        rows = cursor.fetchall()
        cursor.close()  # clean-up a little
        return rows
