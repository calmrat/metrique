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
     * preparing select statements for extraction of object
       column/field values
    '''

    @property
    def proxy(self):
        raise NotImplementedError("Driver has not provided a proxy method!")

    def cursor(self):
        return self.proxy.cursor()

    def _validate_row_limit(self, row_limit):
        # max number of rows to return per call (ie, LIMIT)
        try:
            row_limit = int(row_limit)
        except (TypeError, ValueError):
            raise ValueError(
                "row_limit must be a number. Got (%s)" % row_limit)
        return row_limit

    # FIXME: why not use row_limit as a argument
    # to fetchall()...
    # set hard = True to add the SQL LIMIT clause,
    # otherwise, don't add the hard limit, but only
    # fetch/skip in fetch() instead?
    def fetchall(self, sql, row_limit=0, start=0):
        '''
        Shortcut for getting a cursor, cleaning the sql a bit,
        adding the LIMIT clause, executing the sql, fetching
        all the results
        '''
        self._validate_row_limit(row_limit)

        with self.cursor() as k:
            sql = re.sub('\s+', ' ', sql).strip().encode('utf-8')

            if row_limit > 0:
                sql = re.sub('$', ' LIMIT %i,%i' % (start, row_limit), sql)

            logger.debug('UPDATED SQL:\n %s' % sql.decode('utf-8'))

            rows = None

            k.execute(sql)
            rows = k.fetchall()
        return rows
