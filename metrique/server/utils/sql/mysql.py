#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

# FIXME: add to deps: mysql
import MySQLdb
from metrique.server.utils.sql.basesql import BaseSql


class Mysql(BaseSql):
    '''
    Connection object for Teiid database
    '''

    raise NotImplementedError("FIXME; config is obsolete")

    def __init__(self, config=None, *args, **kwargs):
        super(Mysql, self).__init__(*args, **kwargs)
        #self.db = config['db']
        #self.host = config['host']
        #self.user = config['user']
        #self.passwd = config['passwd']
        #self.enabled = True

    @property
    def proxy(self):
        if not self.enabled:
            raise RuntimeError("SQL is not enabled/configured")

        if not (hasattr(self, '_proxy')):
            # FIXME: set a timeout??
            self._proxy = MySQLdb.connect(host=self.host, user=self.user,
                                          passwd=self.passwd,
                                          db=self.db,
                                          use_unicode=True)
            logger.debug(' ... Connected (New)')
        else:
            logger.debug(' ... Connected (Cached)')

        return self._proxy
