#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from logging import getLogger
logger = getLogger(__name__)

from metrique.server.drivers.basesql import BaseSql
from metrique.server.utils.sql.teiid import TEIID


class BaseTEIID(BaseSql):
    '''
    '''
    def __init__(self, host, db, vdb, port, username, password,
                 *args, **kwargs):
        super(BaseTEIID, self).__init__(host, db, *args, **kwargs)
        self.vdb = vdb
        self.port = port
        self.username = username
        self.password = password

    @property
    def proxy(self):
        self._proxy = Postgresql(vdb=self.vdb, db=self.db, host=self.host,
                                 port=self.port, username=self.username,
                                 password=self.password)
        return self._proxy
