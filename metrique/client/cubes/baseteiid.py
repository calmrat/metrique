#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from logging import getLogger
logger = getLogger(__name__)

from metrique.client.cubes.basesql import BaseSql
from metrique.tools.sql.teiid import TEIID

DEFAULT_CONFIG = {
    "host":         "",
    "port":         "",
    "vdb":          "",
    "username":     "",
    "password":     "",
    "row_limit":    300000
}


class BaseTEIID(BaseSql):
    '''
    '''
    def __init__(self, db, host=None, vdb=None, port=None,
                 username=None, password=None, **kwargs):
        self.host = self.setdefault(host, DEFAULT_CONFIG['host'])
        self.vdb = self.setdefault(vdb, DEFAULT_CONFIG['vdb'])
        self.port = self.setdefault(port, DEFAULT_CONFIG['port'])
        self.username = self.setdefault(username, DEFAULT_CONFIG['username'])
        self.password = self.setdefault(password, DEFAULT_CONFIG['password'])
        super(BaseTEIID, self).__init__(host=self.host, port=self.port,
                                        db=self.db, **kwargs)

    @property
    def proxy(self):
        self._proxy = TEIID(vdb=self.vdb, db=self.db, host=self.host,
                            port=self.port, username=self.username,
                            password=self.password)
        return self._proxy
