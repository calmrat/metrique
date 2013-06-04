#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Jan Grec" <jgrec@redhat.com>

from metrique.client.pyclient import pyclient


class BaseCube(pyclient):
    def __init__(self, *args, **kwargs):
        super(BaseCube, self).__init__(*args, **kwargs)
        self._queryfind = self.query.find
        self.query.find = self._find

    def _find(self, query, fields='', date=None, most_recent=True):
        return self._queryfind(self.cube, query, fields,
                               date, most_recent)
