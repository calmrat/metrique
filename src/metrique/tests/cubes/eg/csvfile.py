#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metriquec.csvobject import CSVObject

cwd = os.path.dirname(os.path.abspath(__file__))
root = '/'.join(cwd.split('/')[0:-1])
fixtures = os.path.join(root, 'fixtures')
DEFAULT_URI = os.path.join(fixtures, 'us-idx-eod.csv')
DEFAULT_ID = 'symbol'


class Csvfile(CSVObject):
    """
    Test Cube; csv based
    """
    name = 'eg_csvfile'

    def extract(self, uri=DEFAULT_URI, _oid=DEFAULT_ID, **kwargs):
        return super(Csvfile, self).extract(uri=uri, _oid=_oid, **kwargs)

if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Csvfile)
