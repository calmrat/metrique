#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metrique.utils import get_cube
csvdata_rows = get_cube('csvdata_rows')

cwd = os.path.dirname(os.path.abspath(__file__))
here = '/'.join(cwd.split('/')[0:-2])
DEFAULT_URI = os.path.join(here, 'us-idx-eod.csv')
DEFAULT_ID = 'symbol'


class Local(csvdata_rows):
    """
    Test Cube; csv based
    """
    name = 'tests_csvdata'

    def extract(self, uri=DEFAULT_URI, _oid=DEFAULT_ID, **kwargs):
        return super(Local, self).extract(uri=uri, _oid=_oid, **kwargs)


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Local)
