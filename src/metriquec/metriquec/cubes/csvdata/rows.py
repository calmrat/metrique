#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import itertools
import os
import pandas as pd
import re
import tempfile
from urllib2 import urlopen

from metrique import pyclient


class Rows(pyclient):
    """
    Object used for extracting data from CSV files

    It's possible to load from a http(s):// or file://.

    The field names are defined by the column headers,
    therefore column heders are required in the csv.
    """
    name = 'csvdata_rows'

    def get_objects(self, uri, _oid=None, _start=None, **kwargs):
        path = self.save_uri(uri)
        objects = pd.read_csv(path, **kwargs)
        # convert to list of dicts
        objects = objects.T.to_dict().values()

        # set uri property for all objects
        objects = self.set_column(objects, 'uri', uri, static=True)

        if _start:
            # set _start based on column values if specified
            objects = self.set_column(objects, '_start', _start, static=True)

        if not _oid:
            # map to row index count by default
            k = itertools.count(1)
            [o.update({'_oid': k.next()}) for o in objects]
        else:
            objects = self.set_column(objects, '_oid', _oid)

        self.objects = objects
        return objects

    def save_uri(self, uri):
        '''
        Load csv from a given uri and save it to a temp file
        or load the csv file directly, if already on disk.
        Supports: http(s) or from file

        :param string uri:
            uri path to load csv contents from
        '''
        self.logger.debug("Loading CSV: %s" % uri)
        if re.match('https?://', uri):
            content = ''.join(urlopen(uri).readlines())
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                path = tmp.name
                self.logger.debug(" ... saving to: %s" % path)
                tmp.write(content)
        else:
            path = re.sub('^file://', '', uri)
            path = os.path.expanduser(path)
        return path

    def set_column(self, objects, key, value, static=False):
        '''
        Save an additional column/field to all objects in memory
        '''
        if type(value) is type or hasattr(value, '__call__'):
            # class or function; use the resulting object after init/exec
            [o.update({key: value(o)}) for o in objects]
        elif static:
            [o.update({key: value}) for o in objects]
        else:
            [o.update({key: o[value]}) for o in objects]
        return objects


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Rows)
