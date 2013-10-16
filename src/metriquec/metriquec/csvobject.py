#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Basic CSV based cube for extracting data from CSV
'''

import os

from metriquec.basecsv import BaseCSV


class CSVObject(BaseCSV):
    """
    Object used for communication with generic CSV objects
    """
    name = 'csvobject'

    #defaults = {
        #'header': ['column1', 'column2', ...]
    #}
    # field check can be made by defining 'header' property
    # in defaults above. A check will be made to compare the
    # expected header to actual

    def extract(self, uri, _oid, cube=None, **kwargs):
        '''
        '''
        if cube:
            self.name = cube
        uri = os.path.expanduser(uri)
        self.logger.debug("Loading CSV: %s" % uri)
        objects = self.loaduri(uri)
        # save the uri for reference too
        objects = self.set_column(objects, 'uri', uri)
        objects = self.set_column(objects, '_oid', _oid)
        return self.cube_save(objects)
