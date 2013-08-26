#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Red Hat Internal
# Author: "Chris Ward <cward@redhat.com>
# GIT: http://git.engineering.redhat.com/?p=users/cward/Metrique.git

import os

from metrique.client.cubes.basecsv import BaseCSV
from metrique.client.utils import new_oid


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

    def extract(self, uri, _oid=None, cube=None, **kwargs):
        '''
        '''
        if cube:
            self.name = cube
        uri = os.path.expanduser(uri)
        self.logger.debug("Loading CSV: %s" % uri)
        objects = self.loaduri(uri)
        # save the uri for reference too
        objects = self.set_column(objects, 'uri', uri)
        objects = self.set_column(objects, '_oid', _oid if _oid else new_oid)
        return self.save_objects(objects)
