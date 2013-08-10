#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Red Hat Internal
# Author: "Chris Ward <cward@redhat.com>
# GIT: http://git.engineering.redhat.com/?p=users/cward/Metrique.git

import logging
logger = logging.getLogger(__name__)
from bson.objectid import ObjectId
import os

from metrique.client.cubes.basecsv import BaseCSV


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

    def extract(self, uri, _id=None, cube=None, **kwargs):
        '''
        '''
        if cube:
            self.name = cube
        uri = os.path.expanduser(uri)
        logger.debug("Loading CSV: %s" % uri)
        objects = self.loaduri(uri)
        # save the uri for reference too
        objects = self.set_column(objects, 'uri', uri)
        if _id:
            objects = self.set_column(objects, '_id', _id)
        else:
            objects = self.set_column(objects, '_id', ObjectId)
        return self.save_objects(objects)
