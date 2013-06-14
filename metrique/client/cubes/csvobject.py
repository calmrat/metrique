#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Red Hat Internal
# Author: "Chris Ward <cward@redhat.com>
# GIT: http://git.engineering.redhat.com/?p=users/cward/Metrique.git

import logging
logger = logging.getLogger(__name__)

from metrique.client.cubes.basecsv import BaseCSV

DEFAULT_CONFIG = {
    'bz_bugs': 'https://bugzilla.redhat.com/buglist.cgi?product=Bugzilla&ctype=csv'
}


class CSVObject(BaseCSV):
    """
    Object used for communication with generic CSV objects
    """
    name = 'csvobject'

    defaults = {
        'index': True,
        #'header': ['column1', 'column2', ...]
    }

    # field check can be made by defining 'header' property
    # in defaults above. A check will be made to compare the
    # expected header to actual

    fields = {}

    def __init__(self, uri=None, **kwargs):
        super(CSVObject, self).__init__(**kwargs)
        if not uri:
            uri = DEFAULT_CONFIG['bz_bugs']
        self.uri = uri

    def extract(self, **kwargs):
        logger.debug("Loading CSV: %s" % self.uri)
        objects = self.loaduri(self.uri)
        [o.update({'uri': self.uri}) for o in objects]
        return self.save_objects(objects)
