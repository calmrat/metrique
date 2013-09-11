#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
This module contains all the ETL (data extract, transform, load)
related api functionality, along with other collection and
data manipulation calls.
'''

from datetime import datetime
from time import time
import os
import pytz

from metriqueu.utils import batch_gen, set_default

DEFAULT_BATCH = 100000
CMD = ''


def save_objects(self, objects, batch=DEFAULT_BATCH, cube=None, owner=None):
    '''
    :param list objects: list of dictionary-like objects to be stored
    :param string owner: owner of cube
    :param string cube: cube name to use
    :param int batch: maximum slice of objects to post at a time
    :rtype: list - list of object ids saved

    Save a list of objects the given metrique.cube.

    Returns back a list of object ids (_id|_oid) saved.
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)

    olen = len(objects) if objects else None
    if not olen:
        self.logger.debug("... No objects to save")
        return []

    # get 'now' utc timezone aware datetime object
    now = pytz.UTC.localize(datetime.utcnow())

    cmd = os.path.join(owner, cube, 'save_objects')

    t1 = time()
    if olen <= batch:
        saved = self._post(cmd, objects=objects, mtime=now)
    else:
        saved = []
        for _batch in batch_gen(objects, batch):
            _saved = self._post(cmd, objects=_batch, mtime=now)
            saved.extend(_saved)

    self.logger.info("... Saved %s docs in ~%is" % (olen, time() - t1))
    return sorted(list(set([o['_oid'] for o in objects if o])))


def remove_objects(self, ids, backup=False, cube=None, owner=None):
    '''
    Remove objects from cube timeline

    :param list ids: list of object ids to remove
    :param bool backup: return the documents removed to client?
    :param string cube: cube name to use
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'remove_objects')
    if not ids:
        return True
    else:
        return self._delete(cmd, ids=ids, backup=backup)
