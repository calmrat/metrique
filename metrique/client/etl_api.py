#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique ETL" related funtions '''

import logging
logger = logging.getLogger(__name__)
from datetime import datetime
from time import time
import pytz

from metrique.tools import batch_gen

DEFAULT_BATCH = 100000
CMD = 'admin/etl'


def list_index(self):
    '''
    List indexes for either timeline or warehouse.
    '''
    return self._get(CMD, 'index', cube=self.name)


def ensure_index(self, key_or_list):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    '''
    return self._get(CMD, 'index', cube=self.name, ensure=key_or_list)


def drop_index(self, index_or_name):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    '''
    return self._get(CMD, 'index', cube=self.name, drop=index_or_name)


def save_objects(self, objects, update=False, batch=DEFAULT_BATCH):
    '''
    :param list objects: list of dictionary-like objects to be stored
    :param boolean update: update already stored objects?
    :param integer batch: maximum slice of objects to post at a time
    :rtype: list - list of object ids saved

    Save a list of objects the given metrique.cube.

    Return back a list of object ids (_id|_oid) saved.
    '''
    olen = len(objects) if objects else None
    if not olen:
        logger.debug("... No objects to save")
        return []

    # get 'now' utc timezone aware datetime object
    now = pytz.UTC.localize(datetime.utcnow())

    t1 = time()
    if olen <= batch:
        saved = self._post(CMD, 'saveobjects', cube=self.name,
                           update=update, objects=objects,
                           mtime=now)
    else:
        saved = []
        for _batch in batch_gen(objects, batch):
            _saved = self._post(CMD, 'saveobjects', cube=self.name,
                                update=update, objects=_batch,
                                mtime=now)
            saved.extend(_saved)

    logger.debug("... Saved %s docs in ~%is" % (olen, time() - t1))

    return sorted(list(set([o['_oid'] for o in objects if o])))


def remove_objects(self, ids, backup=False):
    ''' Remove objects from cube timeline '''
    if not ids:
        return []
    return self._delete(CMD, 'removeobjects', cube=self.name,
                        ids=ids, backup=backup)


def cube_drop(self):
    ''' Drops current cube from timeline '''
    return self._delete(CMD, 'cube/drop', cube=self.name)
