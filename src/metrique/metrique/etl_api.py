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
import pytz

from metrique.utils import batch_gen

DEFAULT_BATCH = 100000
CMD = 'admin/etl'


def list_index(self, cube=None):
    '''
    List indexes for either timeline or warehouse.

    :param string cube: cube name to use
    '''
    if not cube:
        cube = self.name
    return self._get(CMD, 'index', cube=cube)


def ensure_index(self, key_or_list, cube=None):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    :param string cube: cube name to use
    '''
    if not cube:
        cube = self.name
    return self._get(CMD, 'index', cube=cube, ensure=key_or_list)


def drop_index(self, index_or_name, cube=None):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    :param string cube: cube name to use
    '''
    if not cube:
        cube = self.name
    return self._get(CMD, 'index', cube=cube, drop=index_or_name)


def save_objects(self, objects, update=False, batch=DEFAULT_BATCH,
                 cube=None):
    '''
    :param list objects: list of dictionary-like objects to be stored
    :param bool update: update already stored objects?
    :param int batch: maximum slice of objects to post at a time
    :param string cube: cube name to use
    :rtype: list - list of object ids saved

    Save a list of objects the given metrique.cube.

    Returns back a list of object ids (_id|_oid) saved.
    '''
    if not cube:
        cube = self.name
    olen = len(objects) if objects else None
    if not olen:
        self.logger.debug("... No objects to save")
        return []

    # get 'now' utc timezone aware datetime object
    now = pytz.UTC.localize(datetime.utcnow())

    t1 = time()
    if olen <= batch:
        saved = self._post(CMD, 'saveobjects', cube=cube,
                           update=update, objects=objects,
                           mtime=now)
    else:
        saved = []
        for _batch in batch_gen(objects, batch):
            _saved = self._post(CMD, 'saveobjects', cube=cube,
                                update=update, objects=_batch,
                                mtime=now)
            saved.extend(_saved)

    self.logger.info("... Saved %s docs in ~%is" % (olen, time() - t1))
    return sorted(list(set([o['_oid'] for o in objects if o])))


def remove_objects(self, ids, backup=False, cube=None):
    '''
    Remove objects from cube timeline

    :param list ids: list of object ids to remove
    :param bool backup: return the documents removed to client?
    :param string cube: cube name to use
    '''
    if not cube:
        cube = self.name
    if not ids:
        return []
    return self._delete(CMD, 'removeobjects', cube=cube,
                        ids=ids, backup=backup)


def cube_drop(self, cube=None):
    '''
    Drops current cube from timeline

    :param string cube: cube name to use
    '''
    if not cube:
        cube = self.name
    return self._delete(CMD, 'cube/drop', cube=cube)
