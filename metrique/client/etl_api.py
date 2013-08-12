#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique ETL" related funtions '''

import logging
logger = logging.getLogger(__name__)
from datetime import datetime
from time import time
import pytz

DEFAULT_BATCH = 100000
MAX_WORKERS = 2

CMD = 'admin/etl'


def list_index(self, db='timeline'):
    '''
    List indexes for either timeline or warehouse.

    :param string db:
        'timeline' or 'warehouse'
    '''
    return self._get(CMD, 'index', cube=self.name, db=db)


def ensure_index(self, key_or_list, db='timeline'):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    :param string db:
        'timeline' or 'warehouse'
    '''
    return self._get(CMD, 'index', cube=self.name, db=db, ensure=key_or_list)


def drop_index(self, index_or_name, db='timeline'):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    :param string db:
        'timeline' or 'warehouse'
    '''
    return self._get(CMD, 'index', cube=self.name, db=db, drop=index_or_name)


def snapshot(self, ids=None):
    '''
    :param list ids: list of cube object ids or str of comma-separated ids
        Specificly run snapshot for this list of object ids

    Run a warehouse -> timeline (datetimemachine) snapshot
    of the data as it existed in the warehouse and dump
    copies of objects into the timeline, one new object
    per unique state in time.
    '''
    return self._get(CMD, 'snapshot', cube=self.name, ids=ids)


def save_objects(self, objects, update=False,
                 batch=DEFAULT_BATCH, workers=MAX_WORKERS,
                 timeline=False):
    '''
    :param list objects: list of dictionary-like objects to be stored
    :param boolean update: update already stored objects?
    :param integer batch: maximum slice of objects to post at a time
    :param integer workers: number of threaded workers to post in parallel
    :param boolean timeline: target db to save objects is timeline
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
    if olen < batch:
        saved = self._post(CMD, 'saveobjects', cube=self.name,
                           update=update, objects=objects,
                           timeline=timeline, mtime=now)
    else:
        k = 0
        _k = batch

        saved = []
        while k <= olen:
            saved.extend(self.post(CMD, 'saveobjects', cube=self.name,
                         update=update, objects=objects[k:_k],
                         timeline=timeline, mtime=now))
            k = _k
            _k += batch
        else:
            saved.extend(self._post(CMD, 'saveobjects', cube=self.name,
                         update=update, objects=objects[k:],
                         timeline=timeline, mtime=now))

    logger.debug("... Saved %s docs in ~%is" % (olen, time() - t1))
    # timeline objects are expected to have _oid
    # warehouse objects are expected to have _id
    _id = '_oid' if timeline else '_id'
    return sorted(list(set([o[_id] for o in objects if o])))


def cube_drop(self):
    '''
    Drops current cube from warehouse
    '''
    return self._delete(CMD, 'cube/drop', cube=self.name)


# Wrap pymongo.remove()
# def remove(self,
