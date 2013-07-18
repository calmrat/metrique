#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique ETL" related funtions '''

import logging
logger = logging.getLogger(__name__)
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from time import time
import pytz

DEFAULT_BATCH = 100000
MAX_WORKERS = 2

CMD = 'admin/etl'


def index_warehouse(self, fields='__all__'):
    '''
    :param fields: str, or list of str, or str of comma-separated values
        Fields that should be indexed

    Index particular fields of a given cube, assuming
    indexing is enabled for the cube.fields

    Default behavior is to return back __all__, which
    gets translated to a list of all available fields

    '''
    return self._get(CMD, 'index/warehouse', cube=self.name,
                     fields=fields)


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
    olen = len(objects)
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
        ###### FIXME: THINK ABOUT ME ######
        # Should we not even worry about implementing
        # native parallellism into metrique.client?
        # Let the running user worry about it instead?
        # eg, user could do exactly what we're doing
        # below, or they could prefer using ipython
        # distributed, etc.
        #
        # Should we remove this async option from the client?
        ######
        k = 0
        _k = batch
        with ThreadPoolExecutor(workers) as executor:
            pool = []
            # FIXME: why not
            # while _k <= olen:
            while True:
                pool.append(
                    executor.submit(
                        self._post, CMD, 'saveobjects', cube=self.name,
                        update=update, objects=objects[k:_k],
                        timeline=timeline, mtime=now))
                k = _k
                _k += batch
                if _k > olen:
                    break

            pool.append(
                executor.submit(
                    self._post, CMD, 'saveobjects', cube=self.name,
                    update=update, objects=objects[k:],
                    timeline=timeline, mtime=now))

            saved = []
            for future in as_completed(pool):
                # just make sure we didn't hit any exceptions
                saved.extend(future.result())

    logger.debug("... Saved %s docs in ~%is" % (olen, time() - t1))
    # timeline objects are expected to have _oid
    # warehouse objects are expected to have _id
    _id = '_oid' if timeline else '_id'
    return [o[_id] for o in objects]


def cube_drop(self):
    '''
    Drops current cube from warehouse
    '''
    return self._delete(CMD, 'cube/drop', cube=self.name)


# Wrap pymongo.remove()
# def remove(self,
