#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique ETL" related funtions '''

import logging
logger = logging.getLogger(__name__)
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time

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
    :param batch:
    :param integer workers: number of subprocesses to work on saving
    :param boolean timeline:
    :rtype: integer - number of objects successfully saved

    Save a list of objects the given metrique.cube
    '''
    olen = len(objects)
    if not olen:
        logger.debug("... No objects to save")
        return 0

    t1 = time()
    if olen < batch:
        saved = self._post(CMD, 'saveobjects', cube=self.name,
                           update=update, objects=objects,
                           timeline=timeline)
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
        saved = 0
        k = 0
        _k = batch
        with ThreadPoolExecutor(workers) as executor:
            pool = []
            while True:
                pool.append(
                    executor.submit(
                        self._post, CMD, 'saveobjects', cube=self.name,
                        update=update, objects=objects[k:_k],
                        timeline=timeline))
                k = _k
                _k += batch
                if _k > olen:
                    break

            pool.append(
                executor.submit(
                    self._post, CMD, 'saveobjects', cube=self.name,
                    update=update, objects=objects[k:],
                    timeline=timeline))

            for future in as_completed(pool):
                saved += future.result()
    logger.debug("... Saved %s docs in ~%is" % (olen, time() - t1))
    return saved


def cube_drop(self):
    '''
    Drops current cube from warehouse
    '''
    return self._delete(CMD, 'cube/drop', cube=self.name)


# Wrap pymongo.remove()
# def remove(self,
