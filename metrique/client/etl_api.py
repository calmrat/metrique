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


def index_warehouse(self, fields=None):
    '''
    Index particular fields of a given cube, assuming
    indexing is enabled for the cube.fields

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    fields : str, or list of str, or str of comma-separated values
        Fields that should be indexed
    '''
    if not fields:
        fields = '__all__'

    return self._get(CMD, 'index/warehouse', cube=self.name,
                     fields=fields)


def snapshot(self, ids=None):
    '''
    Run a warehouse -> timeline (datetimemachine) snapshot
    of the data as it existed in the warehouse and dump
    copies of objects into the timeline, one new object
    per unique state in time.

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    ids : list of cube object ids or str of comma-separated ids
        Specificly run snapshot for this list of object ids
    '''
    return self._get(CMD, 'snapshot', cube=self.name, ids=ids)


def activity_import(self, ids=None):
    '''
    Run the activity import for a given cube, if the
    cube supports it.

    Essentially, recreate object histories from
    a cubes 'activity history' table row data,
    and dump those pre-calcultated historical
    state object copies into the timeline.

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    ids : list of cube object ids or str of comma-separated ids
        Specificly run snapshot for this list of object ids
    '''
    return self._get(CMD, 'activityimport', cube=self.name, ids=ids)


def save_objects(self, objects, update=False,
                 batch=DEFAULT_BATCH, workers=MAX_WORKERS):
    '''
    Save a list of objects the given metrique.cube

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    objs : list of dicts with 1+ field:value and _id defined
    '''
    olen = len(objects)
    t1 = time()
    if olen < batch:
        saved = self._post(CMD, 'saveobjects', cube=self.name,
                           update=update,
                           objects=objects)
    else:
        saved = 0
        k = 0
        _k = batch
        with ThreadPoolExecutor(workers) as executor:
            pool = []
            while True:
                pool.append(
                    executor.submit(
                        self._post, CMD, 'saveobjects', cube=self.name,
                        update=update, objects=objects[k:_k]))
                k = _k
                _k += batch
                if _k > olen:
                    break

            pool.append(
                executor.submit(
                    self._post, CMD, 'saveobjects', cube=self.name,
                    update=update, objects=objects[k:]))

            for future in as_completed(pool):
                saved += future.result()
    logger.debug("... Saved %s docs in ~%is" % (olen, time() - t1))
    return saved


def drop(self):
    '''
    Drops current cube from warehouse
    '''
    return self._delete(CMD, 'drop', cube=self.name)
    pass
