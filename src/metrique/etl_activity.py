#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Juraj Niznan" <jniznan@redhat.com>

import logging
logger = logging.getLogger(__name__)

from copy import deepcopy
from metrique.utils import ts2dt, dt2ts
from datetime import datetime


def _activity_backwards(val, removed, added):
    if isinstance(added, list) and isinstance(removed, list):
        val = [] if val is None else val
        inconsistent = False
        for ad in added:
            if ad in val:
                val.remove(ad)
            else:
                inconsistent = True
        val.extend(removed)
    else:
        inconsistent = val != added
        val = removed
    return val, inconsistent


def _activity_import_doc(cube, time_doc, activities):
    '''
    Import activities for a single document into timeline.
    '''
    batch_updates = [time_doc]
    # We want to consider only activities that happend before time_doc
    # do not move this, because time_doc._start changes
    # time_doc['_start'] is a timestamp, whereas act[0] is a datetime
    td_start = ts2dt(time_doc['_start'])
    activities = filter(lambda act: (act[0] < td_start and
                                     act[1] in time_doc), activities)
    for when, field, removed, added in activities:
        removed = dt2ts(removed) if isinstance(removed, datetime) else removed
        added = dt2ts(added) if isinstance(added, datetime) else added
        last_doc = batch_updates.pop()
        # check if this activity happened at the same time as the last one,
        # if it did then we need to group them together
        if last_doc['_end'] == when:
            new_doc = last_doc
            last_doc = batch_updates.pop()
        else:
            try:
                # set start to creation time if available
                creation_field = cube.get_property('cfield')
                start = last_doc[creation_field]
            except:
                start = when
            new_doc = deepcopy(last_doc)
            new_doc.pop('_id') if '_id' in new_doc else None
            new_doc['_start'] = start
            new_doc['_end'] = when
            last_doc['_start'] = when
        last_val = last_doc[field]
        new_val, inconsistent = _activity_backwards(new_doc[field],
                                                    removed, added)
        new_doc[field] = new_val
        # Check if the object has the correct field value.
        if inconsistent:
            msg = 'Inconsistency: %s %s: %s -> %s, object has %s' % (
                last_doc['_oid'], field, removed, added, last_val)
            logger.debug(msg)
            msg = '        Types: %s -> %s, object has %s.' % (
                type(removed), type(added), type(last_val))
            logger.debug(msg)
            if '_corrupted' not in new_doc:
                new_doc['_corrupted'] = {}
            new_doc['_corrupted'][field] = added
        # Add the objects to the batch
        batch_updates.append(last_doc)
        batch_updates.append(new_doc)
    return batch_updates


def _get_time_docs_cursor(cube, ids):
    if isinstance(ids, list):
        q = '_oid in %s' % ids
    if isinstance(ids, tuple):
        q = '_oid >= %s and _oid <= %s' % ids
    time_docs = cube.find(q, fields='__all__', date='~',
                          sort=[('_oid', 1), ('_start', 1)], raw=True)
    return time_docs


def _activity_import(cube, ids, batch_size):
    time_docs = _get_time_docs_cursor(cube, ids)

    # generator that yields by ids ascending
    # has format: (id, [(when, field, removed, added)])
    act_generator = cube.activity_get(ids)

    last_doc_id = -1
    aid = -1
    batched_updates = []
    for time_doc in time_docs:
        _oid = time_doc['_oid']
        # we want to update only the oldest version of the object
        while aid < _oid:
            aid, acts = act_generator.next()
        if _oid != last_doc_id and aid == _oid:
            last_doc_id = _oid
            updates = _activity_import_doc(cube, time_doc, acts)
            if len(updates) > 1:
                batched_updates += updates
        if len(batched_updates) >= batch_size:
            cube.save_objects(batched_updates)
            batched_updates = []
    if batched_updates:
        cube.save_objects(batched_updates)


def activity_import(self, ids=None, save_batch_size=1000, chunk_size=1000):
    '''
    Run the activity import for a given cube, if the
    cube supports it.

    Essentially, recreate object histories from
    a cubes 'activity history' table row data,
    and dump those pre-calcultated historical
    state object copies into the timeline.

    :param object ids:
        Multiple choices of what ids can be:
            - None: import for all ids
            - list of ids: import for ids in the list
            - csv list of ids:  import for ids in the csv list
            - 2-tuple of ids: import for the ids in the interval specified by
            the tuple
    :param integer save_batch_size:
        Determines the size of the batch when sending objects to save to the
        Metrique server
    :param integer chunk_size:
        Size of the chunks into which the ids are split, activity import is
        done and saved separately for each batch
    '''
    if ids is None:
        max_oid = self.find('_oid == exists(True)', date='~',
                            sort=[('_oid', -1)], one=True, raw=True)['_oid']
        ids = (0, max_oid)
    if isinstance(ids, tuple):
        for i in range(ids[0], ids[1] + 1, chunk_size):
            _activity_import(self, (i, min(ids[1], i + chunk_size - 1)),
                             batch_size=save_batch_size)
    else:
        if not isinstance(ids, list):
            raise ValueError(
                "Expected ids to be None, tuple or list. Got %s" % type(list))

        for i in range(0, len(ids), chunk_size):
            _activity_import(self, ids[i:i + chunk_size],
                             batch_size=save_batch_size)
