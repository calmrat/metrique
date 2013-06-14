#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Juraj Niznan" <jniznan@redhat.com>

import logging
logger = logging.getLogger(__name__)

from collections import defaultdict
from copy import deepcopy

from metrique.server.cubes import get_cube
from metrique.tools.type_cast import type_cast


def _activity_backwards(c, field, val, removed, added):
    inconsistent = False
    container = c.get_field_property('container', field)

    try:
        if container:
            val = [] if val is None else val
            for ad in added:
                if ad in val:
                    val.remove(ad)
                else:
                    inconsistent = True
            for rem in removed:
                val.append(rem)
        else:
            if val != added[0]:
                inconsistent = True
            val = removed[0]
    except:
        inconsistent = True
    return val, inconsistent


def _activity_batch_update(c, batch_updates, activity):
    '''
    Adds the object before the activity to batch_updates.
    '''
    when, field, removed, added, inconsistent = activity
    last_doc = batch_updates[-1]
    # We ignore the fields that aren't in the timeline
    if field not in last_doc['fields']:
        return

    tid = last_doc['id']
    batch_updates.pop()

    # check if this activity happened at the same time as the last one, if it
    # did then we need to group them together
    if last_doc['end'] == when:
        new_doc = last_doc
        last_doc = batch_updates.pop()
    else:
        try:
            # set start to creation time if available
            creation_field = c.get_field_property('cfield')
            start = last_doc['fields'][creation_field]
        except:
            start = when
        new_doc = {'fields': deepcopy(last_doc['fields']),
                   'id': tid,
                   'start': start,
                   'end': when,
                   'current': False}
    if 'corrupted' in last_doc:
        new_doc['corrupted'] = last_doc['corrupted']
    last_doc['start'] = when
    last_val = last_doc['fields'][field]
    if not inconsistent:
        new_val, inconsistent = _activity_backwards(c, field,
                                                    new_doc['fields'][field],
                                                    removed, added)
        new_doc['fields'][field] = new_val
    # Check if the object has the correct field value.
    if inconsistent:
        logger.warn(u'Inconsistency: %s %s: %s -> %s, '
                    'object has %s.' % (
                        tid, field, removed,
                        added, last_val))
        if 'corrupted' not in new_doc:
            new_doc['corrupted'] = {}
        new_doc['corrupted'][field] = added
    # Add the objects to the batch
    batch_updates.append(last_doc)
    batch_updates.append(new_doc)


def _activity_prepare(c, activities):
    '''
    Creates a list of (when, field, removed, added, inconsistent) pairs.
    It groups those activities that happened at the same time on the
    same field.
    The list is sorted descending by when.
    '''
    d = defaultdict(lambda: defaultdict(list))
    for act in activities:
        what = act['what']
        when = act['when']
        d[when][what].append(act)

    res = []
    for when in d:
        for what in d[when]:
            acts = d[when][what]
            field = c.fieldmap[what]
            fieldtype = c.get_field_property('type', field)
            container = c.get_field_property('container', field)
            try:
                if container:
                    # we must group the added and removed
                    added = [cast_to_list(act['added'], fieldtype)
                             for act in acts]
                    added = sum(added, [])
                    removed = [cast_to_list(act['removed'], fieldtype)
                               for act in acts]
                    removed = sum(removed, [])
                else:
                    # there should be only one activity for this,
                    # otherwise it is corrupted
                    removed = [type_cast(act['removed'], fieldtype)
                               for act in acts]
                    added = [type_cast(act['added'], fieldtype)
                             for act in acts]
                res.append((when, field, removed, added, False))
            except:
                added = [act['added'] for act in acts]
                removed = [act['removed'] for act in acts]
                res.append((when, field, removed, added, True))
    res.sort(reverse=True)
    return res


def _activity_import_doc(c, time_doc, activities, timeline):
    '''
    Import activities for a single document into timeline.
    '''
    batch_updates = [time_doc]
    for act in _activity_prepare(c, activities):
        # We want to consider only activities that happend before time_doc
        if act[0] < time_doc['start']:
            # apply the activity to the batch:
            _activity_batch_update(c, batch_updates, act)

    if len(batch_updates) > 1:
        # make the batch update
        doc = batch_updates[0]
        spec_now = {'_id': doc['_id']}
        update_now = {'$set': {'start': doc['start']}}
        timeline.update(spec_now, update_now, upsert=True)
        timeline.insert(batch_updates[1:])


def _activity_import(cube, ids):
    timeline = get_cube(cube, admin=True, timeline=True)
    timeline.ensure_index([('id', 1), ('start', 1)])
    time_docs = timeline.find({'id': {'$in': ids}},
                              sort=[('id', 1), ('start', 1)])

    h = get_cube('%s_activity' % cube)
    act_docs = h.find({'id': {'$in': ids}}, sort=[('id', 1), ('when', -1)])
    act_docs_iter = iter(act_docs)

    act_id = -1
    last_doc_id = -1
    activities = []
    for time_doc in time_docs:
        tid = time_doc['id']
        # we want to update only the oldest version of the object
        if tid != last_doc_id:
            last_doc_id = tid
            while act_id <= tid:
                try:
                    act_doc = act_docs_iter.next()
                    act_id = act_doc['id']
                    activities.append(act_doc)
                except StopIteration:
                    break
            acts = [act for act in activities if act['id'] == tid]
            _activity_import_doc(c, time_doc, acts, timeline)
            activities = [act for act in activities if act['id'] > tid]


def activity_import(cube, ids=None):
    logger.debug('Running activity history import')
    h = get_cube('%s_activity' % cube, timeline=True, admin=True)
    h.ensure_index([('id', 1), ('when', -1)])
    if ids is None:
        # Run on all the ids
        t = c.get_collection(timeline=True, admin=True)
        docs = t.find({'current': True}, fields=['id'])
        logger.debug('Found %s docs' % docs.count())

        ids = []
        for done, doc in enumerate(docs):
            ids.append(doc['id'])
            if done % 100000 == 0:
                _activity_import(cube, ids)
                ids = []
                logger.debug(' ... %s done' % done)
        _activity_import(cube, ids)
        logger.debug(' ... %s done' % (done + 1))
    elif type(ids) is list:
        _activity_import(cube, ids)
    elif isinstance(ids, basestring):
        ids = map(int, ids.split(','))
        _activity_import(cube, ids)


def cast_to_list(value, fieldtype):
    if value is None:
        return []
    value = type_cast([s.strip() for s in value.split(',')],
                      fieldtype)
    return value if (type(value) is list) else [value]
