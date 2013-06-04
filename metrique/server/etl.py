#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from bson.objectid import ObjectId
from datetime import datetime
from copy import deepcopy
from collections import defaultdict

from metrique.server.drivers.drivermap import get_cube, get_fields

from metrique.tools.constants import UTC, YELLOW, ENDC
from metrique.tools.type_cast import type_cast


def get_last_id(cube, field):
    '''
    '''
    c = get_cube(cube)
    spec = {field: {'$exists': True}}
    _cube = c.get_collection()
    logger.debug(" ... %s.%s._get_last_id spec: %s" % (
        _cube.db.name, _cube.collection, spec))
    last_id = _cube.find_one(spec, {'_id': 1}, sort=[('_id', -1)])
    if last_id:
        value = last_id['_id']
    else:
        value = None
    logger.debug(" ... ... Last ID: %s" % last_id)
    return value


def save_doc(cube, field, tokens, id=None):
    '''
    All subclasses use this method to 'save' a document into the warehouse
    '''
    c = get_cube(cube)
    if field not in c.fields:
        raise ValueError("Invalid field (%s)" % field)

    container = c.get_field_property('container', field, False)

    if tokens and container:
        if type(tokens) is not list:
            raise TypeError("Tokens type must be list()")
        else:
            tokens = sorted(tokens)

    # normalize empty lists -> None
    if not tokens:
        tokens = None

    if id is None:
        id = ObjectId()

    now = datetime.now(UTC)
    spec_now = {'_id': id}
    update_now = {'$set': {field: tokens, '_mtime': now}}

    _cube = c.get_collection(admin=True)
    _cube.update(spec_now, update_now, upsert=True)

    return 1  # eg, one document added


def save_object(cube, obj, _id=None):
    '''
    '''
    if not type(obj) in [list, tuple]:
        obj = [obj]
    for _saved, o in enumerate(obj):
        for field, tokens in o.iteritems():
            save_doc(cube, field, tokens, o[_id])
    return _saved


def _snapshot(cube, ids):
    c = get_cube(cube)
    w = c.get_collection(admin=False, timeline=False)
    t = c.get_collection(admin=True, timeline=True)
    docs = w.find({'_id': {'$in': ids}}, sort=[('_id', 1)])
    time_docs = t.find({'current': True, 'id': {'$in': ids}},
                       sort=[('id', 1)])
    time_docs_iter = iter(time_docs)
    tid = -1

    batch_insert = []
    for doc in docs:
        _id = doc.pop('_id')
        _mtime = doc.pop('_mtime')

        # time_doc will contain first doc that has id >= _id,
        # it might be a document where id > _id
        while tid < _id:
            try:
                time_doc = time_docs_iter.next()
                tid = time_doc['id']
            except StopIteration:
                break

        store_new_doc = False
        if _id == tid:
            if doc != time_doc['fields']:
                store_new_doc = True
                spec_now = {'_id': time_doc['_id']}
                update_now = {'$set': {'current': False},
                              '$set': {'end': _mtime}}
                t.update(spec_now, update_now, upsert=True)
        else:
            store_new_doc = True

        if store_new_doc:
            new_doc = {'fields': doc,
                       'id': _id,
                       'start': _mtime,
                       'end': None,
                       'current': True}
            batch_insert.append(new_doc)
        if len(batch_insert) > 1000:
            t.insert(batch_insert)
            batch_insert = []
    if len(batch_insert) > 0:
        t.insert(batch_insert)


def snapshot(cube, ids=None):
    logger.debug('Running snapshot')
    if ids is None:
        # Run on all the ids
        c = get_cube(cube)
        w = c.get_collection(admin=False, timeline=False)
        docs = w.find(fields=['_id'])
        logger.debug('Found %s docs' % docs.count())

        ids_to_snapshot = []
        for done, doc in enumerate(docs):
            ids_to_snapshot.append(doc['_id'])
            if done % 100000 == 0:
                _snapshot(cube, ids_to_snapshot)
                ids_to_snapshot = []
                logger.debug(' ... %s done' % done)
        _snapshot(cube, ids_to_snapshot)
    elif type(ids) is list:
        _snapshot(cube, ids)
    elif isinstance(ids, basestring):
        ids = map(int, ids.split(','))
        _snapshot(cube, ids)

    logger.debug(' ... %s done' % (done + 1))


def cast_to_list(value, fieldtype):
    if value is None:
        return []
    value = type_cast([s.strip() for s in value.split(',')],
                      fieldtype)
    return value if (type(value) is list) else [value]


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
        logger.warn('Inconsistency: %s %s: %s -> %s, '
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
    c = get_cube(cube)

    timeline = c.get_collection(timeline=True, admin=True)
    time_docs = timeline.find({'id': {'$in': ids}},
                              sort=[('id', 1), ('start', 1)])

    h = c.get_collection(timeline=False, admin=False,
                         cube='%s_activity' % c.name)
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
    c = get_cube(cube)
    h = c.get_collection(timeline=False, admin=True,
                         cube='%s_activity' % c.name)
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


def index_timeline(cube):
    logger.debug(" ... Indexing Timeline")
    c = get_cube(cube)
    t = c.get_collection(timeline=True, admin=True)
    t.ensure_index([('id', 1), ('start', 1)])


def index_warehouse(cube, fields, force=False):
    '''
    NOTE: _id key index is generated automatically by mongo
    '''
    c = get_cube(cube)
    _cube = c.get_collection(admin=True)

    fields = get_fields(cube, fields)
    result = {}
    for field in fields:
        name = '%s-tokens' % field
        if force or c.get_field_property('index', field):
            logger.info(' %s... Indexing Warehouse (%s)%s' %
                        (YELLOW, field, ENDC))
            key = [(field, -1)]
            result[field] = _cube.ensure_index(key, name=name)
        else:
            result[field] = -1
    return result


def extract(cube, **kwargs):
    logger.info(' Starting Update operation!')
    logger.info(' %sCube: %s%s' % (YELLOW, cube, ENDC))
    c = get_cube(cube)

    logger.debug('%sExtract - Start%s' % (YELLOW, ENDC))

    _fields = kwargs.get('fields')
    fields = get_fields(cube, _fields)

    if fields:
        result = {}
        for field in fields:
            kwargs['field'] = field
            logger.debug('%sField: %s%s' % (YELLOW, field, ENDC))
            result[field] = c.extract_func(**kwargs)
            logger.info('Extract - Complete: (%s.%s): %s' %
                        (cube, field, result[field]))
    else:
        result = c.extract_func(**kwargs)
        logger.info('Extract - Complete: (%s): %s' % (cube, result))

    return result


def last_known_warehouse_mtime(cube, field=None):
    '''get the last known warehouse object mtime'''
    c = get_cube(cube)
    _cube = c.get_collection()

    start = None
    if field:
        # we need to check the etl_activity collection
        spec = {'cube': cube, field: {'$exists': True}}
        doc = c._c_etl_activity.find_one(spec, ['%s.mtime' % field])
        if doc:
            start = doc[field]['mtime']
    else:
        # get the most recent _mtime of all objects in the cube
        mtime = '_mtime'
        spec = {}
        doc = _cube.find_one(spec, [mtime], sort=[(mtime, -1)])
        if doc:
            start = doc[field][mtime]

    logger.debug('... Last field mtime: %s' % start)
    return start
