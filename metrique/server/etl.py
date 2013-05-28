#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from bson.objectid import ObjectId
from datetime import datetime
from time import time
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


def insert_doc(collection, new_doc):
    return collection.insert(new_doc)


def update_doc(collection, spec, update):
    return collection.update(spec, update, upsert=True)


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
    for field, tokens in obj.iteritems():
        save_doc(cube, field, tokens, obj[_id])
    return 1


def snapshot_docs(cube, ids):
    c = get_cube(cube)
    w = c.get_collection(admin=False, timeline=False)
    t = c.get_collection(admin=True, timeline=True)
    docs = w.find({'_id': {'$in': ids}}, sort=[('_id', 1)])
    time_docs = t.find({'current': True, 'id': {'$in': ids}},
                       sort=[('id', 1)])
    time_docs_iter = iter(time_docs)
    tid = 0

    for doc in docs:
        _id = doc.pop('_id')
        _mtime = doc.pop('_mtime')
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
                       'end': _mtime,
                       'current': True}
            t.insert(new_doc)


def snapshot(cube):
    logger.debug('Running snapshot')
    c = get_cube(cube)
    start_time = time()
    w = c.get_collection(admin=False, timeline=False)
    t = c.get_collection(admin=True, timeline=True)
    t.ensure_index([('id', 1)])
    docs = w.find(fields=['_id'])
    logger.debug('Found %s docs' % docs.count())

    ids_to_snapshot = []
    for done, doc in enumerate(docs):
        ids_to_snapshot.append(doc['_id'])
        if done % 100000 == 0:
            logger.debug(' ... %s req' % done)
            snapshot_docs(cube, ids_to_snapshot)
            ids_to_snapshot = []
            logger.debug(' ... %s done' % done)
    snapshot_docs(cube, ids_to_snapshot)

    end_time = time() - start_time
    logger.debug(' ... %s done' % done)
    logger.debug('Snapshot finished in %.2f seconds.' % end_time)


def activity_history_import(cube):
    logger.debug('Running activity history import')
    c = get_cube(cube)
    start_time = time()
    h = c.get_collection(timeline=False, admin=False,
                         cube='%s_activity' % c.name)
    t = c.get_collection(timeline=True, admin=True)
    t.ensure_index([('id', 1), ('start', 1)])
    time_docs = t.find({'id': {'$gte': 800000}},
                       sort=[('id', 1), ('start', 1)])
    logger.debug('Found %s docs in timeline.' % time_docs.count())

    # Dictionary of field_id: field_name
    fieldmap = defaultdict(lambda: '')
    for field in c.fields:
        field_id = c.get_field_property('what', field)
        if field_id is not None:
            fieldmap[field_id[1]] = field

    last_doc_id = -1
    for time_doc in time_docs:
        tid = time_doc['id']
        if tid != last_doc_id:
            last_doc_id = tid
            # we want to update only the oldest version of the object
            activities = h.find({'id': tid}, sort=[('when', -1)])
            batch_updates = [time_doc]
            for act in activities:
                # We want to consider only activities that happend before
                # the oldest version of the object from the timeline.
                when = act['when']
                if not (when < time_doc['start']):
                    continue

                last_doc = batch_updates[-1]
                field = fieldmap[act['what']]
                # We ignore the fields that aren't in the timeline
                if field in last_doc['fields']:
                    batch_updates.pop()
                    if last_doc['end'] == when:
                        #part of the same change as the activity before
                        new_doc = last_doc
                        last_doc = batch_updates.pop()
                    else:
                        new_doc = {'fields': deepcopy(last_doc['fields']),
                                   'id': tid,
                                   'start': when,
                                   'end': when,
                                   'current': False}
                    last_doc['start'] = when
                    inconsistent = False
                    time_val = last_doc['fields'][field]
                    type_ = c.get_field_property('type', field)
                    added = act['added']
                    removed = act['removed']
                    if c.get_field_property('container', field):
                        if added is not None:
                            added = type_cast(added.split(','), type_)
                        if removed is not None:
                            removed = type_cast(removed.split(','), type_)
                        if new_doc['fields'][field] is None:
                            new_doc['fields'][field] = []
                        if added is not None:
                            added_list = added if (type(added) is
                                                   list) else [added]
                            for ad in added_list:
                                if ad in new_doc['fields'][field]:
                                    new_doc['fields'][field].remove(ad)
                                else:
                                    inconsistent = True
                        if removed is not None:
                            removed_list = removed if (type(removed) is
                                                       list) else [removed]
                            for rem in removed_list:
                                new_doc['fields'][field].append(rem)
                    else:
                        added = type_cast(added, type_)
                        removed = type_cast(removed, type_)
                        new_doc['fields'][field] = removed
                        if added != time_val:
                            inconsistent = True
                    # Check if the object has the correct field value.
                    if inconsistent:
                        logger.warn('Inconsistency: %s %s: %s -> %s, '
                                    'object has %s.' % (
                                        tid, field, removed,
                                        added, time_val))
                        if 'corrupted' not in new_doc:
                            new_doc['corrupted'] = {}
                        new_doc['corrupted'][field] = added
                    # Add the objects to the batch
                    batch_updates.append(last_doc)
                    batch_updates.append(new_doc)
            if len(batch_updates) > 1:
                # make the batch update
                doc = batch_updates[0]
                spec_now = {'_id': doc['_id']}
                update_now = {'$set': {'start': doc['start']}}
                t.update(spec_now, update_now, upsert=True)
                for doc in batch_updates[1:]:
                    t.insert(doc)
                #logger.warn('Imported: %s' % doc['id'])

    end_time = time() - start_time
    logger.debug('Import finished in %.2f seconds.' % end_time)


def index_timeline(cube):
    logger.debug(" ... Indexing Timeline)")
    c = get_cube(cube)
    _cube = c.get_collection(timeline=True, admin=True)
    keys = [('field', 1), ('id', -1), ('mtime', -1)]
    result = {}
    for tup in keys:
        name, direction = tup
        result = _cube.ensure_index(tup, name=name)
        result.update({name: result})
    return result


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


def extract(cube, index=False, **kwargs):
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
                        (cube, field, result))
        if index:
            index_warehouse(cube, fields)
    else:
        result = c.extract_func(**kwargs)
        logger.info('Extract - Complete: (%s): %s' % (cube, result))
        if index:
            index_warehouse(cube, '__all__')

    return result


def last_known_warehouse_mtime(cube, field=None):
    '''get the last known warehouse object mtime'''
    c = get_cube(cube)

    if field:
        spec = {field: {'$exists': True}}
    else:
        spec = {}
    doc = c.get_collection().find_one(spec, ['_mtime'], sort=[('_mtime', -1)])
    if doc:
        start = doc['_mtime']
    else:
        start = None
    logger.debug('... Last field mtime: %s' % start)
    return start
