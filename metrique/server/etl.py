#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from bson.objectid import ObjectId
from datetime import datetime
from time import time
from copy import deepcopy

from metrique.server.drivers.drivermap import get_cube, get_fields

from metrique.tools.constants import UTC, YELLOW, ENDC


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


def snapshot(cube):
    logger.debug('Running snapshot')
    c = get_cube(cube)
    start_time = time()
    w = c._db_warehouse_data
    t = c._db_timeline_admin
    t.ensure_index([('id', 1)])
    # find all docs in Warehouse, sort by _id
    docs = w.find(sort=[('_id', 1)])
    # find all current docs in Timeline, sort by id:
    time_docs = t.find({'current': True}, sort=[('id', 1)])
    time_docs_iter = iter(time_docs)
    logger.debug('Found %s docs' % docs.count())
    it, tid, it_bound = 0, -1, time_docs.count()
    done = 0

    for doc in docs:
        if done % 100000 == 0:
            logger.debug(' ... %s done' % done)
        done += 1
        _id = doc['_id']
        while tid < _id and it < it_bound:
            try:
                time_doc = time_docs_iter.next()
                tid = time_doc['id']
                it += 1
            except StopIteration:
                break

        store_new_doc = False
        if _id == tid:
            if doc['fields'] != time_doc['fields']:
                store_new_doc = True
                # update end to now
                _id = time_doc['_id']
                spec_now = {'_id': _id}
                update_now = {'$set': {'current': False},
                              '$set': {'end': doc['mtime']}}
                update_doc(t, spec_now, update_now)
        else:
            store_new_doc = True

        if store_new_doc:
            new_doc = {'fields': doc['fields'],
                       'id': _id,
                       'start': doc['mtime'],
                       'end': doc['mtime'],
                       'current': True}
            insert_doc(t, new_doc)
    end_time = time() - start_time
    logger.debug(' ... %s done' % done)
    logger.debug('Snapshot finished in %.2f seconds.' % end_time)


def activity_history_import(cube):
    logger.debug('Running activity history import')
    c = get_cube(cube)

    start_time = time()
    h = c.get_collection(timeline=False, admin=False,
                         name='%s_activity' % c.name)
    t = c.get_collection(timeline=True, admin=True)
    t.ensure_index([('id', 1), ('start', 1)])
    time_docs = t.find({'id': {'$lte': 10}},
                       sort=[('id', 1), ('start', 1)])
    logger.debug('Found %s docs in timeline.' % time_docs.count())

    last_doc_id = -1
    for time_doc in time_docs:
        # time_doc is the oldest version of the object in the timeline
        tid = time_doc['id']
        if tid != last_doc_id:
            last_doc_id = tid
        activities = h.find({'fields.id.tokens.token': tid},
                            sort=[('fields.when.tokens.token', -1)])
        batch_updates = [time_doc]
        for act in activities:
            # We want to consider only activities that happend before the
            # oldest version of the object from the timeline.
            when = act['fields']['when']['tokens'][0]['token']
            if not (when < time_doc['start']):
                continue

            last_doc = batch_updates[-1]
            field_id = act['fields']['what']['tokens'][0]['token']
            field = c.fieldmap[field_id]
            # We ignore the fields that aren't in the timeline
            if field in last_doc['fields']:
                logger.warn('id: %s, when: %s' % (tid, when))
                logger.warn('Field: %s, added: %s, removed: %s\n' % (
                    field, act['fields']['added'],
                    act['fields']['removed']))
                #FIXME this is a hack
                activity_added_tokens = act['fields']['added']['tokens'][0]['token']
                time_tokens = last_doc['fields'][field]['tokens'][0]['token']
                # Now we have to check if the object has the correct field
                # value.
                if activity_added_tokens != time_tokens:
                    batch_updates = []
                    logger.warn('Inconsistency: The object with id %s has'
                                'in the field %s the value %s.'
                                'The activity that'
                                'happened before has %s' % (
                                    tid, field, time_tokens,
                                    activity_added_tokens))
                    break
                batch_updates.pop()
                if last_doc['end'] == when:
                    #part of the same change as the activity before
                    new_doc = last_doc
                    last_doc = batch_updates.pop()
                else:
                    new_doc = {'fields': deepcopy(last_doc['fields']),
                               'id': tid,
                               'start': 0,
                               'end': when,
                               'current': False}
                last_doc['start'] = when
                new_doc['fields'][field]['tokens'] = act['fields']['removed']['tokens']
                batch_updates.append(last_doc)
                batch_updates.append(new_doc)
        if len(batch_updates) > 1:
            # make the batch update
            doc = batch_updates[0]
            spec_now = {'_id': doc['_id']}
            update_now = {'$set': {'start': doc['start']}}
            update_doc(t, spec_now, update_now)
            for doc in batch_updates[1:]:
                insert_doc(t, doc)

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
            logger.info(' %s... Indexing Warehouse (%s)%s' % (YELLOW, field, ENDC))
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
            logger.info('Extract - Complete: (%s.%s): %s' % (cube, field, result[field]))
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
