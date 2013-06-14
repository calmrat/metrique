#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from datetime import datetime

from metrique.server.cubes import get_fields, get_cube
from metrique.server.job import job_save

from metrique.tools.constants import YELLOW, ENDC


@job_save('etl_index_warehouse')
def index_warehouse(cube, fields):
    '''
    NOTE: _id key index is generated automatically by mongo
    '''
    _cube = get_cube(cube, admin=True)

    result = {}
    for field in get_fields(cube, fields):
        logger.info(' %s... Indexing Warehouse (%s)%s' %
                    (YELLOW, field, ENDC))
        key = [(field, -1)]
        name = '%s-tokens' % field
        result[field] = _cube.ensure_index(key, name=name)
    return result


def _prep_object(obj, when=None):
    if not obj:
        raise ValueError("Empty object")
    elif not isinstance(obj, dict):
        raise TypeError(
            "Expected objects as dict, got type(%s)" % type(obj))
    else:
        if not when:
            when = datetime.utcnow()
        obj.update({'_mtime': when})
    return obj


@job_save('etl_save_objects')
def save_objects(cube, objects):
    if not objects:
        raise ValueError("Empty objects list")
    elif not type(objects) in [list, tuple]:
        raise TypeError("Expected list or tuple, got type(%s)" % type(objects))

    now = datetime.utcnow()
    [_prep_object(obj, now) for obj in objects]
    _cube = get_cube(cube, admin=True)
    for obj in iter(objects):
        _cube.save(obj, manipulate=False)
    return len(objects)


def _snapshot(cube, ids):
    w = get_cube(cube)
    t = get_cube(cube, admin=True, timeline=True)

    docs = w.find({'_id': {'$in': ids}}, sort=[('_id', 1)])

    logger.debug('Snapshot Timeline Index: Start')
    t.ensure_index([('current', 1), ('id', 1)])
    logger.debug('... Snapshot Timeline Index: Done')

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


@job_save('etl_snapshot')
def snapshot(cube, ids=None):
    logger.debug('Running snapshot')
    if ids is None:
        # Run on all the ids
        _cube = get_cube(cube)
        docs = _cube.find(fields=['_id'])
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
