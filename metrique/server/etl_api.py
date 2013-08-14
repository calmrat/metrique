#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from copy import copy
from datetime import datetime
from dateutil.parser import parse as dt_parse
from bson.objectid import ObjectId
import pytz

from metrique.server.cubes import get_cube, get_etl_activity
from metrique.server.job import job_save
from metrique.server.config import mongodb

from metrique.tools.constants import RE_PROP, RE_OBJECT_ID

mongodb_config = mongodb()


@job_save('etl_index')
def index(cube, db, ensure=None, drop=None):
    '''
    :param str cube:
        name of cube (collection) to index
    :param string db:
        'timeline' or 'warehouse'
    :param string/list ensure:
        Either a single key or a list of (key, direction) pairs (lists)
        to ensure index on.
    :param string/list drop:
        index (or name of index) to drop
    '''
    if db == 'timeline':
        timeline = True
    elif db == 'warehouse':
        timeline = False
    else:
        raise ValueError('db must be either "timeline" or "warehouse".')

    cube = get_cube(cube, admin=True, timeline=timeline)

    if drop is not None:
        # when drop is a list of tuples, the json
        # serialization->deserialization process leaves us with a list of
        # lists, so we need to convert it back to a list of tuples.
        drop = map(tuple, drop) if isinstance(drop, list) else drop
        cube.drop_index(drop)

    if ensure is not None:
        # same as for drop:
        ensure = map(tuple, ensure) if isinstance(ensure, list) else ensure
        cube.ensure_index(ensure)

    return cube.index_information()


def hash_obj(obj):
    '''
    calculate the objects hash based on all field values,
    not including adding additional meta data, like, _id
    IF its a 'randomly generated' ObjectId and _mtime, etc.
    '''
    _obj = copy(obj)
    if '_id' in obj and type(obj['_id']) is ObjectId:
        del _obj['_id']
    return hash(frozenset(obj.items()))


def _prep_object(obj, mtime, timeline):
    '''
    :param dict obj: dictionary that will be converted to mongodb doc
    :param datetime mtime: datetime to apply as mtime for objects

    Do some basic object validatation and add
    an _mtime datetime value
    '''
    if not obj:
        return {}
    elif not isinstance(obj, dict):
        raise TypeError(
            "Expected dict object, got type(%s)."
            "\nObject: %s" % (type(obj), obj))
    else:
        # DISABLED
        # FIXME: test what happens if a object contains nested structures/dicts
        #obj['_hash'] = hash_obj(obj)

        # if the object has _id and it looks like ObjectId then convert it
        if '_id' in obj:
            if isinstance(obj['_id'], str) and RE_OBJECT_ID.match(obj['_id']):
                obj['_id'] = ObjectId(obj['_id'])
        elif not timeline:
            # generate and apply a mongodb (bson) ObjectId if
            # one doesn't already exist.
            # Usually, this is generated serverside, but we
            # generate it here so we know those ids without
            # having to wait for the round trip back from save_objects
            # and the objectids generated here should be unique...
            # downside is we push more data across the network, but
            # we'd have to pull it back anyway, if generated serverside
            # since ultimately we want to return back the object _ids list
            # to client calling .save_objects()
            obj['_id'] = ObjectId()
        # add the time when the object was last manipulated
        obj.update({'_mtime': mtime})
        return obj


def _update_objects(_cube, objects):
    '''
    :param cube: cube object (pymongo collection connection)
    :param list objects: list of dictionary-like objects

    Update all the objects (docs) into the given cube (mongodb collection)

    Use `$set` so we only update/overwrite the fields in the given docs,
    rather than overwriting the whole document.
    '''
    fields = []
    for obj in objects:
        fields.extend(obj.keys())
        _cube.update({'_id': obj.pop('_id')},
                     {'$set': obj}, upsert=True,
                     manipulate=False)
    return list(set(fields))


def _save_objects(_cube, objects):
    '''
    :param cube: cube object (pymongo collection connection)
    :param list objects: list of dictionary-like objects

    Save all the objects (docs) into the given cube (mongodb collection)

    Use `save` to overwrite the entire document with the new version
    or `insert` when we have a document without a _id, indicating
    it's a new document, rather than an update of an existing doc.
    '''
    # save rather than insert b/c insert would add dups (_id) docs
    # if for object's we've already stored
    # maybe 'insert' only objects which don't have
    # and _id
    batch = []
    fields = []
    for obj in iter(objects):
        fields.extend(obj.keys())
        if '_id' in obj:
            _cube.save(obj, manipulate=False)
        else:
            batch.append(obj)
    if batch:
        _cube.insert(batch, manipulate=False)
    return list(set(fields))


@job_save('etl_save_objects')
def save_objects(cube, objects, update=False, timeline=False,
                 mtime=None):
    '''
    :param str cube: target cube (collection) to save objects to
    :param list objects: list of dictionary-like objects to be stored
    :param boolean update: update already stored objects?
    :param boolean timeline: target db to save objects is timeline
    :param datetime mtime: datetime to apply as mtime for objects
    :rtype: list - list of object ids saved

    Get a list of dictionary objects from client and insert
    or save them to the warehouse or timeline.

    Apply the given mtime to all objects or apply utcnow(). _mtime
    is used to support timebased 'delta' updates.
    '''
    if not objects:
        return -1
    elif not type(objects) in [list, tuple]:
        raise TypeError("Expected list or tuple, got type(%s): %s" %
                        (type(objects), objects))
    elif not mtime:
        mtime = datetime.utcnow()

    objects = [_prep_object(obj, mtime, timeline) for obj in objects if obj]

    _cube = get_cube(cube, admin=True, timeline=timeline)

    if update:
        fields = _update_objects(_cube, objects)
    else:
        fields = _save_objects(_cube, objects)

    logger.debug('[%s] Saved %s objects' % (cube, len(objects)))

    # store info about which cube.fields got updated and when
    _etl = etl_activity_update(cube, fields, mtime)
    logger.debug('ETL Activity Update: %s' % _etl)

    # return object ids saved
    try:
        ids = [o['_id'] for o in objects]
    except KeyError:
        ids = []
    return ids


def etl_activity_update(cube, fields, mtime):
    '''
    :param str cube: target cube (collection) to save objects to
    :param list fields: list fields updated
    :param datetime mtime: datetime to apply as mtime for objects

    Update etl_activity collection in metrique mongodb with
    information about which cube.fields have been manipulated
    and when.
    '''
    fields = list(set(fields))
    spec = {'_id': cube}
    mtimes = dict([(f, mtime) for f in fields if not RE_PROP.match(f)])
    mtimes.update({'_mtime': mtime})
    update = {'$set': mtimes}
    return get_etl_activity().update(spec, update, upsert=True, safe=True)


@job_save('etl_drop')
def drop(cube):
    '''
    :param str cube: target cube (collection) to save objects to

    Wraps pymongo's drop() for the given cube (collection)
    '''
    return get_cube(cube).drop()


def _timeline_batch_insert(t, docs, size=1):
    '''
    :param timeline pymongo.collection: timeline collection
    :param list objects: list of dictionary-like objects to be stored
    :param int size: target size for batch inserts

    Simply batch insert into the timeline if the number of
    docs exceeds the given size parameter.
    '''
    if len(docs) >= size:
        t.insert(docs)
        return []
    else:
        return docs


def _prep_timeline_obj(obj, _id, _mtime):
    '''
    :param dict doc: dictionary object targetted to be saved to timeline
    :param str _id: object id
    :param datetime _mtime: datetime of when the object was last modified

    Update a doc with timeline specific properties. Specifically,
    `_oid` (object id), `_mtime` (datetime object was updated)
    and default `_end` of None, which signifies this object is
    the 'current' version, which matches the same object's
    state in warehouse.
    '''
    # normalize _mtime to be timezone aware... assume utc
    tzaware = hasattr(_mtime, 'tzinfo') and _mtime.tzinfo
    if _mtime and not tzaware:
        _mtime = pytz.UTC.localize(_mtime)
    obj.update({'_oid': _id,
                '_start': _mtime,
                '_end': None})
    return obj


def _snapshot(cube, ids):
    w = get_cube(cube)
    t = get_cube(cube, admin=True, timeline=True)

    docs = w.find({'_id': {'$in': ids}}, sort=[('_id', 1)])
    time_docs = t.find({'_end': None, '_oid': {'$in': ids}},
                       sort=[('_oid', 1)])
    time_docs_iter = iter(time_docs)
    _oid = -1

    batch_size = mongodb_config.tl_batch_size
    batch_insert = []
    for doc in docs:
        _id = doc.pop('_id')
        _mtime = doc.pop('_mtime')
        if not isinstance(_mtime, datetime):
            # in the off case that we don't already have a datetime
            # object, try to parse it as a string...
            try:
                _mtime = dt_parse(_mtime).replace(tzinfo=pytz.UTC)
            except Exception:
                raise TypeError(
                    'Expected datetime object/string, got: %s' % _mtime)

        # time_doc will contain first doc that has _oid >= _id,
        # it might be a document where _oid > _id
        while _oid < _id:
            try:
                time_doc = time_docs_iter.next()
                _oid = time_doc['_oid']
            except StopIteration:
                break

        if _id == _oid:
            time_doc_items = time_doc.items()
            if any(item not in time_doc_items for item in doc.iteritems()):
                batch_insert.append(_prep_timeline_obj(doc, _id, _mtime))
                spec_now = {'_id': time_doc['_id']}
                update_now = {'$set': {'_end': _mtime}}
                t.update(spec_now, update_now, upsert=True)
        else:
            batch_insert.append(_prep_timeline_obj(doc, _id, _mtime))

        batch_insert = _timeline_batch_insert(t, batch_insert, batch_size)

    # insert the last few remaining docs in batch_insert that
    # are less than the max batch size (eg, 1000) not already inserted
    _timeline_batch_insert(t, batch_insert, 1)


@job_save('etl_snapshot')
def snapshot(cube, ids=None):
    '''
    :param str cube: target cube (collection) to save objects to
    :param list, string ids: list or csv string of object ids to snap

    Run a snapshot against the given cube. Essentially, find all
    objects in the warehouse that differ from the most recent (_end: None)
    objects in timeline. Dump full copies of the objects into timeline.
    '''
    logger.debug('Running snapshot')
    _cube = get_cube(cube)

    t = get_cube(cube, admin=True, timeline=True)
    logger.debug('... Timeline Index: Start')
    t.ensure_index([('_end', 1), ('_oid', 1)])
    t.ensure_index([('_oid', 1), ('_start', 1)])
    logger.debug('... Timeline Index: Done')

    if ids is None:
        # Run on all the ids
        docs = _cube.find(fields=['_id'])
        logger.debug('... Found %s docs' % docs.count())

        bsize = mongodb_config.snapshot_batch_size
        ids_to_snapshot = []
        for done, doc in enumerate(docs):
            ids_to_snapshot.append(doc['_id'])
            if (done + 1) % bsize == 0:
                _snapshot(cube, ids_to_snapshot)
                ids_to_snapshot = []
                logger.debug('... %s done' % (done + 1))
        _snapshot(cube, ids_to_snapshot)
        logger.debug('... %s done' % (done + 1))
    elif type(ids) is list:
        _snapshot(cube, ids)
    elif isinstance(ids, basestring):
        ids = map(int, ids.split(','))
        _snapshot(cube, ids)
    else:
        raise ValueError(
            "ids expected to be list or csv string. Got: %s" % type(ids))
