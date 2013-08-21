#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from copy import copy
from datetime import datetime
from bson.objectid import ObjectId

from metrique.server.cubes import get_cube, get_etl_activity
from metrique.server.job import job_save
from metrique.server.config import mongodb

from metrique.tools import dt2ts
from metrique.tools import oid as new_oid
from metrique.tools.constants import RE_PROP

mongodb_config = mongodb()


@job_save('etl_index')
def index(cube, ensure=None, drop=None):
    '''
    :param str cube:
        name of cube (collection) to index
    :param string/list ensure:
        Either a single key or a list of (key, direction) pairs (lists)
        to ensure index on.
    :param string/list drop:
        index (or name of index) to drop
    '''
    cube = get_cube(cube, admin=True)

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


def _prep_object(obj, mtime):
    '''
    :param dict obj: dictionary that will be converted to mongodb doc
    :param int mtime: timestamp to apply as _start for objects

    Do some basic object validatation and add an _start timestamp value
    '''
    if not isinstance(obj, dict):
        raise TypeError(
            "Expected dict object, got type(%s)."
            "\nObject: %s" % (type(obj), obj))

    if '_oid' not in obj:
        raise ValueError('Object must have an _oid specified.')

    # no object should have _mtime, we use _start
    if '_mtime' in obj:
        obj['_start'] = obj.pop('_mtime')

    if '_start' not in obj:
        # add the time when the object was last manipulated,
        # if one isn't already included
        obj['_start'] = mtime

    if not isinstance(obj['_start'], (int, long, float, complex)):
        raise TypeError(
            'Expected "numerical" type, got: %s' % type(obj['_start']))

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


def _get_time_docs(_cube, oids):
    '''
    A generator that yields the docs with with _end == None in the order
    of the specfied oids.
    If such an object does not exist for a particular oid, it yields None.

    :param pymongo.collection _cube:
        cube object (pymongo collection connection)
    :param list oids:
        A list of '_oid' field values.
    '''
    time_docs = _cube.find({'_oid': {'$in': oids}, '_end': None},
                           sort=[('_oid', 1)])
    time_docs_iter = iter(time_docs)
    _oid = -1
    for oid in oids:
        # time_doc will contain first doc that has _oid >= oid,
        # it might be a document where _oid > oid
        while _oid < oid:
            try:
                time_doc = time_docs_iter.next()
                _oid = time_doc['_oid']
            except StopIteration:
                break
        if oid == _oid:
            yield time_doc
        else:
            yield None


def _save_and_snapshot(_cube, objects):
    '''
    Each object in objects must have '_oid' and '_start' fields specified
    and it can *not* have fields '_end' and '_id' specified.
    In timeline(TL), the most recent version of an object has _end == None.
    For each object this method tries to find the most recent version of it
    in TL. If there is one, if the field-values specified in the new object
    are different than those in th object from TL, it will end the old object
    and insert the new one (fields that are not specified in the new object
    are copied from the old one).
    If there is not a version of the object in TL, it will just insert it.

    :param pymongo.collection _cube:
        cube object (pymongo collection connection)
    :param list objects:
        list of dictionary-like objects
    '''
    logger.debug('... Timeline Index: Start')
    _cube.ensure_index([('_oid', 1), ('_end', 1)])
    _cube.ensure_index([('_oid', 1), ('_start', 1)])
    logger.debug('... Timeline Index: Done')

    logger.debug('... Snapshot %s objects.' % len(objects))

    # we need to sort the objects by their _oid, this is essential
    objects.sort(key=lambda x: x['_oid'])

    time_docs = _get_time_docs(_cube, [obj['_oid'] for obj in objects])

    batch_insert = []
    fields = []
    for doc in objects:
        _start = doc.pop('_start')
        fields.extend(doc.keys())

        time_doc = time_docs.next()

        save_new = False
        if time_doc is not None:
            time_doc_items = time_doc.items()
            if any(item not in time_doc_items for item in doc.iteritems()):
                _cube.update({'_id': time_doc['_id']},
                             {'$set': {'_end': _start}},
                             upsert=True, manipulate=False)
                save_new = True
                # from time_doc, we must copy the fields that are not present
                # in doc (in the case when we want to update only some of the
                # fields)
                time_doc.update(doc)
                doc = time_doc
        else:
            save_new = True

        if save_new:
            doc.update({'_id': new_oid(),
                        '_start': _start,
                        '_end': None})
            batch_insert.append(doc)

    if batch_insert:
        _cube.insert(batch_insert, manipulate=False)

    return list(set(fields) - set(['_oid']))


def _save_no_snapshot(_cube, objects):
    '''
    Save all the objects (docs) into the given cube (mongodb collection)
    Each object must have '_oid', '_start', '_end' fields.
    The '_id' field is voluntary and its presence or absence determines
    the save method (see below).

    Use `save` to overwrite the entire document with the new version
    or `insert` when we have a document without a _id, indicating
    it's a new document, rather than an update of an existing doc.

    :param pymongo.collection _cube:
        cube object (pymongo collection connection)
    :param list objects:
        list of dictionary-like objects
    '''
    # save rather than insert b/c insert would add dups (_id) docs
    # if for object's we've already stored
    # maybe 'insert' only objects which don't have
    # and _id
    logger.debug('... No snapshot %s objects.' % len(objects))

    batch = []
    fields = []
    for obj in iter(objects):
        fields.extend(obj.keys())
        if '_id' in obj:
            _cube.save(obj, manipulate=False)
        else:
            obj.update({'_id': new_oid()})
            batch.append(obj)
    if batch:
        _cube.insert(batch, manipulate=False)
    return list(set(fields))


def _save_objects(_cube, objects):
    '''
    Save all the objects (docs) into the given cube (mongodb collection)
    Each object must have '_oid' and '_start' fields.
    If an object has an '_end' field, it will be saved without snapshot,
    otherwise it will be saved with snapshot.
    The '_id' field is allowed only if the object also has the '_end' field
    and its presence or absence determines the save method.


    :param pymongo.collection _cube:
        cube object (pymongo collection connection)
    :param list objects:
        list of dictionary-like objects
    '''
    # Split the objects based on the presence of '_end' field:
    no_snap = [obj for obj in objects if '_end' in obj]
    fields1 = _save_no_snapshot(_cube, no_snap) if len(no_snap) > 0 else []
    to_snap = [obj for obj in objects if '_end' not in obj]
    fields2 = _save_and_snapshot(_cube, to_snap) if len(to_snap) > 0 else []
    return list(set(fields1 + fields2))


@job_save('etl_remove_objects')
def remove_objects(cube, ids, backup=False):
    '''
    Remove all the objects (docs) from the given cube (mongodb collection)

    :param pymongo.collection _cube:
        cube object (pymongo collection connection)
    :param list ids:
        list of object ids
    '''
    if not ids:
        return []
    elif not type(ids) in [list, tuple]:
        raise TypeError("Expected list or tuple, got type(%s): %s" %
                        (type(ids), ids))
    else:
        spec = {'_oid': {'$in': ids}}
        _cube = get_cube(cube)
        if backup:
            docs = _cube.find(spec)
            if docs:
                docs = tuple(docs)
        else:
            docs = []
        get_cube(cube).remove(spec, safe=True)
        return docs


@job_save('etl_save_objects')
def save_objects(cube, objects, update=False, mtime=None):
    '''
    :param str cube: target cube (collection) to save objects to
    :param list objects: list of dictionary-like objects to be stored
    :param boolean update: update already stored objects?
    :param datetime mtime: datetime to apply as mtime for objects
    :rtype: list - list of object ids saved

    Get a list of dictionary objects from client and insert
    or save them to the timeline.

    Apply the given mtime to all objects or apply utcnow(). _mtime
    is used to support timebased 'delta' updates.
    '''
    if not objects:
        return -1
    elif not type(objects) in [list, tuple]:
        raise TypeError("Expected list or tuple, got type(%s): %s" %
                        (type(objects), objects))

    mtime = dt2ts(mtime) if mtime else dt2ts(datetime.utcnow())

    objects = [_prep_object(obj, mtime) for obj in objects if obj]

    _cube = get_cube(cube, admin=True)

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
        oids = [o['_oid'] for o in objects]
    except KeyError:
        oids = []
    return oids


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
