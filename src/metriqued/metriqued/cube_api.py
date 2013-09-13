#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from itertools import chain
import logging
logger = logging.getLogger(__name__)

from metriqued.utils import get_collection, get_auth_keys
from metriqued.utils import get_mtime, get_cube_quota_count
from metriqued.utils import make_update_spec
from metriqued.utils import insert_bulk, insert_meta_docs
from metriqued.utils import exec_update_role, prepare_objects
from metriqued.utils import validate_owner_cube_objects
from metriqued.utils import cfind
from metriqued.config import role_is_valid, action_is_valid
from metriqued.config import mongodb, IMMUTABLE_DOC_ID_PREFIX
from metriqued.user_api import user_is_valid

from metriqueu.utils import dt2ts, utcnow

mongodb_config = mongodb()


def register(owner, cube):
    '''
    Client registration method

    Update the user__cube __meta__ doc with defaults

    Bump the user's total cube count, by 1
    '''
    auth_keys_spec = {'_oid': owner}
    fields = {'cube_count': 1, 'cube_quota': 1}
    doc = get_auth_keys().find_one(auth_keys_spec, fields)
    cube_quota, cube_count = get_cube_quota_count(doc)

    if not (cube_quota <= -1 or (cube_quota - cube_count) > 0):
        raise RuntimeError(
            "quota_depleted (%s of %s)" % (cube_quota, cube_count))

    _cube = get_collection(owner, cube, admin=True, create=True)
    insert_meta_docs(_cube, owner)

    _cc = _cube.database.collection_names()
    real_cc = sum([1 for c in _cc if c.startswith(owner)])
    update = {'$set': {'cube_count': real_cc}}
    get_auth_keys().update(auth_keys_spec, update, safe=True)
    # do something with the fact that we know if it was
    # successful or not?
    return True


def update_role(owner, cube, username, action, role):
    if owner == username and username != 'admin':
        # allow admin to do whatever (eg, disable access)
        # but don't allow the owner to shoot themselves
        # in the foot
        raise RuntimeError("cannot modify cube.owner's role")
    role_is_valid(role)
    action_is_valid(action)
    user_is_valid(username)
    _cube = get_collection(owner, cube)
    result = exec_update_role(_cube, username, role, action)
    if result:
        return True
    else:
        return False


def drop_cube(owner, cube):
    '''
    :param str cube: target cube (collection) to save objects to

    Wraps pymongo's drop() for the given cube (collection)
    '''
    get_collection(owner, cube, admin=True).drop()
    return True


def index(owner, cube, ensure=None, drop=None):
    '''
    :param str cube:
        name of cube (collection) to index
    :param string/list ensure:
        Either a single key or a list of (key, direction) pairs (lists)
        to ensure index on.
    :param string/list drop:
        index (or name of index) to drop
    '''
    _cube = get_collection(owner, cube, admin=True)

    if drop is not None:
        # when drop is a list of tuples, the json
        # serialization->deserialization process leaves us with a list of
        # lists, so we need to convert it back to a list of tuples.
        drop = map(tuple, drop) if isinstance(drop, list) else drop

        # FIXME: CHECK THAT DROP DOES NOT CONTAIN ANY _id or _oid_...
        # SYSTEM DEFAULT (IMMUTABLE!) INDEXES!

        _cube.drop_index(drop)

    if ensure is not None:
        # same as for drop:
        ensure = map(tuple, ensure) if isinstance(ensure, list) else ensure
        _cube.ensure_index(ensure)

    return _cube.index_information()


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
    logger.debug('... To snapshot: %s objects.' % len(objects))

    # .update() all oid version with end:null to end:new[_start]
    # then insert new

    _starts = dict([(doc['_oid'], doc['_start']) for doc in objects])
    _oids = _starts.keys()

    _oid_spec = {'$in': _oids}
    _end_spec = None
    fields = {'_id': 1, '_oid': 1}
    current_docs = cfind(_cube, _oid=_oid_spec, _end=_end_spec, fields=fields)
    current_ids = dict([(doc['_id'], doc['_oid']) for doc in current_docs])

    for _id, _oid in current_ids.items():
        update = make_update_spec(_starts[_oid])
        _cube.update({'_id': _id}, update, multi=False)
    logger.debug('... "snapshot" saving %s objects.' % len(objects))
    insert_bulk(_cube, objects)


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
    logger.debug('... "no snapshot" saving %s objects.' % len(objects))
    insert_bulk(_cube, objects)


def _save_objects(_cube, fields, no_snap, to_snap, mtime):
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
    _save_no_snapshot(_cube, no_snap) if len(no_snap) > 0 else []
    _save_and_snapshot(_cube, to_snap) if len(to_snap) > 0 else []
    # update cube's mtime doc
    _cube.save({'_id': '__mtime__', 'value': mtime})
    # return object ids saved
    return [o['_oid'] for o in chain(no_snap, to_snap)]


def save_objects(owner, cube, objects, mtime=None):
    '''
    :param str owner: target owner's cube
    :param str cube: target cube (collection) to save objects to
    :param list objects: list of dictionary-like objects to be stored
    :param datetime mtime: datetime to apply as mtime for objects
    :rtype: list - list of object ids saved

    Get a list of dictionary objects from client and insert
    or save them to the timeline.

    Apply the given mtime to all objects or apply utcnow(). _mtime
    is used to support timebased 'delta' updates.
    '''
    validate_owner_cube_objects(owner, cube, objects)

    mtime = dt2ts(mtime) if mtime else utcnow()

    _cube = get_collection(owner, cube)

    current_mtime = get_mtime(_cube)
    if current_mtime > mtime:
        raise ValueError(
            "invalid mtime (%s); "
            "must be > current mtime (%s)" % (mtime, current_mtime))

    no_snap, to_snap, fields, _oids = prepare_objects(_cube, objects, mtime)

    objects = no_snap + to_snap
    olen = len(objects)

    if not olen:
        logger.debug('[%s.%s] No NEW objects to save' % (owner, cube))
        return []
    else:
        logger.debug('[%s.%s] Saved %s objects' % (owner, cube, olen))
        result = _save_objects(_cube, fields, no_snap, to_snap, mtime)
        return result


def _contains_immutable_doc_id(ids):
    return any([True for x in ids if x.startswith(IMMUTABLE_DOC_ID_PREFIX)])


def remove_objects(owner, cube, ids, backup=False):
    '''
    Remove all the objects (docs) from the given cube (mongodb collection)

    :param pymongo.collection _cube:
        cube object (pymongo collection connection)
    :param list ids:
        list of object ids
    '''
    if not ids:
        logger.debug('REMOVE: no ids provided')
        return []
    elif not isinstance(ids, list):
        raise TypeError("Expected list, got %s: %s" %
                        (type(ids), ids))
    elif _contains_immutable_doc_id(ids):
        raise ValueError("Object ids with '%s' prefix "
                         "are immutable" % IMMUTABLE_DOC_ID_PREFIX)
    else:
        _oid_spec = {'$in': ids}
        if backup:
            docs = cfind(_oid=_oid_spec)
            if docs:
                docs = tuple(docs)
        else:
            docs = []

        full_spec = {'_oid': {'$in': ids}}
        try:
            get_collection(owner, cube, admin=True).remove(full_spec,
                                                           safe=True)
        except Exception as e:
            raise RuntimeError("Failed to remove docs: %s" % e)
        else:
            return docs


def stats(owner, cube):
    _cube = get_collection(owner, cube)
    mtime = get_mtime(_cube)
    stats = dict(cube=cube, mtime=mtime)
    return stats
