#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from bson import SON
import logging
logger = logging.getLogger(__name__)
import os
import pickle
import pql
from tornado.web import HTTPError

from metriqued.config import mongodb
from metriqueu.utils import utcnow, jsonhash, batch_gen

OBJECTS_MAX_BYTES = 16777216
EXISTS_SPEC = {'$exists': 1}
MTIME_SPEC = {'_id': '__mtime__'}
MTIME_FIELDS = {'value': 1, '_id': -1}
BASE_INDEX = [('_start', -1), ('_end', -1), ('_oid', -1), ('_hash', 1)]

mongodb_config = mongodb()

AUTH_KEYS = mongodb_config.c_auth_keys


def get_collection(owner, cube, admin=False, create=False):
    ''' return back a mongodb connection to give cube collection '''
    collection = '%s__%s' % (owner, cube)
    if collection in mongodb_config.db_timeline_data.db.collection_names():
        if create:
            raise HTTPError(
                409, "collection already exists: %s" % collection)
    else:
        if not create:
            raise HTTPError(
                412, "collection does not exist: %s" % collection)
    if admin:
        return mongodb_config.db_timeline_admin.db[collection]
    else:
        return mongodb_config.db_timeline_data.db[collection]


def get_auth_keys():
    return AUTH_KEYS


def get_cube_quota_count(doc):
    if doc:
        cube_quota = doc.get('cube_quota', None)
        cube_count = doc.get('cube_count', None)
    else:
        cube_quota = None
        cube_count = None
    if cube_quota is None:
        cube_quota = 0  # FIXME: SET AS CONFIGURABLE DEFAULT
    if cube_count is None:
        cube_count = 0  # FIXME: SET AS CONFIGURABLE DEFAULT
    cube_quota = int(cube_quota)
    cube_count = int(cube_count)
    return cube_quota, cube_count


def get_mtime(_cube):
    return _cube.find_one(MTIME_SPEC, MTIME_FIELDS)['value']


def ensure_index_base(owner, cube):
    _cube = get_collection(owner, cube)
    _cube.ensure_index(BASE_INDEX)


def cfind(_cube, _start=None, _end=None, _oid=None, _hash=None,
          fields=None, spec=None, sort=None, **kwargs):
    # note, to force limit; use __getitem__ like...
    # docs_limited_50 = cfind(...)[50]
    # SEE:
    # http://api.mongodb.org/python/current/api/pymongo/cursor.html
    # section #pymongo.cursor.Cursor.__getitem__
    # trying to use limit=... fails to work given our index
    index_spec = make_index_spec(_start, _end, _oid, _hash)
    if spec:
        index_spec.update(spec)
    result = _cube.find(index_spec, fields,
                        sort=sort, **kwargs).hint(BASE_INDEX)
    return result


def strip_split(item):
    if isinstance(item, basestring):
        return [s.strip() for s in item.split(',')]
    elif item is None:
        return []
    elif not isinstance(item, (list, tuple)):
        raise ValueError('Expected a list/tuple')
    else:
        # nothing to do here...
        return item


def make_update_spec(_start):
    return {'$set': {'_end': _start}}


def make_index_spec(_start=None, _end=None, _oid=None, _hash=None):
    _start = EXISTS_SPEC if not _start else _start
    _end = EXISTS_SPEC if not _end else _end
    _oid = EXISTS_SPEC if not _oid else _oid
    _hash = EXISTS_SPEC if not _hash else _hash
    spec = SON([('_start', _start),
                ('_end', _end),
                ('_oid', _oid),
                ('_hash', _hash)])
    return spec


def exec_update_role(_cube, username, role, action):
    spec = {'_id': role}
    update = {'$%s' % action: {'value': username}}
    _cube.update(spec, update, safe=True, multi=False)
    return True


def estimate_obj_size(obj):
    return len(pickle.dumps(obj))


def insert_bulk(_cube, docs, size=-1):
    # little reason to batch insert...
    # http://stackoverflow.com/questions/16753366
    # and after testing, it seems splitting things
    # up more slows things down.
    if size <= 0:
        _cube.insert(docs, manipulate=False)
    else:
        for batch in batch_gen(docs, size):
            _cube.insert(batch, manipulate=False)


def insert_meta_docs(_cube, owner):
    now_utc = utcnow()
    meta_docs = [
        {'_id': '__created__', 'value': now_utc},
        {'_id': '__mtime__', 'value': now_utc},
        {'_id': '__owner__', 'value': owner},
        {'_id': '__read__', 'value': [owner]},
        {'_id': '__write__', 'value': [owner]},
        {'_id': '__admin__', 'value': [owner]},
    ]
    _cube.insert(meta_docs, safe=True)


def validate_owner_cube_objects(owner, cube, objects):
    if not (owner and cube and objects):
        raise ValueError('owner, cube, objects required')
    elif not isinstance(objects, list):
        raise TypeError("Expected list, got %s" % type(objects))
    elif not all([1 if isinstance(obj, dict) else 0 for obj in objects]):
        raise TypeError(
            "Expected dict object, got type(%s)."
            "\nObject: %s" % (type(obj), obj))
    elif not all([1 if '_oid' in obj else 0 for obj in objects]):
        raise ValueError(
            'Object must have an _oid specified. Got: \n%s' % obj)
    else:
        return True


def prepare_objects(_cube, objects, mtime):
    '''
    :param dict obj: dictionary that will be converted to mongodb doc
    :param int mtime: timestamp to apply as _start for objects

    Do some basic object validatation and add an _start timestamp value
    '''
    olen_r = len(objects)
    logger.debug('Received %s objects' % olen_r)

    _hashes = set()
    _oids = set()
    for obj in objects:
        _start = None
        _end = None
        # if we have _id, it will be included in the hash calculation
        # if not, it will be added automatically by mongo on insert

        if '_start' in obj:
            if not isinstance(obj['_start'], (int, float)):
                raise TypeError(
                    'Expected int/float type, got: %s' % type(obj['_start']))
            _start = obj['_start']
            del obj['_start']
            _with_start = False
        else:
            # use the time when the obj version was captured as "creation date"
            _start = mtime
            _with_start = True

        if '_end' in obj:
            if not _with_start:
                raise ValueError("objects with _end must have _start")
            if not (isinstance(obj['_end'], (int, float))):
                raise TypeError(
                    'Expected int/float type, got: %s' % type(obj['_end']))
            _end = obj['_end']
            del obj['_end']

        _hash = jsonhash(obj)

        if '_hash' in obj and _hash != obj['_hash']:
            raise ValueError("object hash mismatch")
        else:
            obj['_hash'] = _hash
            _hashes.add(_hash)

        # add back _start and _end properties
        obj['_start'] = _start
        obj['_end'] = _end

        if '_oid' not in obj:
            obj['_oid'] = _hash
        _oids.add(obj['_oid'])

    # FIXME: refactor this so we split the _hashes
    # mongodb lookups iterate across 16M max
    # spec docs...
    # get the estimate size, as follows
    #est_size_hashes = estimate_obj_size(_hashes)

    # Get dup hashes and filter objects to include only non dup hashes
    _hash_spec = {'$in': list(_hashes)}

    index_spec = make_index_spec(_hash=_hash_spec)
    docs = _cube.find(index_spec, {'_hash': 1, '_id': -1}).hint(BASE_INDEX)
    _dup_hashes = set([doc['_hash'] for doc in docs])
    objects = [obj for obj in objects if obj['_hash'] not in _dup_hashes]

    olen_n = len(objects)
    olen_diff = olen_r - olen_n
    logger.debug('Found %s Existing (current) objects' % (olen_diff))
    logger.debug('Saving %s NEW objects' % olen_n)

    # get list of objects which have other versions
    _oid_spec = {'$in': list(_oids)}
    index_spec = make_index_spec(_oid=_oid_spec)
    docs = _cube.find(index_spec, {'_oid': 1, '_id': -1}).hint(BASE_INDEX)
    _known_oids = set([doc['_oid'] for doc in docs])

    no_snap = [obj for obj in objects if not obj.get('_oid') in _known_oids]
    to_snap = [obj for obj in objects if obj.get('_oid') in _known_oids]
    return no_snap, to_snap, _oids


def parse_pql_query(query):
    if not query:
        return {}
    if not isinstance(query, basestring):
        raise TypeError("query expected as a string")
    pql_parser = pql.SchemaFreeParser()
    try:
        spec = pql_parser.parse(query)
    except Exception as e:
        raise ValueError("Invalid Query (%s):\n%s" % (query, str(e)))
    logger.debug("PQL Query: %s" % query)
    logger.debug('Query: %s' % spec)
    return spec


def get_pid_from_file(pid_file):
    pid_file = os.path.expanduser(pid_file)
    if os.path.exists(pid_file):
        pid = int(open(pid_file).readlines()[0])
    else:
        pid = 0
    return pid


def remove_pid_file(pid_file, quiet=True):
    try:
        os.remove(pid_file)
    except OSError:
        if quiet:
            pass
        else:
            raise
