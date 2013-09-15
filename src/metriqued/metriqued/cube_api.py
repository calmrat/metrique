#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from bson import ObjectId
import logging
logger = logging.getLogger(__name__)
from itertools import chain
from tornado.web import authenticated

from metriqued.core_api import MetriqueHdlr

from metriqued.utils import make_update_spec
from metriqued.utils import insert_bulk
from metriqued.utils import exec_update_role
from metriqued.utils import ifind
from metriqued.utils import BASE_INDEX, make_index_spec
from metriqued.config import DEFAULT_CUBE_QUOTA

from metriqueu.utils import dt2ts, utcnow, jsonhash


class DropHdlr(MetriqueHdlr):
    ''' RequestsHandler for droping given cube from timeline '''
    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        result = self.drop_cube(owner=owner, cube=cube)
        self.write(result)

    def drop_cube(self, owner, cube):
        '''
        :param str cube: target cube (collection) to save objects to

        Wraps pymongo's drop() for the given cube (collection)
        '''
        if not self.cube_exists(owner, cube):
            raise ValueError("cube does not exist")
        self.timeline(owner, cube, admin=True).drop()
        spec = {'_id': self.cjoin(owner, cube)}
        self.cube_profile(admin=True).remove(spec)

        # FIXME: make the block below + the same code in
        # RegisterHdlr ($putToSet rather than $pull though)
        # a generic function which accepts pull/pushtoset
        # check for overlap with the other pull/pushproperty
        # setter func; maybe merge and have a cube arg
        # so we can run all cases with through a single
        # core func

        # pop out the cube name from the owners profile
        user_profile_spec = {'_id': owner}
        update = {'$pull': {'own': self.cjoin(owner, cube)}}
        self.user_profile(admin=True).update(user_profile_spec,
                                             update, safe=True)
        return True


class IndexHdlr(MetriqueHdlr):
    '''
    RequestHandler for ensuring mongodb indexes
    in timeline collection for a given cube
    '''
    @authenticated
    def get(self, owner, cube):
        result = self.index(owner=owner, cube=cube)
        self.write(result)

    @authenticated
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        ensure = self.get_argument('ensure')
        result = self.index(owner=owner, cube=cube, ensure=ensure)
        self.write(result)

    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        drop = self.get_argument('drop')
        result = self.index(owner=owner, cube=cube, drop=drop)
        self.write(result)

    def index(self, owner, cube, ensure=None, drop=None):
        '''
        :param str cube:
            name of cube (collection) to index
        :param string/list ensure:
            Either a single key or a list of (key, direction) pairs (lists)
            to ensure index on.
        :param string/list drop:
            index (or name of index) to drop
        '''
        _cube = self.timeline(owner, cube, admin=True)
        if drop is not None:
            # when drop is a list of tuples, the json
            # serialization->deserialization process leaves us with a list of
            # lists, so we need to convert it back to a list of tuples.
            drop = map(tuple, drop) if isinstance(drop, list) else drop
            if drop in [BASE_INDEX]:
                raise ValueError("can't drop system indexes")
            _cube.drop_index(drop)

        if ensure is not None:
            # same as for drop:
            ensure = map(tuple, ensure) if isinstance(ensure,
                                                      list) else ensure
            _cube.ensure_index(ensure)
        return _cube.index_information()


class ListHdlr(MetriqueHdlr):
    '''
    RequestHandler for querying about available cubes and cube.fields
    '''
    def current_user_acl(self, roles):
        roles = self.valid_role(roles)
        if not isinstance(roles, list):
            raise TypeError(
                "expected roles to be list; got %s" % type(roles))
        roles = self.get_user_profile(self.current_user, keys=roles)
        return roles if roles else []

    @authenticated
    def get(self, owner=None, cube=None):
        if (owner and cube):
            # return a 'best effort' of fields
            # in the case that there are homogenous docs,
            # 1 doc is enough; but if you have a high
            # variety of doc fields... the sample
            # size needs to be high (maxed out?)
            # to ensure accuracy
            sample_size = self.get_argument('sample_size')
            query = self.get_argument('query')
            self._requires_owner_read(owner, cube)
            names = self.sample_fields(owner, cube, sample_size,
                                       query=query)
        elif self._is_admin(owner, cube):
            names = [c for c in self._timeline_data.collection_names()
                     if not c.startswith('system')]
        else:
            # return back a full list of collections
            # available, filtered for READ acl for current user
            roles = ['read', 'own']
            read, own = self.current_user_acl(roles)
            read = [] if not read else read
            own = [] if not own else own
            names = read + own
        if owner:
            # filter out by startswith prefix string
            names = [n for n in names if n and n.startswith(owner)]
        names = filter(None, names)
        self.write(names)

    def sample_fields(self, owner, cube, sample_size=None, query=None):
        docs = self.sample_timeline(owner, cube, sample_size, query)
        cube_fields = list(set([k for d in docs for k in d.keys()]))
        return cube_fields


class RegisterHdlr(MetriqueHdlr):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        result = self.register(owner=owner, cube=cube)
        self.write(result)

    def register(self, owner, cube):
        '''
        Client registration method

        Update the user__cube __meta__ doc with defaults

        Bump the user's total cube count, by 1
        '''
        # FIXME: take out a lock; to avoid situation
        # where client tries to create multiple cubes
        # simultaneously and we hit race condition
        # where user creates more cubes than has quota
        if self.cube_exists(owner, cube):
            raise ValueError("cube already exists")

        q, o = self.get_user_profile(owner, keys=['cube_quota',
                                                  'own'])
        c = len(o) if o else 0
        q = q if q else 0
        remaining = q - c
        infinite = q <= -1
        if not (infinite or remaining > 0):
            raise RuntimeError(
                "quota_depleted (%s of %s)" % (q, c))

        now_utc = utcnow()
        spec = {'_id': self.cjoin(owner, cube)}
        update = {'$set': {'owner': owner,
                           'created': now_utc,
                           'mtime': now_utc,
                           'cube_quota': DEFAULT_CUBE_QUOTA,
                           'read': [],
                           'write': [],
                           'own': [],
                           'admin': []}}
        self.cube_profile(admin=True).update(spec, update,
                                             upsert=True, mutli=False)

        user_profile_spec = {'_id': owner}
        update = {'$addToSet': {'own': self.cjoin(owner, cube)}}
        self.user_profile(admin=True).update(user_profile_spec,
                                             update, safe=True)
        # run core index
        _cube = self.timeline(owner, cube, admin=True)
        _cube.ensure_index(BASE_INDEX)
        # return back how much remaining in quota
        # do something like ... '$count: {'own': true}' instead?
        own, cube_quota = self.get_user_profile(owner,
                                                keys=['own',
                                                      'cube_quota'])
        return remaining


class RemoveObjectsHdlr(MetriqueHdlr):
    '''
    RequestHandler for saving a given object to a
    metrique server cube
    '''
    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        ids = self.get_argument('ids')
        backup = self.get_argument('backup')
        result = self.remove_objects(owner=owner, cube=cube,
                                     ids=ids, backup=backup)
        self.write(result)

    def remove_objects(self, owner, cube, ids, backup=False):
        '''
        Remove all the objects (docs) from the given
        cube (mongodb collection)

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
        else:
            _oid_spec = {'$in': ids}
            if backup:
                docs = ifind(_oid=_oid_spec)
                if docs:
                    docs = tuple(docs)
            else:
                docs = []

            _cube = self.timeline(owner, cube, admin=True)
            full_spec = {'_oid': {'$in': ids}}
            try:
                _cube.remove(full_spec, safe=True)
            except Exception as e:
                raise RuntimeError("Failed to remove docs: %s" % e)
            else:
                return docs


class SaveObjectsHdlr(MetriqueHdlr):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def post(self, owner, cube):
        self._requires_owner_write(owner, cube)
        objects = self.get_argument('objects')
        mtime = self.get_argument('mtime')
        result = self.save_objects(owner=owner, cube=cube,
                                   objects=objects, mtime=mtime)
        self.write(result)

    @staticmethod
    def _prepare_key(obj, key):
        if key in obj:
            if not isinstance(obj[key], (int, float)):
                raise TypeError(
                    'Expected int/float type, got: %s' % type(obj[key]))
            _key = obj[key]
            _with_key = True
            del obj[key]
        else:
            _key = None
            _with_key = False
        return obj, _key, _with_key

    def prepare_objects(self, _cube, objects, mtime):
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

            obj, _start, _with_start = self._prepare_key(obj, '_start')
            obj, _end, _with_end = self._prepare_key(obj, '_end')

            if _with_end and not _with_start:
                    raise ValueError("objects with _end must have _start")
            if not _start:
                _start = mtime

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

            # we want to avoid serializing in and out later
            obj['_id'] = str(ObjectId())

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

        no_snap = [obj for obj in objects
                   if not obj.get('_oid') in _known_oids]
        to_snap = [obj for obj in objects
                   if obj.get('_oid') in _known_oids]
        return no_snap, to_snap, _oids

    @staticmethod
    def _save_and_snapshot(_cube, objects):
        '''
        Each object in objects must have '_oid' and '_start' fields
        specified and it can *not* have fields '_end' and '_id'
        specified.
        In timeline(TL), the most recent version of an object has
        _end == None.
        For each object this method tries to find the most recent
        version of it
        in TL. If there is one, if the field-values specified in the
        new object are different than those in th object from TL, it
        will end the old object and insert the new one (fields that
        are not specified in the new object are copied from the old one).
        If there is not a version of the object in TL, it will just
        insert it.

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
        current_docs = ifind(_cube=_cube, _oid=_oid_spec,
                             _end=_end_spec, fields=fields)
        current_ids = dict(
            [(doc['_id'], doc['_oid']) for doc in current_docs])

        for _id, _oid in current_ids.items():
            update = make_update_spec(_starts[_oid])
            _cube.update({'_id': _id}, update, multi=False)
        logger.debug('... "snapshot" saving %s objects.' % len(objects))
        insert_bulk(_cube, objects)

    @staticmethod
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

    def _save_objects(self, _cube, no_snap, to_snap, mtime):
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
        self._save_no_snapshot(_cube, no_snap) if len(no_snap) > 0 else []
        self._save_and_snapshot(_cube, to_snap) if len(to_snap) > 0 else []
        # update cube's mtime doc
        _cube.save({'_id': '__mtime__', 'value': mtime})
        # return object ids saved
        return [o['_oid'] for o in chain(no_snap, to_snap)]

    def save_objects(self, owner, cube, objects, mtime=None):
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
        self._validate_owner_cube_objects(owner, cube, objects)
        self.cube_exists(owner, cube, raise_on_null=True)
        mtime = dt2ts(mtime) if mtime else utcnow()
        current_mtime = self.get_mtime(owner, cube)
        if current_mtime > mtime:
            raise ValueError(
                "invalid mtime (%s); "
                "must be > current mtime (%s)" % (mtime, current_mtime))

        _cube = self.timeline(owner, cube, admin=True)
        no_snap, to_snap, _oids = self.prepare_objects(_cube, objects, mtime)

        objects = chain(no_snap, to_snap)
        olen = len(tuple(objects))

        if not olen:
            logger.debug('[%s.%s] No NEW objects to save' % (owner, cube))
            return []
        else:
            logger.debug('[%s.%s] Saved %s objects' % (owner, cube, olen))
            result = self._save_objects(_cube, no_snap, to_snap, mtime)
            return result

    @staticmethod
    def _validate_owner_cube_objects(owner, cube, objects):
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


class StatsHdlr(MetriqueHdlr):
    '''
    RequestHandler for managing cube role properties

    action can be push, pop
    role can be read, write, admin
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        result = self.stats(owner=owner, cube=cube)
        self.write(result)

    def stats(self, owner, cube):
        _cube = self.timeline(owner, cube)
        mtime = self.get_mtime(owner, cube)
        size = _cube.count()
        stats = dict(cube=cube, mtime=mtime, size=size)
        return stats


class UpdateRoleHdlr(MetriqueHdlr):
    '''
    RequestHandler for managing cube role properties

    action can be push, pop
    role can be read, write, admin
    '''
    @authenticated
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        username = self.get_argument('username')
        action = self.get_argument('action', 'push')
        role = self.get_argument('role', 'read')
        result = self.update_role(owner=owner, cube=cube,
                                  username=username,
                                  action=action, role=role)
        self.write(result)

    def update_role(self, owner, cube, username, action, role):
        if not self.user_exists(username, check_only=True):
            raise ValueError("user does not exist")
        self.valid_action(action)
        self.valid_role(role)
        _cube = self.timeline(owner, cube)
        result = exec_update_role(_cube, username, role, action)
        if result:
            return True
        else:
            return False
