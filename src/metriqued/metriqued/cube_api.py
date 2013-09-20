#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from bson import ObjectId
import logging
logger = logging.getLogger(__name__)
from tornado.web import authenticated

from metriqued.core_api import MetriqueHdlr

from metriqued.utils import insert_bulk
from metriqued.utils import ifind
from metriqued.utils import BASE_INDEX, SYSTEM_INDEXES

from metriqueu.utils import dt2ts, utcnow, jsonhash

# FIXME: change '__' between cube/owner to '.' and make dots
# in cube names illegal


# FIXME: add ability to backup before dropping
class DropHdlr(MetriqueHdlr):
    ''' RequestsHandler for droping given cube from timeline '''
    @authenticated
    def delete(self, owner, cube):
        result = self.drop_cube(owner=owner, cube=cube)
        self.write(result)

    def drop_cube(self, owner, cube):
        '''
        :param str cube: target cube (collection) to save objects to

        Wraps pymongo's drop() for the given cube (collection)
        '''
        self.requires_owner_admin(owner, cube)
        if not self.cube_exists(owner, cube):
            self._raise(404, '%s.%s does not exist' % (owner, cube))
        spec = {'_id': self.cjoin(owner, cube)}
        # drop the cube
        self.timeline(owner, cube, admin=True).drop()
        spec = {'_id': self.cjoin(owner, cube)}
        # drop the entire cube profile
        self.cube_profile(admin=True).remove(spec)
        # pull the cube from the owner's profile
        collection = self.cjoin(owner, cube)
        self.update_user_profile(owner, 'pull', '_own', collection)
        return True


class IndexHdlr(MetriqueHdlr):
    '''
    RequestHandler for ensuring mongodb indexes
    in timeline collection for a given cube
    '''
    @authenticated
    def delete(self, owner, cube):
        drop = self.get_argument('drop')
        result = self.index(owner=owner, cube=cube, drop=drop)
        self.write(result)

    @authenticated
    def get(self, owner, cube):
        result = self.index(owner=owner, cube=cube)
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
        self.cube_exists(owner, cube)
        _cube = self.timeline(owner, cube, admin=True)
        if drop is not None:
            self.requires_owner_admin(owner, cube)
            # when drop is a list of tuples, the json
            # serialization->deserialization process leaves us
            # with a list of lists which pymongo rejects; convert
            # to ordered dict instead
            if drop in SYSTEM_INDEXES:
                raise ValueError("can't drop system indexes")
            _cube.drop_index(drop)
        elif ensure is not None:
            # same as for drop  ^^^ see comments above
            self.requires_owner_admin(owner, cube)
            _cube.ensure_index(ensure)
        else:
            self.requires_owner_read(owner, cube)
        return _cube.index_information()

    @authenticated
    def post(self, owner, cube):
        ensure = self.get_argument('ensure')
        result = self.index(owner=owner, cube=cube, ensure=ensure)
        self.write(result)


class ListHdlr(MetriqueHdlr):
    '''
    RequestHandler for querying about available cubes and cube.fields
    '''
    def current_user_acl(self, roles):
        self.valid_cube_role(roles)
        roles = self.get_user_profile(self.current_user, keys=roles,
                                      null_value=[])
        return roles if roles else []

    @authenticated
    def get(self, owner=None, cube=None):
        if (owner and cube):
            # return a 'best effort' of fields in the case that there are
            # homogenous docs, 1 doc is enough; but if you have a high
            # variety of doc fields... the sample size needs to be high
            # (maxed out?) to ensure accuracy
            sample_size = self.get_argument('sample_size')
            query = self.get_argument('query')
            names = self.sample_fields(owner, cube, sample_size, query=query)
        elif self.requires_owner_admin(owner, cube, raise_if_not=False):
            names = self.get_non_system_collections(owner, cube)
        else:
            names = self.get_readable_collections()
        if owner and not cube:
            # filter out by startswith prefix string
            names = [n for n in names if n and n.startswith(owner)]
        names = filter(None, names)
        self.write(names)

    def get_non_system_collections(self, owner, cube):
        self.requires_owner_admin(owner, cube)
        return [c for c in self._timeline_data.collection_names()
                if not c.startswith('system')]

    def get_readable_collections(self):
        # return back a collections if user is owner or can read them
        roles = ['read', 'own']
        read, own = self.current_user_acl(roles)
        # FIXME: return back owner/cube separated into a dict?
        return read + own

    def sample_fields(self, owner, cube, sample_size=None, query=None):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        docs = self.sample_timeline(owner, cube, sample_size, query)
        cube_fields = list(set([k for d in docs for k in d.keys()]))
        return cube_fields


class RegisterHdlr(MetriqueHdlr):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self, owner, cube):
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
        self.requires_owner_admin(owner)
        if self.cube_exists(owner, cube, raise_if_not=False):
            self._raise(409, "cube already exists")

        # FIXME: move to remaining =  self.check_user_cube_quota(...)
        quota, own = self.get_user_profile(owner, keys=['_cube_quota',
                                                        '_own'])
        if quota is None:
            remaining = True
        else:
            own = len(own) if own else 0
            quota = quota or 0
            remaining = quota - own

        if not remaining or remaining <= 0:
            self._raise(409, "quota depleted (%s of %s)" % (quota, own))

        now_utc = utcnow()
        collection = self.cjoin(owner, cube)

        doc = {'_id': collection,
               'owner': owner,
               'created': now_utc,
               'read': [],
               'write': [],
               'admin': []}
        self.cube_profile(admin=True).insert(doc)

        # push the collection into the list of ones user owns
        self.update_user_profile(owner, 'addToSet', 'own', collection)

        # run core index
        _cube = self.timeline(owner, cube, admin=True)
        _cube.ensure_index(BASE_INDEX)
        return remaining


class RemoveObjectsHdlr(MetriqueHdlr):
    '''
    RequestHandler for saving a given object to a
    metrique server cube
    '''
    @authenticated
    def delete(self, owner, cube):
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
        self.cube_exists(owner, cube)
        self.requires_owner_admin(owner, cube)
        if not ids:
            logger.debug('REMOVE: no ids provided')
            return []
        elif not isinstance(ids, list):
            self._raise(400, "Expected list, got %s: %s" %
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
            full_spec = {'_oid': _oid_spec}
            _cube.remove(full_spec, safe=True)
            return docs


class SaveObjectsHdlr(MetriqueHdlr):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def post(self, owner, cube):
        objects = self.get_argument('objects')
        mtime = self.get_argument('mtime')
        result = self.save_objects(owner=owner, cube=cube,
                                   objects=objects, mtime=mtime)
        self.write(result)

    def prepare_objects(self, _cube, objects, mtime):
        '''
        :param dict obj: dictionary that will be converted to mongodb doc
        :param int mtime: timestamp to apply as _start for objects

        Do some basic object validatation and add an _start timestamp value
        '''
        new_obj_hashes = []
        for obj in objects:
            _start = obj.pop('_start') if '_start' in obj else None
            _end = obj.pop('_end') if '_end' in obj else None

            if _end is not None and _start is None:
                self._raise(400, "objects with _end must have _start")
            if not _start:
                _start = mtime
            if not isinstance(_start, (int, float)):
                self._raise(400, "_start must be float/int")
            if not isinstance(_end, (int, float)) and _end is not None:
                self._raise(400, "_end must be float/int/None")

            if '_id' in obj:
                self._raise(400, "_id field CAN NOT be defined: %s" % obj)
            if '_hash' in obj:
                self._raise(400, "_hash field CAN NOT be defined: %s" % obj)
            if '_oid' not in obj:
                self._raise(400, "_oid field MUST be defined: %s" % obj)

            # hash the object (minus _start/_end)
            _hash = jsonhash(obj)
            obj['_hash'] = _hash
            if _end is None:
                new_obj_hashes.append(_hash)

            # add back _start and _end properties
            obj['_start'] = _start
            obj['_end'] = _end

            # we want to avoid serializing in and out later
            obj['_id'] = str(ObjectId())

        # FIXME: refactor this so we split the _hashes
        # mongodb lookups iterate across 16M max
        # spec docs...
        # get the estimate size, as follows
        #est_size_hashes = estimate_obj_size(_hashes)

        # Filter out objects whose most recent version did not change
        docs = ifind(_cube=_cube, _hash={'$in': new_obj_hashes},
                     _end=None, fields={'_hash': 1, '_id': -1})
        _dup_hashes = set([doc['_hash'] for doc in docs])
        objects = [obj for obj in objects if obj['_hash'] not in _dup_hashes]
        objects = filter(None, objects)
        return objects

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
        self.cube_exists(owner, cube)
        self.requires_owner_write(owner, cube)
        mtime = dt2ts(mtime) if mtime else utcnow()
        current_mtime = self.get_cube_last_start(owner, cube)
        if current_mtime and mtime and current_mtime > mtime:
            raise ValueError(
                "invalid mtime (%s); "
                "must be > current mtime (%s)" % (mtime, current_mtime))
        _cube = self.timeline(owner, cube, admin=True)

        olen_r = len(objects)
        logger.debug('[%s.%s] Recieved %s objects' % (owner, cube, olen_r))

        objects = self.prepare_objects(_cube, objects, mtime)

        logger.debug('[%s.%s] %s objects match their current version in db' % (
            owner, cube, olen_r - len(objects)))

        if not objects:
            logger.debug('[%s.%s] No NEW objects to save' % (owner, cube))
            return []
        else:
            logger.debug('[%s.%s] Saving %s objects' % (owner, cube,
                                                        len(objects)))
            # End the most recent versions in the db of those objects that
            # have newer versionsi (newest version must have _end == None,
            # activity import saves objects for which this might not be true):
            to_snap = dict([(o['_oid'], o['_start']) for o in objects
                            if o['_end'] is None])
            if to_snap:
                db_versions = ifind(_cube=_cube, _oid={'$in': to_snap.keys()},
                                    _end=None, fields={'_id': 1, '_oid': 1})
                snapped = 0
                for doc in db_versions:
                    _cube.update({'_id': doc['_id']},
                                 {'$set': {'_end': to_snap[doc['_oid']]}},
                                 multi=False)
                    snapped += 1
                logger.debug('[%s.%s] Updated %s OLD versions' %
                             (owner, cube, snapped))
            # Insert all new versions:
            insert_bulk(_cube, objects)
            logger.debug('[%s.%s] Saved %s NEW versions' % (owner, cube,
                                                            len(objects)))
            # return object ids saved
            return [o['_oid'] for o in objects]


class StatsHdlr(MetriqueHdlr):
    '''
    RequestHandler for managing cube role properties

    action can be addToSet, pull
    role can be read, write, admin
    '''
    @authenticated
    def get(self, owner, cube):
        result = self.stats(owner=owner, cube=cube)
        self.write(result)

    def stats(self, owner, cube):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        _cube = self.timeline(owner, cube)
        size = _cube.count()
        stats = dict(cube=cube, size=size)
        return stats


class UpdateRoleHdlr(MetriqueHdlr):
    '''
    RequestHandler for managing cube role properties

    action can be addToSet, pull
    role can be read, write, admin
    '''
    @authenticated
    def post(self, owner, cube):
        username = self.get_argument('username')
        action = self.get_argument('action')
        role = self.get_argument('role')
        result = self.update_role(owner=owner, cube=cube,
                                  username=username,
                                  action=action, role=role)
        self.write(result)

    def update_role(self, owner, cube, username, action='addToSet',
                    role='read'):
        self.cube_exists(owner, cube)
        self.requires_owner_admin(owner, cube)
        self.valid_action(action)
        self.valid_cube_role(role)
        return self.update_cube_profile(owner, cube, action, role, username)
