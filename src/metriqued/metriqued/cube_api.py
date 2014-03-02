#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriqued.cube_api
~~~~~~~~~~~~~~~~~~

This module contains all Cube related api functionality.
'''

from copy import copy
from datetime import datetime
import gzip
import os
import re
import shlex
import subprocess
import tempfile
from types import NoneType
from tornado.web import authenticated
# FIXME: gen.coroutine async decorator for find, index, export, saveobjects...

from metriqued.core_api import MongoDBBackendHdlr
from metriqued.utils import query_add_date, parse_pql_query
from metriqueu.utils import utcnow, batch_gen, jsonhash

OBJ_KEYS = set(['_id', '_hash', '_oid', '_start', '_end'])


class DropHdlr(MongoDBBackendHdlr):
    ''' RequestsHandler for dropping given cube from timeline '''
    @authenticated
    def delete(self, owner, cube):
        result = self.drop_cube(owner=owner, cube=cube)
        self.write(result)

    def drop_cube(self, owner, cube):
        '''
        Wraps pymongo's drop() for the given cube (collection)

        :param owner: username of cube owner
        :param cube: cube name
        '''
        self.requires_admin(owner, cube)
        if not self.cube_exists(owner, cube):
            self._raise(404, '%s.%s does not exist' % (owner, cube))
        # drop the cube
        _cube = self.cjoin(owner, cube)
        self.mongodb_config.db_timeline_admin[_cube].drop()
        # drop the entire cube profile
        spec = {'_id': _cube}
        self.cube_profile(admin=True).remove(spec)
        # pull the cube from the owner's profile
        self.update_user_profile(owner, 'pull', 'own', _cube)
        return True


# FIXME: add 'gzip' to find() and drop this!
class ExportHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for exporting a collection (cube) to gzipped json
    '''
    @authenticated
    def get(self, owner, cube):
        self.requires_admin(owner, cube)
        path = ''
        try:
            path = self.mongoexport(owner, cube)
            with open(path, 'rb') as f:
                while 1:
                    data = f.read(16384)
                    if not data:
                        break
                    self.write(data, binary=True)
        finally:
            if os.path.exists(path):
                os.remove(path)

    # FIXME: UNICODE IS NOT PROPERLY ENCODED!
    def mongoexport(self, owner, cube):
        '''
        Calls mongoexport command line application and returns
        the results as compressed gzip file.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        conf = self.mongodb_config
        _cube = '__'.join((owner, cube))
        now = datetime.now().isoformat()

        fd, path = tempfile.mkstemp(prefix=_cube + '-',
                                    suffix='-%s.json' % now)
        path_gz = path + '.gz'

        x = conf['mongoexport']
        db = '--db timeline'
        collection = '--collection %s' % _cube
        out = '--out %s' % path
        ssl = '--ssl' if conf['ssl'] else ''
        auth = conf['auth']
        authdb = '--authenticationDatabase admin' if auth else ''
        user = '--username admin' if auth else ''
        _pass = '--password %s' % conf['admin_password'] if auth else ''
        cmd = ' '.join([x, db, collection, out, ssl, authdb, user, _pass])
        _cmd = re.sub('password.*$', 'password *****', cmd)
        self.logger.debug('Running: %s' % _cmd)
        try:
            subprocess.check_call(shlex.split(cmd.encode('ascii')),
                                  stdout=open(os.devnull, 'wb'),
                                  stderr=open(os.devnull, 'wb'))
            f_in = open(path, 'rb')
            f_out = gzip.open(path_gz, 'wb')
            f_out.writelines(f_in)
            f_in.close()
            f_out.close()
        finally:
            if os.path.exists(path):
                os.remove(path)
        return path_gz


class IndexHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for creating indexes for a given cube
    '''
    @authenticated
    def delete(self, owner, cube):
        '''
        Delete an existing cube index.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        self.requires_admin(owner, cube)
        drop = self.get_argument('drop')
        _cube = self.timeline(owner, cube, admin=True)
        if drop:
            # json serialization->deserialization process leaves
            # us with a list of lists which pymongo rejects
            drop = map(tuple, drop) if isinstance(drop, list) else drop
            _cube.drop_index(drop)
        self.write(_cube.index_information())

    @authenticated
    def get(self, owner, cube):
        '''
        Return a list of the existing cube indexes.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        self.requires_read(owner, cube)
        _cube = self.timeline(owner, cube, admin=True)
        self.write(_cube.index_information())

    @authenticated
    def post(self, owner, cube):
        '''
        Create a new index for a cube.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        self.requires_admin(owner, cube)
        ensure = self.get_argument('ensure')
        background = self.get_argument('background', True)
        name = self.get_argument('name', None)
        kwargs = dict()
        if name:
            kwargs['name'] = name
        if background:
            kwargs['background'] = background
        _cube = self.timeline(owner, cube, admin=True)
        if ensure:
            # json serialization->deserialization process leaves us
            # with a list of lists which pymongo rejects; convert
            # to ordered dict instead
            ensure = map(tuple, ensure) if isinstance(ensure, list) else ensure
            _cube.ensure_index(ensure, **kwargs)
        self.write(_cube.index_information())


class ListHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for querying about available cubes and cube.fields
    '''
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
        else:
            names = self.get_readable_collections()
        if owner and not cube:
            # filter out by startswith prefix string
            names = [n for n in names if n and n.startswith(owner)]
        names = filter(None, names)
        self.write(names)

    def sample_fields(self, owner, cube, sample_size=None, query=None):
        '''
        Sample object fields to get back a list of known field names.

        Since cube object contents can vary widely, in theory, it's
        possible a "sample" of less than "all" might result in an
        incomplete list of field values.

        To ensure a complete list offield values then, one must have
        a sample size equal to the number of objects in the cube.

        If all objects are known to be uniform, a sample size of 1
        is sufficient.

        :param owner: username of cube owner
        :param cube: cube name
        :param sample_size: number of objects to sample
        :param query: high-level query used to create population to sample
        '''
        self.requires_read(owner, cube)
        docs = self.sample_cube(owner, cube, sample_size, query)
        cube_fields = list(set([k for d in docs for k in d.keys()]))
        return cube_fields


class RenameHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for renaming cubes.
    '''
    def post(self, owner, cube):
        new_name = self.get_argument('new_name')
        result = self.rename(owner=owner, cube=cube, new_name=new_name)
        self.write(result)

    def rename(self, owner, cube, new_name):
        '''
        Rename a cube.

        :param owner: username of cube owner
        :param cube: cube name
        :param new_new: the new name of the cube
        '''
        self.logger.debug("Renaming [%s] %s -> %s" % (owner, cube, new_name))
        self.requires_admin(owner, cube)
        if cube == new_name:
            self._raise(409, "cube is already named %s" % new_name)
        self.cube_exists(owner, cube)
        if self.cube_exists(owner, new_name, raise_if_not=False):
            self._raise(409, "cube already exists (%s)" % new_name)

        _cube_profile = self.cube_profile(admin=True)

        old = self.cjoin(owner, cube)
        new = self.cjoin(owner, new_name)

        # get the cube_profile doc
        spec = {'_id': old}
        doc = _cube_profile.find_one(spec)
        # save the doc with new _id
        doc.update({'_id': new})
        _cube_profile.insert(doc)
        # rename the collection

        self.mongodb_config.db_timeline_admin[old].rename(new)
        # push the collection into the list of ones user owns
        self.update_user_profile(owner, 'addToSet', 'own', new)
        # pull the old cube from user profile's 'own'
        self.update_user_profile(owner, 'pull', 'own', old)
        # remove the old doc
        _cube_profile.remove(spec)
        return True


class RegisterHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for registering new cubes.
    '''
    def post(self, owner, cube):
        result = self.register(owner=owner, cube=cube)
        self.write(result)

    def register(self, owner, cube):
        '''
        Client registration method

        Cube registrations is open access. All registered
        users can create cubes, assuming their quota has
        not been filled already.

        Update the cube_profile with new cube defaults values.

        Bump the user's total cube count, by 1

        Create default cubes indexes.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        # FIXME: take out a lock; to avoid situation
        # where client tries to create multiple cubes
        # simultaneously and we hit race condition
        # where user creates more cubes than has quota
        # ie, cube create lock...
        if self.cube_exists(owner, cube, raise_if_not=False):
            self._raise(409, "cube already exists")

        # FIXME: move to remaining = self.check_user_cube_quota(...)
        quota, own = self.get_user_profile(owner, keys=['cube_quota',
                                                        'own'])
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
               'creater': owner,
               'created': now_utc,
               'read': [],
               'write': [],
               'admin': [owner]}
        self.cube_profile(admin=True).insert(doc)

        # push the collection into the list of ones user owns
        self.update_user_profile(owner, 'addToSet', 'own', collection)

        # run core index
        _cube = self.timeline(owner, cube, admin=True)
        # ensure basic indices:
        _cube.ensure_index('_hash')
        _cube.ensure_index('_oid')
        _cube.ensure_index('_start')  # used in core_api.get_cube_last_start
        _cube.ensure_index('_end')  # default for non-historical queries
        return remaining


class RemoveObjectsHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for removing objects from a cube.
    '''
    @authenticated
    def delete(self, owner, cube):
        query = self.get_argument('query')
        date = self.get_argument('date')
        result = self.remove_objects(owner=owner, cube=cube,
                                     query=query, date=date)
        self.write(result)

    def remove_objects(self, owner, cube, query, date=None):
        '''
        Remove all the objects from the given cube.

        :param owner: username of cube owner
        :param cube: cube name
        :param string query: pql query string
        :param string date: metrique date(range)
        '''
        self.requires_admin(owner, cube)
        if not query:
            return []

        if isinstance(query, basestring):
            query = query_add_date(query, date)
            spec = parse_pql_query(query)
        elif isinstance(query, (list, tuple)):
            spec = {'_id': {'$in': query}}
        else:
            raise ValueError(
                'Expected query string or list of ids, got: %s' % type(query))

        _cube = self.timeline(owner, cube, admin=True)
        return _cube.remove(spec)


class SaveObjectsHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for saving/persisting objects to a cube
    '''
    _exclude_hash = ['_hash', '_id', '_start', '_end']
    _include_static = ['_start', '_end', '_oid']
    _include_ongoing = ['_oid']

    @authenticated
    def post(self, owner, cube):
        objects = self.get_argument('objects')
        result = self.save_objects(owner=owner, cube=cube,
                                   objects=objects)
        self.write(result)

    def insert_bulk(self, _cube, docs, size=10000):
        '''
        Insert a list of objects into a give cube.

        :param _cube: mongodb cube collection proxy
        :param docs: list of docs to insert
        :param size: max size of insert batches
        '''
        # little reason to batch insert...
        # http://stackoverflow.com/questions/16753366
        # and after testing, it seems splitting things
        # up more slows things down.
        if size <= 0:
            _cube.insert(docs, manipulate=False)
        else:
            for batch in batch_gen(docs, size):
                _cube.insert(batch, manipulate=False)

    def prepare_objects(self, _cube, objects):
        '''
        Validate and normalize objects.

        :param _cube: mongodb cube collection proxy
        :param obejcts: list of objects to manipulate
        '''
        start = utcnow()
        for o in objects:
            # _hash is of object contents, excluding metadata
            o = self._obj_hash(o, key='_hash', exclude=self._exclude_hash)

            o = self._obj_end(o)
            _end = o.get('_end')
            if not isinstance(_end, (NoneType, float, int)):
                self._raise(400, "_end must be float/int epoch or None")

            o = self._obj_start(o, start)
            _start = o.get('_start')
            if not isinstance(_start, (float, int)):
                self._raise(400, "_start must be defined, as float/int epoch")

            _oid = o.get('_oid')
            if not isinstance(_oid, (float, int)):
                self._raise(400, "_oid must be defined, as float/int")

            # give object a unique, constant (referencable) _id
            if _end:
                # if the object at the exact start/end/oid is later
                # updated, it's possible to save(upsert)
                o = self._obj_hash(o, key='_id', include=self._include_static)
            else:
                # if the object is 'current value' without _end,
                # id is the hash of only the oid
                o = self._obj_hash(o, key='_id', include=self._include_ongoing)
        return objects

    def _snap_current(self, _cube, objects):
        # End the most recent versions in the db of those objects that
        # have newer versionsi (newest version must have _end == None,
        # activity import saves objects for which this might not be true):
        to_snap_start = dict([(o['_oid'], o['_start']) for o in objects
                              if o['_end'] is None])
        if not to_snap_start:
            return

        snap_oids = to_snap_start.keys()
        # update all the current versions such that the _end becomes
        # the new versions _start
        db_versions = _cube.find(
            {'_oid': {'$in': snap_oids}, '_end': None},
            fields={'_id': 1, '_oid': 1})
        for k, obj in enumerate(db_versions):
            _oid = obj['_oid']
            # get the current _id (gen'd against _end:None
            old_id = obj['_id']
            spec = {'_id': old_id},
            # Re-generate _id now that _end is set
            obj = self._obj_hash(obj, key='_id', include=self._include_static)
            new_id = obj['_id']
            update = {'$set': {
                '_id': new_id,
                '_end': to_snap_start[_oid]
            }}
            _cube.find_and_modify(spec, update=update)
        self.logger.debug(' ... Updated %s OLD versions' % k)
        return objects

    def save_objects(self, owner, cube, objects):
        '''
        Get a list of dictionary objects from client and insert
        or save them to the timeline.

        :param owner: username of cube owner
        :param cube: cube name
        :param obejcts: list of objects to save
        '''
        self.requires_write(owner, cube)
        _cube = self.timeline(owner, cube, admin=True)

        olen = len(objects)
        self.logger.debug(
            '[%s.%s] Recieved %s objects' % (owner, cube, olen))

        objects = self.prepare_objects(_cube, objects)
        if not objects:
            self.logger.debug('[%s.%s] No new objects to save' % (
                owner, cube))
            return []

        olen = len(objects)
        self.logger.debug('[%s.%s] Saving %s new objects' % (
            owner, cube, olen))

        self._snap_current(objects)

        # save each object; overwrite existing, only if not already saved
        q = '_id == %s and _hash == %s'
        [_cube.save(o, upsert=True) for o in objects
         if not _cube.count(q % (o['_id'], o['_hash']))]

        self.logger.debug('[%s.%s] Saved %s NEW versions' % (
            owner, cube, len(objects)))
        return

    def _obj_end(self, obj, default=None):
        obj['_end'] = obj.get('_end', default)
        return obj

    def _obj_hash(self, obj, key, exclude=None, include=None):
        o = copy(obj)
        if include:
            o = dict([(k, v) for k, v in o.item() if k in include])
        elif exclude:
            [o.pop(k) for k in exclude if k in obj]
        obj[key] = jsonhash(o)
        return obj

    def _obj_start(self, obj, default=None):
        _start = obj.get('_start', default)
        obj['_start'] = _start or utcnow()
        return obj


class StatsHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for getting basic statistics about a cube
    '''
    @authenticated
    def get(self, owner, cube):
        result = self.stats(owner=owner, cube=cube)
        self.write(result)

    def stats(self, owner, cube):
        '''
        Return basic statistics about a cube.

        Wraps mongodb's 'collstats' function.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        self.requires_read(owner, cube)
        _cube = self.mongodb_config.db_timeline_data
        stats = _cube.command("collstats", self.cjoin(owner, cube))
        return stats


class UpdateRoleHdlr(MongoDBBackendHdlr):
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
        '''
        Update user's ACL role for a given cube.

        :param owner: username of cube owner
        :param cube: cube name
        :param username: username who's ACLs will be manipulated
        :param action: update action to take

        Available actions:
            * pull - remove a value
            * addToSet - add a value
            * set - set or replace a value
        '''
        self.requires_admin(owner, cube)
        self.valid_cube_role(role)
        result = self.update_cube_profile(owner, cube, action, role, username)
        # push the collection into the list of ones user owns
        collection = self.cjoin(owner, cube)
        self.update_user_profile(username, 'addToSet', role, collection)
        return result
