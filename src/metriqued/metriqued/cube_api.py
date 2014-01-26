#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

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
        # drop the cube
        _cube = self.cjoin(owner, cube)
        self.mongodb_config.db_timeline_admin[_cube].drop()
        # drop the entire cube profile
        spec = {'_id': _cube}
        self.cube_profile(admin=True).remove(spec)
        # pull the cube from the owner's profile
        self.update_user_profile(owner, 'pull', 'own', _cube)
        return True


class ExportHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for exporting a collection (cube) to gzipped json
    '''
    @authenticated
    def get(self, owner, cube):
        self.cube_exists(owner, cube)
        self.requires_owner_admin(owner, cube)
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
    RequestHandler for ensuring mongodb indexes
    in timeline collection for a given cube
    '''
    @authenticated
    def delete(self, owner, cube):
        self.cube_exists(owner, cube)
        self.requires_owner_admin(owner, cube)
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
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        _cube = self.timeline(owner, cube, admin=True)
        self.write(_cube.index_information())

    @authenticated
    def post(self, owner, cube):
        self.cube_exists(owner, cube)
        self.requires_owner_admin(owner, cube)
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
        read = self.current_user_acl(['read'])
        own = self.current_user_acl(['own'])
        return read + own

    def sample_fields(self, owner, cube, sample_size=None, query=None):
        self.cube_exists(owner, cube)
        self.requires_owner_read(owner, cube)
        docs = self.sample_timeline(owner, cube, sample_size, query)
        cube_fields = list(set([k for d in docs for k in d.keys()]))
        return cube_fields


class RenameHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self, owner, cube):
        new_name = self.get_argument('new_name')
        result = self.rename(owner=owner, cube=cube, new_name=new_name)
        self.write(result)

    def rename(self, owner, cube, new_name):
        '''
        Client registration method

        Default behaivor (not currently overridable) is to permit
        cube registrations by all registered users.

        Update the user__cube __meta__ doc with defaults

        Bump the user's total cube count, by 1
        '''
        self.logger.debug("Renaming [%s] %s -> %s" % (owner, cube, new_name))
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
    RequestHandler for registering new users to metrique
    '''
    def post(self, owner, cube):
        result = self.register(owner=owner, cube=cube)
        self.write(result)

    def register(self, owner, cube):
        '''
        Client registration method

        Default behaivor (not currently overridable) is to permit
        cube registrations by all registered users.

        Update the user__cube __meta__ doc with defaults

        Bump the user's total cube count, by 1
        '''
        # FIXME: take out a lock; to avoid situation
        # where client tries to create multiple cubes
        # simultaneously and we hit race condition
        # where user creates more cubes than has quota
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
        # ensure basic indices:
        _cube.ensure_index('_hash')
        _cube.ensure_index('_oid')
        _cube.ensure_index('_start')  # used in core_api.get_cube_last_start
        _cube.ensure_index('_end')  # default for non-historical queries
        return remaining


class RemoveObjectsHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for saving a given object to a
    metrique server cube
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
        Remove all the objects (docs) from the given
        cube (mongodb collection)

        :param pymongo.collection cube:
            cube object (pymongo collection connection)
        :param string query:
            pql query string
        :param string date:
            metrique date(range)
        '''
        self.cube_exists(owner, cube)
        self.requires_owner_admin(owner, cube)
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
    RequestHandler for saving a given object to a metrique server cube
    '''
    def insert_bulk(self, _cube, docs, size=-1):
        # little reason to batch insert...
        # http://stackoverflow.com/questions/16753366
        # and after testing, it seems splitting things
        # up more slows things down.
        if size <= 0:
            _cube.insert(docs, manipulate=False)
        else:
            for batch in batch_gen(docs, size):
                _cube.insert(batch, manipulate=False)

    @authenticated
    def post(self, owner, cube):
        objects = self.get_argument('objects')
        result = self.save_objects(owner=owner, cube=cube,
                                   objects=objects)
        self.write(result)

    def prepare_objects(self, _cube, objects):
        '''
        :param dict obj: dictionary that will be converted to mongodb doc

        Do some basic object validatation and add an _start timestamp value
        '''
        hashes = []
        ids = []
        start = utcnow()
        exclude = ['_hash', '_id', '_start', '_end']
        for o in objects:
            keys = set(o.keys())
            o = o if '_start' in keys else self._obj_start(o, start)
            o = o if '_end' in keys else self._obj_end(o)
            o = o if '_hash' in keys else self._obj_hash(o, key='_hash',
                                                         exclude=exclude)
            # give it a unique _id based on all contents
            o = o if '_id' in keys else self._obj_hash(o, key='_id')

            if not isinstance(o['_start'], (float, int)):
                self._raise(400, "_start must be float/int")
            if not isinstance(o['_end'], (NoneType, float, int)):
                self._raise(400, "_end must be float/int/None")

            if o['_end'] is not None:
                ids.append(o['_id'])
            hashes.append(o['_hash'])

        # Filter out object 'current' versions already set
        docs = _cube.find({'_hash': {'$in': hashes}, '_end': None},
                          fields={'_hash': 1, '_id': -1})
        dups = set([doc['_hash'] for doc in docs])
        objects = [o for o in objects if o['_hash'] not in dups]

        if ids:
            # objects with _end are considered 'complete' and should remain
            # constant, forever and are expected to always map to the same _id
            # check for any which already exist and don't save them again
            docs = _cube.find({'_id': {'$in': ids}})
            dups = set([doc['_id'] for doc in docs])
            objects = [o for o in objects if o['_id'] not in dups]
        return objects

    def save_objects(self, owner, cube, objects):
        '''
        :param str owner: target owner's cube
        :param str cube: target cube (collection) to save objects to
        :param list objects: list of dictionary-like objects to be stored
        :rtype: list - list of object ids saved

        Get a list of dictionary objects from client and insert
        or save them to the timeline.
        '''
        self.cube_exists(owner, cube)
        self.requires_owner_write(owner, cube)
        _cube = self.timeline(owner, cube, admin=True)

        olen_r = len(objects)
        self.logger.debug(
            '[%s.%s] Recieved %s objects' % (owner, cube, olen_r))

        objects = self.prepare_objects(_cube, objects)

        self.logger.debug(
            '[%s.%s] SKIPPED %s objs matching their current version in db' % (
            owner, cube, olen_r - len(objects)))

        if not objects:
            self.logger.debug('[%s.%s] No NEW objects to save' % (owner, cube))
            return []
        else:
            self.logger.debug('[%s.%s] Saving %s objects' % (owner, cube,
                                                             len(objects)))
            # End the most recent versions in the db of those objects that
            # have newer versionsi (newest version must have _end == None,
            # activity import saves objects for which this might not be true):
            to_snap_start = dict([(o['_oid'], o['_start']) for o in objects
                                 if o['_end'] is None])
            if to_snap_start:
                # update all the current versions such that the _end becomes
                # the new versions _start
                # then insert all the new objects as-are
                db_versions = _cube.find(
                    {'_oid': {'$in': to_snap_start.keys()}, '_end': None},
                    fields={'_id': 1, '_oid': 1})
                snapped = 0
                for doc in db_versions:
                    _cube.update(
                        {'_id': doc['_id']},
                        {'$set': {'_end': to_snap_start[doc['_oid']]}},
                        multi=False)
                    snapped += 1
                self.logger.debug('[%s.%s] Updated %s OLD versions' %
                                  (owner, cube, snapped))
            # Insert all new versions:
            self.insert_bulk(_cube, objects)
            self.logger.debug('[%s.%s] Saved %s NEW versions' % (owner, cube,
                                                                 len(objects)))
            # return object ids saved
            return [o['_oid'] for o in objects]

    def _obj_end(self, obj, default=None):
        obj['_end'] = obj.get('_end', default)
        return obj

    def _obj_hash(self, obj, key, exclude=None):
        o = copy(obj)
        if exclude:
            [o.pop(k) for k in exclude if k in obj]
        obj[key] = jsonhash(o)
        return obj

    def _obj_start(self, obj, default=None):
        _start = obj.get('_start', default)
        obj['_start'] = _start or utcnow()
        return obj


class StatsHdlr(MongoDBBackendHdlr):
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
        self.cube_exists(owner, cube)
        self.requires_owner_admin(owner, cube)
        self.valid_cube_role(role)
        result = self.update_cube_profile(owner, cube, action, role, username)
        # push the collection into the list of ones user owns
        collection = self.cjoin(owner, cube)
        self.update_user_profile(username, 'addToSet', role, collection)
        return result
