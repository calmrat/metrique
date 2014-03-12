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
import itertools
import logging
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
from metriqueu.utils import utcnow, jsonhash

logger = logging.getLogger(__name__)


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
        logger.debug('Running: %s' % _cmd)
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
        logger.debug("Renaming [%s] %s -> %s" % (owner, cube, new_name))
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
        _cube.ensure_index([('_hash', 1), ('_end', 1)])
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
    @authenticated
    def post(self, owner, cube):
        objects = self.get_argument('objects')
        autosnap = self.get_argument('autosnap')
        result = self.save_objects(owner=owner, cube=cube,
                                   objects=objects, autosnap=autosnap)
        self.write(result)

    def _prepare_objects(self, objects, autosnap=True):
        '''
        Validate and normalize objects.

        :param _cube: mongodb cube collection proxy
        :param obejcts: list of objects to manipulate
        '''
        start = utcnow()
        _exclude_hash = ['_hash', '_id', '_start', '_end']
        _end_types_ok = NoneType if autosnap else (NoneType, float, int)
        for i, o in enumerate(objects):
            # apply default start and typecheck
            o = self._obj_start(o, start)
            _start = o.get('_start')
            if not isinstance(_start, (float, int)):
                self._raise(400, "_start must be float/int epoch")
            # and apply default end and typecheck
            o = self._obj_end(o)
            _end = o.get('_end')
            if not isinstance(_end, _end_types_ok):
                self._raise(400, "_end must be float/int epoch or None")

            # give object a unique, constant (referencable) _id
            o = self._obj_id(o)
            # _hash is of object contents, excluding _metadata
            o = self._obj_hash(o, key='_hash', exclude=_exclude_hash)
            objects[i] = o

        return objects

    @staticmethod
    def _get_start(_id, objs):
        for o in objs:
            if o['_oid'] == _id:
                return o['_start']

    def _prep_snap_objects(self, _cube, objects):
        # get current obj where _end:None and the _hash isn't the
        # same (ie, objects isn't a duplicate)
        hash_map = dict([(o['_oid'], o['_hash']) for o in objects])
        _objs = _cube.find({'_end': None,
                            '_oid': {'$in': hash_map.keys()},
                            '_hash': {'$nin': hash_map.values()}})
        k = _objs.count()
        logger.debug('%s objects need to be rotated' % k)
        if k > 0:  # nothing to rotate...
            for o in _objs:
                # _end of existing obj where _end:None should get new's _start
                o['_end'] = self._get_start(o['_oid'], objects)
                o = self._obj_id(o)
                objects.append(o)
        return objects

    def save_objects(self, owner, cube, objects, autosnap=True):
        '''
        Get a list of dictionary objects from client and insert
        or save them to the timeline.

        :param owner: username of cube owner
        :param cube: cube name
        :param obejcts: list of objects to save
        :param autosnap: flag indicating non-dup _end:None objects
                         should rotate previous _end:None value before
                         saving

        Incoming objects must all have _oid and _start defined.
        Optionally, objects can have _end defined.

        '''
        self.requires_write(owner, cube)

        logger.debug(
            '[%s.%s] Recieved %s objects' % (owner, cube, len(objects)))

        if not objects:
            return []

        _cube = self.timeline(owner, cube, admin=True)

        objects = self._prepare_objects(objects, autosnap)

        snap_objects, save_objects = [], []
        for o in objects:
            if o['_end'] is None:
                snap_objects.append(o)
            else:
                save_objects.append(o)
        del objects

        if autosnap:
            # append rotated versions to save over previous _end:None docs
            snap_objects = self._prep_snap_objects(_cube, snap_objects)

        # save each object; overwrite existing (same _oid + _start or _oid if
        # _end = None) or upsert
        objects = itertools.chain(save_objects, snap_objects)
        _ids = [_cube.save(o, manipulate=True) for o in objects]
        logger.debug('[%s.%s] %s versions saved' % (owner, cube, len(_ids)))
        return _ids

    def _obj_id(self, obj):
        if obj['_end']:
            # if the object at the exact start/oid is later
            # updated, it's possible to just save(upsert=True)
            _id = ':'.join(map(str, (obj['_oid'], obj['_start'])))
        else:
            # if the object is 'current value' without _end,
            # use only _oid, as _start could change
            _id = str(obj['_oid'])
        obj['_id'] = _id
        return obj

    def _obj_end(self, obj, default=None):
        obj['_end'] = obj.get('_end', default)
        return obj

    def _obj_hash(self, obj, key, exclude=None, include=None):
        o = copy(obj)
        if include:
            include = set(include)
            o = dict([(k, v) for k, v in o.items() if k in include])
        elif exclude:
            keys = set(obj.keys())
            [o.pop(k) for k in exclude if k in keys]
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
