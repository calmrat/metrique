#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.mongodb_api
~~~~~~~~~~~~~~~~~~~~

MongoDB client API for persisting and querying of
data cubes backed by MongoDB.

'''
from __future__ import unicode_literals

import logging
logger = logging.getLogger(__name__)

from collections import defaultdict
from getpass import getuser
from operator import itemgetter
import os
import random
import re

try:
    from pymongo import MongoClient, MongoReplicaSetClient
    from pymongo.read_preferences import ReadPreference
    from pymongo.errors import OperationFailure
except ImportError:
    raise ImportError("Pymongo 2.6+ required!")

from metrique.core_api import BaseClient, MetriqueObject
from metrique.utils import parse_pql_query, batch_gen
from metrique.result import Result

ETC_DIR = os.environ.get('METRIQUE_ETC')
SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

READ_PREFERENCE = {
    'PRIMARY_PREFERRED': ReadPreference.PRIMARY,
    'PRIMARY': ReadPreference.PRIMARY,
    'SECONDARY': ReadPreference.SECONDARY,
    'SECONDARY_PREFERRED': ReadPreference.SECONDARY_PREFERRED,
    'NEAREST': ReadPreference.NEAREST,
}

INDEX_ENSURE_SECS = 60 * 60
VALID_CUBE_SHARE_ROLES = ['read', 'readWrite', 'dbAdmin', 'userAdmin']
CUBE_OWNER_ROLES = ['readWrite', 'dbAdmin', 'userAdmin']
RESTRICTED_COLLECTION_NAMES = ['admin', 'local', 'system']
INVALID_USERNAME_RE = re.compile('[^a-zA-Z_]')


class MongoDBClient(BaseClient):
    '''
    This is the main client bindings for metrique http
    rest api.

    The is a base class that clients are expected to
    subclass to build metrique cubes which are designed
    to interact with remote metriqued hosts.

    Currently, the following API methods are exported:

    **User**
        + login: main interface for authenticating against metriqued
        + logout: log out of an existing mongodb connection
        + register: register a new user account

    **Cube**
        + list_all: list all remote cubes current user has read access to
        + sample_fields: sample remote cube object fields names
        + drop: drop (delete) a remote cube
        + register: register a new remote cube
        + save: save/persist objects to the remote cube (expects list of dicts)
        + rename: rename a remote cube
        + remove: remove (delete) objects from the remote cube
        + index_list: list all indexes currently available for a remote cube
        + index: create a new index for a remote cube
        + index_drop: remove (delete) an index from a remote cube

    **Query**
        + find: run pql (mongodb) query remotely
        + history: aggregate historical counts for objects matching a query
        + deptree: find all child ids for a given parent id
        + count: count the number of results matching a query
        + distinct: get a list of unique object property values
        + sample: query for a psuedo-random set of objects
        + aggregate: run pql (mongodb) aggregate query remotely
    '''
    default_fields = '~'
    default_sort = [('_start', -1)]

    def __init__(self, mongodb_host=None, mongodb_port=None,
                 mongodb_username=None, mongodb_password=None,
                 mongodb_auth=None, mongodb_ssl=None,
                 mongodb_ssl_certificate=None, mongodb_index_ensure_secs=None,
                 mongodb_read_preference=None, mongodb_replica_set=None,
                 mongodb_tz_aware=None, mongodb_write_concern=None,
                 mongodb_batch_size=None,
                 mongodb_owner=None,
                 *args, **kwargs):
        '''
        :param mongodb_auth: Enable authentication
        :param mongodb_batch_size: The number of objs save at a time
        :param mongodb_password: mongodb password
        :param mongodb_username: mongodb username
        :param mongodb_host: mongodb host(s) to connect to
        :param mongodb_owner: mongodb owner user database cube is in
        :param mongodb_port: mongodb port to connect to
        :param mongodb_read_preference: default - NEAREST
        :param mongodb_replica_set: name of replica set, if any
        :param mongodb_ssl: enable ssl
        :param mongodb_ssl_certificate: path to ssl combined .pem
        :param mongodb_tz_aware: return back tz_aware dates?
        :param mongodb_write_concern: # of inst's to write to before finish
        '''
        super(MongoDBClient, self).__init__(*args, **kwargs)
        options = dict(host=mongodb_host,
                       port=mongodb_port,
                       auth=mongodb_auth,
                       username=mongodb_username,
                       password=mongodb_password,
                       owner=mongodb_owner,
                       ssl=mongodb_ssl,
                       ssl_certificate=mongodb_ssl_certificate,
                       index_ensure_secs=mongodb_index_ensure_secs,
                       read_preference=mongodb_read_preference,
                       replica_set=mongodb_replica_set,
                       tz_aware=mongodb_tz_aware,
                       write_concern=mongodb_write_concern,
                       batch_size=mongodb_batch_size)
        defaults = dict(host='127.0.0.1',
                        port=27017,
                        auth=False,
                        username=getuser(),
                        password='',
                        owner=None,
                        ssl=False,
                        ssl_certificate=SSL_PEM,
                        index_ensure_secs=INDEX_ENSURE_SECS,
                        read_preference='NEAREST',
                        replica_set=None,
                        tz_aware=True,
                        write_concern=0,
                        batch_size=10000)
        self.configure('mongodb', options, defaults)

    def __getitem__(self, query):
        return self.find(query=query, fields=self.default_fields,
                         date='~', merge_versions=False,
                         sort=self.default_sort)

    def keys(self, sample_size=1):
        return self.sample_fields(sample_size=sample_size)

    def values(self, sample_size=1):
        return self.sample_docs(sample_size=sample_size)

######################### DB API ##################################
    def _ensure_base_indexes(self, _cube):
        s = self.config['mongodb'].get('index_ensure_secs')
        _cube.ensure_index('_oid', background=False, cache_for=s)
        _cube.ensure_index('_hash', background=False, cache_for=s)
        _cube.ensure_index([('_start', -1), ('_end', -1)],
                           background=False, cache_for=s)
        _cube.ensure_index([('_end', -1)],
                           background=False, cache_for=s)

    def get_db(self, owner=None):
        owner = owner or self.config['mongodb'].get('owner')
        # assume owner is config'd username if not set
        owner = owner or self.config['mongodb'].get('username')
        if not owner:
            raise RuntimeError("[%s] Invalid db!" % owner)
        try:
            return self.proxy[owner]
        except OperationFailure as e:
            raise RuntimeError("unable to get db! (%s)" % e)

    def get_collection(self, owner=None, cube=None):
        cube = cube or self.name
        if not cube:
            raise RuntimeError("[%s] Invalid cube!" % cube)
        _cube = self.get_db(owner)[cube]
        try:
            self._ensure_base_indexes(_cube)
        except OperationFailure as e:
            logger.debug(e)
        return _cube

    @property
    def db(self):
        return self.proxy[self.config['mongodb'].get('username')]

    @property
    def proxy(self):
        _proxy = getattr(self, '_proxy', None)
        if not _proxy:
            kwargs = {}
            ssl = self.config['mongodb'].get('ssl')
            if ssl:
                cert = self.config['mongodb'].get('ssl_certificate')
                # include ssl options only if it's enabled
                # certfile is a combined key+cert
                kwargs.update(dict(ssl=ssl, ssl_certfile=cert))
            if self.config['mongodb'].get('replica_set'):
                _proxy = self._load_mongo_replica_client(**kwargs)
            else:
                _proxy = self._load_mongo_client(**kwargs)
            auth = self.config['mongodb'].get('auth')
            username = self.config['mongodb'].get('username')
            password = self.config['mongodb'].get('password')
            if auth:
                _proxy = self._authenticate(_proxy, username, password)
            self._proxy = _proxy
        return _proxy

    def _authenticate(self, proxy, username, password):
        if not (username and password):
            raise ValueError(
                "username:%s, password:%s required" % (username, password))
        ok = proxy[username].authenticate(username, password)
        if ok:
            logger.debug('Authenticated as %s' % username)
            return proxy
        raise RuntimeError(
            "MongoDB failed to authenticate user (%s)" % username)

    def _load_mongo_client(self, **kwargs):
        logger.debug('Loading new MongoClient connection')
        host = self.config['mongodb'].get('host')
        port = self.config['mongodb'].get('port')
        tz_aware = self.config['mongodb'].get('tz_aware')
        w = self.config['mongodb'].get('write_concern')
        _proxy = MongoClient(host, port, tz_aware=tz_aware,
                             w=w, **kwargs)
        return _proxy

    def _load_mongo_replica_client(self, **kwargs):
        host = self.config['mongodb'].get('host')
        port = self.config['mongodb'].get('port')
        tz_aware = self.config['mongodb'].get('tz_aware')
        w = self.config['mongodb'].get('write_concern')
        replica_set = self.config['mongodb'].get('replica_set')
        pref = self.config['mongodb'].get('read_preference')
        read_preference = READ_PREFERENCE[pref]

        logger.debug('Loading new MongoReplicaSetClient connection')
        _proxy = MongoReplicaSetClient(host, port, tz_aware=tz_aware,
                                       w=w, replicaSet=replica_set,
                                       read_preference=read_preference,
                                       **kwargs)
        return _proxy

######################### User API ################################
    def whoami(self, auth=False):
        '''Local api call to check the username of running user'''
        return self.config['mongodb'].get('username')

    def _validate_password(self, password):
        is_str = isinstance(password, basestring)
        char_8_plus = len(password) >= 8
        ok = all((is_str, char_8_plus))
        if not ok:
            raise ValueError("Invalid password; must be len(string) >= 8")
        return password

    def _validate_username(self, username):
        if not isinstance(username, basestring):
            raise TypeError("username must be a string")
        elif INVALID_USERNAME_RE.search(username):
            raise ValueError(
                "Invalid username '%s'; "
                "lowercase, ascii alpha [a-z_] characters only!" % username)
        elif username in RESTRICTED_COLLECTION_NAMES:
            raise ValueError(
                "username '%s' is not permitted" % username)
        else:
            return username.lower()

    def _validate_cube_roles(self, roles):
        if isinstance(roles, basestring):
            roles = [roles]
        if not isinstance(roles, (list, tuple)):
            raise TypeError("roles must be single string or list")
        roles = set(map(str, roles))
        if not roles <= set(VALID_CUBE_SHARE_ROLES):
            raise ValueError("invalid roles %s, try: %s" % (
                roles, VALID_CUBE_SHARE_ROLES))
        return sorted(roles)

    # FIXME: add 'user_update_roles()...
    def user_register(self, username=None, password=None):
        '''
        Register new user.

        :param user: Name of the user you're managing
        :param password: Password (plain text), if any of user
        '''
        if username and not password:
            raise RuntimeError('must specify password!')
        password = password or self.config['mongodb'].get('password')
        username = username or self.config['mongodb'].get('username')
        username = self._validate_username(username)
        password = self._validate_password(password)
        logger.info('Registering new user %s' % username)
        self.proxy[username].add_user(username, password,
                                      roles=CUBE_OWNER_ROLES)
        spec = parse_pql_query('user == "%s"' % username)
        result = self.proxy[username].system.users.find(spec).count()
        return bool(result)

    def user_remove(self, username, clear_db=False):
        username = username or self.config['mongodb'].get('username')
        username = self._validate_username(username)
        logger.info('Removing user %s' % username)
        self.proxy[username].remove_user(username)
        if clear_db:
            self.proxy.drop_database(username)
        spec = parse_pql_query('user == "%s"' % username)
        result = not bool(self.proxy[username].system.users.find(spec).count())
        return result

######################### Cube API ################################
    def ls(self, startswith=None, owner=None):
        '''
        List all cubes available to the calling client.

        :param startswith: string to use in a simple "startswith" query filter
        :returns list: sorted list of cube names
        '''
        db = self.get_db(owner)
        cubes = db.collection_names(include_system_collections=False)
        startswith = unicode(startswith or '')
        cubes = [name for name in cubes if name.startswith(startswith)]
        RCN = RESTRICTED_COLLECTION_NAMES
        not_restricted = lambda c: all(not c.startswith(name) for name in RCN)
        cubes = filter(not_restricted, cubes)
        logger.info('[%s] Listing available cubes starting with "%s")' % (
            owner, startswith))
        return sorted(cubes)

    def share(self, with_user, roles=None, cube=None, owner=None):
        '''
        Give cube access rights to another user
        '''
        with_user = self._validate_username(with_user)
        roles = self._validate_cube_roles(roles or ['read'])
        _cube = self.get_db(owner)
        logger.info(
            '[%s] Sharing cube with %s (%s)' % (_cube, with_user, roles))
        result = _cube.add_user(name=with_user, roles=roles,
                                userSource=with_user)
        return result

    def drop(self, cube=None, owner=None):
        '''
        Drop (delete) cube.

        :param quiet: ignore exceptions
        :param owner: username of cube owner
        :param cube: cube name
        '''
        _cube = self.get_collection(owner, cube)
        logger.info('[%s] Dropping cube' % _cube)
        _cube.drop()
        result = not bool(self.name in self.db.collection_names())
        return result

    def index_list(self, cube=None, owner=None):
        '''
        List all cube indexes

        :param cube: cube name
        :param owner: username of cube owner
        '''
        _cube = self.get_collection(owner, cube)
        logger.info('[%s] Listing indexes' % _cube)
        result = _cube.index_information()
        return result

    def index(self, key_or_list, cube=None, owner=None, **kwargs):
        '''
        Build a new index on a cube.

        Examples:
            + ensure_index('field_name')
            + ensure_index([('field_name', 1), ('other_field_name', -1)])

        :param key_or_list: A single field or a list of (key, direction) pairs
        :param name: (optional) Custom name to use for this index
        :param background: MongoDB should create in the background
        :param cube: cube name
        :param owner: username of cube owner
        '''
        _cube = self.get_collection(owner, cube)
        logger.info('[%s] Writing new index %s' % (_cube, key_or_list))
        s = self.config['mongodb'].get('index_ensure_secs')
        kwargs['cache_for'] = kwargs.get('cache_for', s)
        result = _cube.ensure_index(key_or_list, **kwargs)
        return result

    def index_drop(self, index_or_name, cube=None, owner=None):
        '''
        Drops the specified index on this cube.

        :param index_or_name: index (or name of index) to drop
        :param cube: cube name
        :param owner: username of cube owner
        '''
        _cube = self.get_collection(owner, cube)
        logger.info('[%s] Droping index %s' % (_cube, index_or_name))
        result = _cube.drop_index(index_or_name)
        return result

    ######## SAVE/REMOVE ########

    def rename(self, new_name=None, new_owner=None, drop_target=False,
               cube=None, owner=None):
        '''
        Rename a cube.

        :param new_name: new cube name
        :param new_owner: new cube owner (admin privleges required!)
        :param cube: cube name
        :param owner: username of cube owner
        '''
        if not (new_name or new_owner):
            raise ValueError("must set either/or new_name or new_owner")
        new_name = new_name or cube or self.name
        if not new_name:
            raise ValueError("new_name is not set!")
        _cube = self.get_collection(owner, cube)
        if new_owner:
            _from = _cube.full_name
            _to = '%s.%s' % (new_owner, new_name)
            self.proxy['admin'].command(
                'renameCollection', _from, to=_to, dropTarget=drop_target)
            # don't touch the new collection until after attempting
            # the rename; collection would otherwise be created
            # empty automatically then the rename fails because
            # target already exists.
            _new_db = self.get_db(new_owner)
            result = bool(new_name in _new_db.collection_names())
        else:
            logger.info('[%s] Renaming cube -> %s' % (_cube, new_name))
            _cube.rename(new_name, dropTarget=drop_target)
            result = bool(new_name in self.db.collection_names())
        if cube is None and result:
            self.name = new_name
        return result

######################## ETL API ##################################
    def get_objects(self, flush=False, autosnap=True):
        '''Main API method for sub-classed cubes to override for the
        generation of the objects which are to (potentially) be added
        to the cube (assuming no duplicates)
        '''
        if flush:
            return self.flush(autosnap=True)
        return self

    def get_last_field(self, field):
        '''Shortcut for querying to get the last field value for
        a given owner, cube.

        :param field: field name to query
        '''
        last = self.find(query=None, fields=[field],
                         sort=[(field, -1)], one=True, raw=True)
        if last:
            last = last.get(field)
        logger.debug("last %s.%s: %s" % (self.name, field, last))
        return last

    def remove(self, query, date=None, cube=None, owner=None):
        '''
        Remove objects from a cube.

        :param query: `pql` query to filter sample query with
        :param cube: cube name
        :param owner: username of cube owner
        '''
        spec = parse_pql_query(query, date)
        _cube = self.get_collection(owner, cube)
        logger.info("[%s] Removing objects (%s): %s" % (_cube, date, query))
        result = _cube.remove(spec)
        return result

    def flush(self, autosnap=True, batch_size=None, fast=True,
              cube=None, owner=None):
        '''
        Persist a list of objects to MongoDB.

        Returns back a list of object ids saved.

        :param objects: list of dictionary-like objects to be stored
        :param cube: cube name
        :param owner: username of cube owner
        :param start: ISO format datetime to apply as _start
                      per object, serverside
        :param autosnap: rotate _end:None's before saving new objects
        :returns result: _ids saved
        '''
        batch_size = batch_size or self.config['mongodb'].get('batch_size')
        _cube = self.get_collection(owner, cube)
        _ids = []
        for batch in batch_gen(self.objects.values(), batch_size):
            _ = self._flush(_cube=_cube, objects=batch, autosnap=autosnap,
                            fast=fast, cube=cube, owner=owner)
            _ids.extend(_)
        return sorted(_ids)

    def _flush_save(self, _cube, objects, fast=True):
        objects = [dict(o) for o in objects if o['_end'] is None]
        if not objects:
            _ids = []
        elif fast:
            _ids = [o['_id'] for o in objects]
            [_cube.save(o, manipulate=False) for o in objects]
        else:
            _ids = [_cube.save(dict(o), manipulate=True) for o in objects]
        return _ids

    def _flush_insert(self, _cube, objects, fast=True):
        objects = [dict(o) for o in objects if o['_end'] is not None]
        if not objects:
            _ids = []
        elif fast:
            _ids = [o['_id'] for o in objects]
            _cube.insert(objects, manipulate=False)
        else:
            _ids = _cube.insert(objects, manipulate=True)
        return _ids

    def _flush(self, _cube, objects, autosnap=True, fast=True,
               cube=None, owner=None):
        olen = len(objects)
        if olen == 0:
            logger.debug("No objects to flush!")
            return []
        logger.info("[%s] Flushing %s objects" % (_cube, olen))

        objects, dup_ids = self._filter_dups(_cube, objects)
        # remove dups from instance object container
        [self.objects.pop(_id) for _id in set(dup_ids)]

        if objects and autosnap:
            # append rotated versions to save over previous _end:None docs
            objects = self._add_snap_objects(_cube, objects)

        olen = len(objects)
        _ids = []
        if objects:
            # save each object; overwrite existing
            # (same _oid + _start or _oid if _end = None) or upsert
            logger.debug('[%s] Saving %s versions' % (_cube, len(objects)))
            _saved = self._flush_save(_cube, objects, fast)
            _inserted = self._flush_insert(_cube, objects, fast)
            _ids = set(_saved) | set(_inserted)
            # pop those we're already flushed out of the instance container
            failed = olen - len(_ids)
            if failed > 0:
                logger.warn("%s objects failed to flush!" % failed)
            # new 'snapshoted' objects are included in _ids, but they
            # aren't in self.objects, so ignore them
        [self.objects.pop(_id) for _id in _ids if _id in self.objects]
        logger.debug(
            "[%s] %s objects remaining" % (_cube, len(self.objects)))
        return _ids

    def _filter_end_null_dups(self, _cube, objects):
        # filter out dups which have null _end value
        _hashes = [o['_hash'] for o in objects if o['_end'] is None]
        if _hashes:
            spec = parse_pql_query('_hash in %s' % _hashes, date=None)
            return set(_cube.find(spec).distinct('_id'))
        else:
            return set()

    def _filter_end_not_null_dups(self, _cube, objects):
        # filter out dups which have non-null _end value
        tups = [(o['_id'], o['_hash']) for o in objects
                if o['_end'] is not None]
        _ids, _hashes = zip(*tups) if tups else [], []
        if _ids:
            spec = parse_pql_query(
                '_id in %s and _hash in %s' % (_ids, _hashes), date='~')
            return set(_cube.find(spec).distinct('_id'))
        else:
            return set()

    def _filter_dups(self, _cube, objects):
        logger.debug('Filtering duplicate objects...')
        olen = len(objects)

        non_null_ids = self._filter_end_not_null_dups(_cube, objects)
        null_ids = self._filter_end_null_dups(_cube, objects)
        dup_ids = non_null_ids | null_ids

        if dup_ids:
            # update self.object container to contain only non-dups
            objects = [o for o in objects if o['_id'] not in dup_ids]

        _olen = len(objects)
        diff = olen - _olen
        logger.info(' ... %s objects filtered; %s remain' % (diff, _olen))
        return objects, dup_ids

    def _add_snap_objects(self, _cube, objects):
        olen = len(objects)
        _ids = [o['_id'] for o in objects if o['_end'] is None]

        if not _ids:
            logger.debug(
                '[%s] 0 of %s objects need to be rotated' % (_cube, olen))
            return objects

        spec = parse_pql_query('_id in %s' % _ids, date=None)
        _objs = _cube.find(spec, {'_hash': 0})
        k = _objs.count()
        logger.debug(
            '[%s] %s of %s objects need to be rotated' % (_cube, k, olen))
        if k == 0:  # nothing to rotate...
            return objects
        for o in _objs:
            # _end of existing obj where _end:None should get new's _start
            # look this up in the instance objects mapping
            _start = self.objects[o['_id']]['_start']
            o['_end'] = _start
            del o['_id']
            objects.append(MetriqueObject(**o))
        return objects

######################## Query API ################################
    def aggregate(self, pipeline, cube=None, owner=None):
        '''
        Run a pql mongodb aggregate pipeline on remote cube

        :param pipeline: The aggregation pipeline. $match, $project, etc.
        :param cube: cube name
        :param owner: username of cube owner
        '''
        _cube = self.get_collection(owner, cube)
        result = _cube.aggregate(pipeline)
        return result

    def count(self, query=None, date=None, cube=None, owner=None):
        '''
        Run a pql mongodb based query on the given cube and return only
        the count of resulting matches.

        :param query: The query in pql
        :param date: date (metrique date range) that should be queried
                    If date==None then the most recent versions of the
                    objects will be queried.
        :param cube: cube name
        :param owner: username of cube owner
        '''
        _cube = self.get_collection(owner, cube)
        spec = parse_pql_query(query, date)
        result = _cube.find(spec).count()
        return result

    def _parse_fields(self, fields):
        _fields = {'_id': 0, '_start': 1, '_end': 1, '_oid': 1}
        if fields in [None, False]:
            _fields = None
        elif fields in ['~', True]:
            _fields = None
        elif isinstance(fields, dict):
            _fields.update(fields)
        elif isinstance(fields, basestring):
            _fields.update({s.strip(): 1 for s in fields.split(',')})
        elif isinstance(fields, (list, tuple)):
            _fields.update({s.strip(): 1 for s in fields})
        else:
            raise ValueError("invalid fields value")
        return _fields

    def find(self, query=None, fields=None, date=None, sort=None, one=False,
             raw=False, explain=False, merge_versions=False, skip=0,
             limit=0, as_cursor=False, cube=None, owner=None):
        '''
        Run a pql mongodb based query on the given cube.

        :param query: The query in pql
        :param fields: Fields that should be returned (comma-separated)
        :param date: date (metrique date range) that should be queried.
                    If date==None then the most recent versions of the
                    objects will be queried.
        :param explain: return execution plan instead of results
        :param merge_versions: merge versions where fields values equal
        :param one: return back only first matching object
        :param sort: return back results sorted
        :param raw: return back raw JSON results rather than pandas dataframe
        :param skip: number of results matched to skip and not return
        :param limit: number of results matched to return of total found
        :param cube: cube name
        :param owner: username of cube owner
        '''
        _cube = self.get_collection(owner, cube)
        spec = parse_pql_query(query, date)
        fields = self._parse_fields(fields)

        merge_versions = False if fields is None or one else merge_versions
        if merge_versions:
            fields = fields or {}
            fields.update({'_start': 1, '_end': 1, '_oid': 1})

        find = _cube.find_one if one else _cube.find
        result = find(spec, fields=fields, sort=sort, explain=explain,
                      skip=skip, limit=limit)

        if one or explain or as_cursor:
            return result
        result = list(result)
        if merge_versions:
            result = self._merge_versions(result)
        if raw:
            return result
        else:
            return Result(result, date)

    def _merge_versions(self, objects):
        # contains a dummy document to avoid some condition
        # checks in merge_doc
        ret = [{'_oid': None}]
        no_check = set(['_start', '_end'])

        def merge_doc(doc):
            '''
            merges doc with the last document in ret if possible
            '''
            last = ret[-1]
            ret.append(doc)
            if doc['_oid'] == last['_oid'] and doc['_start'] == last['_end']:
                if all(item in last.items() or item[0] in no_check
                       for item in doc.iteritems()):
                    # the fields of interest did not change, merge docs:
                    last['_end'] = doc['_end']
                    ret.pop()

        objects = sorted(objects,
                         key=itemgetter('_oid', '_start', '_end'))
        logger.debug("merging doc versions...")
        [merge_doc(obj) for obj in objects]
        logger.debug('... done')
        return ret[1:]

    def history(self, query, by_field=None, date_list=None, cube=None,
                owner=None):
        '''
        Run a pql mongodb based query on the given cube and return back the
        aggregate historical counts of matching results.

        :param query: The query in pql
        :param by_field: Which field to slice/dice and aggregate from
        :param date: list of dates that should be used to bin the results
        :param cube: cube name
        :param owner: username of cube owner
        '''
        query = '%s and _start < %s and (_end >= %s or _end == None)' % (
                query, max(date_list), min(date_list))
        spec = parse_pql_query(query)

        pipeline = [
            {'$match': spec},
            {'$group':
             {'_id': '$%s' % by_field if by_field else '_id',
              'starts': {'$push': '$_start'},
              'ends': {'$push': '$_end'}}
             }]
        data = self.aggregate(pipeline)['result']
        data = self._history_accumulate(data, date_list)
        data = self._history_convert(data, by_field)
        return data

    def _history_accumulate(self, data, date_list):
        date_list = sorted(date_list)
        # accumulate the counts
        res = defaultdict(lambda: defaultdict(int))
        for group in data:
            starts = sorted(group['starts'])
            ends = sorted([x for x in group['ends'] if x is not None])
            _id = group['_id']
            ind = 0
            # assuming date_list is sorted
            for date in date_list:
                while ind < len(starts) and starts[ind] < date:
                    ind += 1
                res[date][_id] = ind
            ind = 0
            for date in date_list:
                while ind < len(ends) and ends[ind] < date:
                    ind += 1
                res[date][_id] -= ind
        return res

    def _history_convert(self, data, by_field):
        # convert to the return form
        ret = []
        for date, value in data.items():
            if by_field:
                vals = []
                for field_val, count in value.items():
                    vals.append({by_field: field_val,
                                 "count": count})
                ret.append({"date": date,
                            "values": vals})
            else:
                ret.append({"date": date,
                            "count": value['id']})
        return ret

    def deptree(self, field, oids, date=None, level=None, cube=None,
                owner=None):
        '''
        Dependency tree builder. Recursively fetchs objects that
        are children of the initial set of parent object ids provided.

        :param field: Field that contains the 'parent of' data
        :param oids: Object oids to build depedency tree for
        :param date: date (metrique date range) that should be queried.
                    If date==None then the most recent versions of the
                    objects will be queried.
        :param level: limit depth of recursion
        :param cube: cube name
        :param owner: username of cube owner
        '''
        if not level or level < 1:
            level = 1
        if isinstance(oids, basestring):
            oids = [s.strip() for s in oids.split(',')]
        checked = set(oids)
        fringe = oids
        loop_k = 0
        _cube = self.get_collection(owner, cube)
        while len(fringe) > 0:
            if level and loop_k == abs(level):
                break
            query = '_oid in %s and %s != None' % (fringe, field)
            spec = parse_pql_query(query, date)
            fields = {'_id': -1, '_oid': 1, field: 1}
            docs = _cube.find(spec, fields=fields)
            fringe = set([oid for doc in docs for oid in doc[field]])
            fringe = filter(lambda oid: oid not in checked, fringe)
            checked |= set(fringe)
            loop_k += 1
        return sorted(checked)

    def distinct(self, field, query=None, date=None, cube=None, owner=None):
        '''
        Return back a distinct (unique) list of field values
        across the entire cube dataset

        :param field: field to get distinct token values from
        :param cube: cube name
        :param owner: username of cube owner
        '''
        _cube = self.get_collection(owner, cube)
        if query:
            spec = parse_pql_query(query, date)
            result = _cube.find(spec).distinct(field)
        else:
            result = _cube.distinct(field)
        return result

    def sample_fields(self, sample_size=None, query=None, date=None,
                      cube=None, owner=None):
        '''
        List a sample of all valid fields for a given cube.

        Assuming all cube objects have the same exact fields, sampling
        fields should result in a complete list of object fields.

        However, if cube objects have different fields, sampling fields
        might not result in a complete list of object fields, since
        some object variants might not be included in the sample queried.

        :param sample_size: number of random documents to query
        :param query: `pql` query to filter sample query with
        :param cube: cube name
        :param owner: username of cube owner
        :returns list: sorted list of fields
        '''
        docs = self.sample_docs(sample_size=sample_size, query=query,
                                date=date, cube=cube, owner=owner)
        result = sorted({k for d in docs for k in d.iterkeys()})
        return result

    def sample_docs(self, sample_size=None, query=None, date=None,
                    cube=None, owner=None):
        '''
        Take a randomized sample of documents from a cube.

        :param sample_size: number of random documents to query
        :param query: `pql` query to filter sample query with
        :param cube: cube name
        :param owner: username of cube owner
        :returns list: sorted list of fields
        '''
        sample_size = sample_size or 1
        spec = parse_pql_query(query, date)
        _cube = self.get_collection(owner)
        docs = _cube.find(spec)
        n = docs.count()
        if n <= sample_size:
            docs = list(docs)
        else:
            to_sample = sorted(set(random.sample(xrange(n), sample_size)))
            docs = [docs[i] for i in to_sample]
        return docs
