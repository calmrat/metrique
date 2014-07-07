#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.mongodb
~~~~~~~~~~~~~~~~

This module contains MongoDB data storage proxy and
MongoDB specific MetriqueContainer API.
'''

from __future__ import unicode_literals, absolute_import

import logging
logger = logging.getLogger('metrique')

from collections import defaultdict
from copy import copy
from getpass import getuser
from operator import itemgetter
import os
import random
import re
from time import time

try:
    import pql
    HAS_PQL = True
except ImportError:
    HAS_PQL = False
    logger.warn('pql module is not installed!')

try:
    from pymongo import MongoClient, MongoReplicaSetClient
    from pymongo.collection import Collection
    from pymongo.read_preferences import ReadPreference
    from pymongo.errors import OperationFailure

    READ_PREFERENCE = {
        'PRIMARY_PREFERRED': ReadPreference.PRIMARY,
        'PRIMARY': ReadPreference.PRIMARY,
        'SECONDARY': ReadPreference.SECONDARY,
        'SECONDARY_PREFERRED': ReadPreference.SECONDARY_PREFERRED,
        'NEAREST': ReadPreference.NEAREST,
    }
    HAS_PYMONGO = True
except ImportError:
    READ_PREFERENCE = {}
    HAS_PYMONGO = False
    logger.warn('pymongo 2.6+ not installed!')

from metrique import MetriqueObject, MetriqueContainer
from metrique import parse
from metrique.result import Result
from metrique.utils import configure, is_true, is_array, is_defined
from metrique.utils import validate_username, validate_password, validate_roles

ETC_DIR = os.environ.get('METRIQUE_ETC')
CACHE_DIR = os.environ.get('METRIQUE_CACHE') or '/tmp'
DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metrique.json')


# FIXME: convert all datetimes->float using 'manipulator'
# ################################ MONGODB ###################################
class MongoDBProxy(object):
    '''
        :param auth: Enable authentication
        :param batch_size: The number of objs save at a time
        :param password: mongodb password
        :param username: mongodb username
        :param host: mongodb host(s) to connect to
        :param db: mongodb db user database cube is in
        :param port: mongodb port to connect to
        :param read_preference: default - NEAREST
        :param replica_set: name of replica set, if any
        :param ssl: enable ssl
        :param ssl_certificate: path to ssl combined .pem
        :param tz_aware: return back tz_aware dates?
        :param write_concern: # of inst's to write to before finish

    Takes kwargs, but ignores them.
    '''
    config = None
    config_key = 'mongodb'
    config_file = DEFAULT_CONFIG
    RESTRICTED_COLLECTIONS = ['admin', 'local', 'system', 'system.indexes']
    SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

    def __init__(self, db=None, table=None, host=None, port=None,
                 username=None, password=None, auth=None, ssl=None,
                 ssl_certificate=None, read_preference=None,
                 replica_set=None, tz_aware=None, write_concern=None,
                 config_file=None, config_key=None, **kwargs):
        '''
        Accept additional kwargs, but ignore them.
        '''
        is_true(HAS_PYMONGO, '`pip install pymongo` 2.6+ required')
        self.RESTRICTED_COLLECTIONS = copy(self.RESTRICTED_COLLECTIONS)
        options = dict(auth=auth,
                       host=host,
                       db=db,
                       password=password,
                       port=port,
                       read_preference=read_preference,
                       replica_set=replica_set,
                       ssl=ssl,
                       ssl_certificate=ssl_certificate,
                       table=table,
                       tz_aware=tz_aware,
                       username=username,
                       write_concern=write_concern)
        defaults = dict(auth=False,
                        host='127.0.0.1',
                        db=None,
                        password='',
                        port=27017,
                        read_preference='NEAREST',
                        replica_set=None,
                        ssl=False,
                        ssl_certificate=self.SSL_PEM,
                        table=None,
                        tz_aware=False,
                        username=getuser(),
                        write_concern=0)
        self.config = self.config or {}
        self.config_file = config_file or self.config_file
        self.config_key = config_key or MongoDBProxy.config_key
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                section_only=True,
                                update=self.config)
        # db is required; default db is db username else local username
        self.config['db'] = self.config['db'] or self.config['username']
        is_defined(self.config.get('db'), 'db can not be null')

    def __repr__(self):
        db = self.config.get('db')
        return '%s(db="%s">)' % (
            self.__class__.__name__, db)

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
        host = self.config.get('host')
        port = self.config.get('port')
        tz_aware = self.config.get('tz_aware')
        w = self.config.get('write_concern')
        _proxy = MongoClient(host, port, tz_aware=tz_aware,
                             w=w, **kwargs)
        return _proxy

    def _load_mongo_replica_client(self, **kwargs):
        host = self.config.get('host')
        port = self.config.get('port')
        tz_aware = self.config.get('tz_aware')
        w = self.config.get('write_concern')
        replica_set = self.config.get('replica_set')
        pref = self.config.get('read_preference')
        read_preference = READ_PREFERENCE[pref]

        logger.debug('Loading new MongoReplicaSetClient connection')
        _proxy = MongoReplicaSetClient(host, port, tz_aware=tz_aware,
                                       w=w, replicaSet=replica_set,
                                       read_preference=read_preference,
                                       **kwargs)
        return _proxy

    def _parse_query(self, query, date=None):
        '''
        Given a pql based query string, parse it using
        pql.SchemaFreeParser and return the resulting
        pymongo 'spec' dictionary.

        :param query: pql query
        '''
        if not HAS_PQL:
            raise RuntimeError("`pip install pql` required")
        _subpat = re.compile(' in \([^\)]+\)')
        date = parse.date_range(date, func='epoch_utc')
        if query and date:
            query = '%s and %s' % (query, date)
        elif date:
            query = date
        else:
            pass
        _q = _subpat.sub(' in (...)', query) if query else query
        logger.debug('Query: %s' % _q)
        if not query:
            return {}
        if not isinstance(query, basestring):
            raise TypeError("query expected as a string")
        pql_parser = pql.SchemaFreeParser()
        try:
            spec = pql_parser.parse(query)
        except Exception as e:
            raise SyntaxError("Invalid Query (%s)" % str(e))
        logger.debug('... spec: %s' % spec)
        return spec

    def autotable(self, name, *args, **kwargs):
        return self.ensure_collection(collection=name, *args, **kwargs)

    def columns(self, sample_size=1, collection=None):
        sample_size = sample_size or 1
        columns = self.sample_fields(collection=collection,
                                     sample_size=sample_size)
        return sorted(columns)

    def get_db(self, db=None):
        db = db or self.config.get('db')
        if not db:
            raise RuntimeError("[%s] Invalid db!" % db)
        try:
            return self.proxy[db]
        except OperationFailure as e:
            raise RuntimeError("unable to get db! (%s)" % e)

    def get_collection(self, name=None, db=None):
        if isinstance(name, Collection) and name.name == name:
            return name  # we already have the collection...
        else:
            name = name.name if isinstance(name, Collection) else name
            name = name or self.config.get('table')
            is_defined(name, "collection name can not be null!")
            db = db or self.config.get('db')
            _cube = self.get_db(db)[name]
        return _cube

    def initialize(self):
        kwargs = {}
        ssl = self.config.get('ssl')
        if ssl:
            cert = self.config.get('ssl_certificate')
            # include ssl options only if it's enabled
            # certfile is a combined key+cert
            kwargs.update(dict(ssl=ssl, ssl_certfile=cert))
        if self.config.get('replica_set'):
            _proxy = self._load_mongo_replica_client(**kwargs)
        else:
            _proxy = self._load_mongo_client(**kwargs)
        auth = self.config.get('auth')
        username = self.config.get('username')
        password = self.config.get('password')
        if auth:
            _proxy = self._authenticate(_proxy, username, password)
        self._proxy = _proxy

    @property
    def proxy(self):
        if not getattr(self, '_proxy', None):
            self.initialize()
        return self._proxy

    def set_db(self, db):
        is_defined(db, "[%s] Invalid db!" % db)
        try:
            self.proxy[db]
        except OperationFailure as e:
            raise RuntimeError("unable to get db! (%s)" % e)
        self.config['db'] = db

    def count(self, query=None, date=None, db=None, collection=None):
        '''
        Run a pql mongodb based query on the given cube and return only
        the count of resulting matches.

        :param query: The query in pql
        :param date: date (metrique date range) that should be queried
                    If date==None then the most recent versions of the
                    objects will be queried.
        :param collection: cube name
        :param db: username of cube db
        '''
        _cube = self.get_collection(collection)
        spec = self._parse_query(query, date)
        result = _cube.find(spec).count()
        return result

    def distinct(self, field, query=None, date=None, collection=None):
        '''
        Return back a distinct (unique) list of field values
        across the entire cube dataset

        :param field: field to get distinct token values from
        :param collection: cube name
        :param db: username of cube db
        '''
        _cube = self.get_collection(collection)
        if query:
            spec = self._parse_query(query, date)
            result = _cube.find(spec).distinct(field)
        else:
            result = _cube.distinct(field)
        return result

    def drop_db(self, db=None):
        db = db or self.config['db']
        return self.proxy.drop_database(db)

    def drop(self, *args, **kwargs):
        return self.drop_collections(*args, **kwargs)

    def drop_collections(self, collections=None):
        collections = collections or []
        if not collections or collections is True:
            collections = self.ls()
        else:
            collections = list(collections)
        db = self.get_db()
        return [db.drop_collection(c) for c in collections
                if c not in self.RESTRICTED_COLLECTIONS]

    def ensure_collection(self, collection=None, **kwargs):
        collection = collection or self.config.get('table')
        is_defined(collection, 'collection name can not be null!')
        db = self.get_db()
        return db.create_collection(collection, **kwargs)

    @property
    def exists(self, collection=None, db=None):
        collection = collection or self.config.get('table')
        is_defined(collection, 'collection must be defined!')
        db = db or self.config['db']
        return collection in self.proxy.ls()

    def find(self, query=None, fields=None, date=None, sort=None,
             one=False, raw=False, explain=False, merge_versions=False, skip=0,
             limit=0, as_cursor=False, collection=None):
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
        :param collection: cube name
        :param db: username of cube db
        '''
        _cube = self.get_collection(collection)
        spec = self._parse_query(query, date)
        fields = parse.parse_fields(fields, as_dict=True) or None

        merge_versions = False if fields is None or one else merge_versions
        if merge_versions:
            fields = fields or {}
            fields.update({'_start': 1, '_end': 1, '_oid': 1})

        find = _cube.find_one if one else _cube.find
        result = find(spec, fields=fields, sort=sort, explain=explain,
                      skip=skip, limit=limit)

        if merge_versions:
            result = self._merge_versions(result)
        if one or explain or as_cursor or raw:
            return result
        else:
            result = list(result)
            return Result(result, date)

    def get_last_field(self, field,  collection=None):
        '''Shortcut for querying to get the last field value for
        a given db, cube.

        :param field: field name to query
        '''
        last = self.find(collection=collection, query=None, fields=[field],
                         sort=[(field, -1)], one=True, raw=True)
        if last:
            last = last.get(field)
        logger.debug("last %s: %s" % (field, last))
        return last

    def index_list(self, collection=None):
        '''
        List all cube indexes

        :param collection: cube name
        :param db: username of cube db
        '''
        _cube = self.get_collection(collection)
        logger.info('[%s] Listing indexes' % _cube)
        result = _cube.index_information()
        return result

    def index(self, key_or_list, name=None, collection=None, **kwargs):
        '''
        Build a new index on a cube.

        Examples:
            + ensure_index('field_name')
            + ensure_index([('field_name', 1), ('other_field_name', -1)])

        :param key_or_list: A single field or a list of (key, direction) pairs
        :param name: (optional) Custom name to use for this index
        :param background: MongoDB should create in the background
        :param collection: cube name
        :param db: username of cube db
        '''
        _cube = self.get_collection(collection)
        logger.info('[%s] Writing new index %s' % (_cube, key_or_list))
        if name:
            kwargs['name'] = name
        result = _cube.ensure_index(key_or_list, **kwargs)
        return result

    def index_drop(self, index_or_name, collection=None):
        '''
        Drops the specified index on this cube.

        :param index_or_name: index (or name of index) to drop
        :param collection: cube name
        :param db: username of cube db
        '''
        _cube = self.get_collection(collection)
        logger.info('[%s] Droping index %s' % (_cube, index_or_name))
        result = _cube.drop_index(index_or_name)
        return result

    def insert(self, objects, fast=True, collection=None):
        _cube = self.get_collection(collection)
        objects = objects.values() if isinstance(objects, dict) else objects
        # need to be sure we are working with dicts...
        objects = [dict(o) for o in objects]
        is_array(objects, 'objects must be a list')
        if fast:
            _ids = [o['_id'] for o in objects]
            _cube.insert(objects, manipulate=False)
        else:
            _ids = _cube.insert(objects, manipulate=True)
        return _ids

    def save(self, objects, fast=True, collection=None):
        _cube = self.get_collection(collection)
        objects = objects.values() if isinstance(objects, dict) else objects
        objects = [dict(o) for o in objects]
        if fast:
            _ids = [o['_id'] for o in objects]
            [_cube.save(o, manipulate=False) for o in objects]
        else:
            _ids = [_cube.save(o, manipulate=True) for o in objects]
        return _ids

    def ls(self, include_sys=False):
        db = self.get_db()
        collections = db.collection_names(include_sys)
        return collections

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

    def sample_docs(self, sample_size=1, query=None, date=None,
                    collection=None):
        '''
        Take a randomized sample of documents from a cube.

        :param sample_size: number of random documents to query
        :param query: `pql` query to filter sample query with
        :param collection: cube name
        :param db: username of cube db
        :returns list: sorted list of fields
        '''
        _cube = self.get_collection(collection)
        sample_size = sample_size or 1
        spec = self._parse_query(query, date)
        docs = _cube.find(spec)
        n = docs.count()
        if n <= sample_size:
            docs = list(docs)
        else:
            to_sample = sorted(set(random.sample(xrange(n), sample_size)))
            docs = [docs[i] for i in to_sample]
        return docs

    def sample_fields(self, sample_size=None, query=None, date=None,
                      collection=None):
        '''
        List a sample of all valid fields for a given cube.

        Assuming all cube objects have the same exact fields, sampling
        fields should result in a complete list of object fields.

        However, if cube objects have different fields, sampling fields
        might not result in a complete list of object fields, since
        some object variants might not be included in the sample queried.

        :param sample_size: number of random documents to query
        :param query: `pql` query to filter sample query with
        :param collection: cube name
        :param db: username of cube db
        :returns list: sorted list of fields
        '''
        docs = self.sample_docs(collection=collection, sample_size=sample_size,
                                query=query, date=date)
        result = sorted({k for d in docs for k in d.iterkeys()})
        return result

    def upsert(self, objects, autosnap=None, collection=None):
        collection = self.get_collection(collection)
        objects = objects.values() if isinstance(objects, dict) else objects
        objects = [dict(o) for o in objects]
        is_array(objects, 'objects must be a list')
        if autosnap is None:
            # assume autosnap:True if all objects have _end:None
            # otherwise, false (all objects have _end:non-null or
            # a mix of both)
            autosnap = all([o['_end'] is None for o in objects])
            logger.warn('AUTOSNAP auto-set to: %s' % autosnap)

        _ids = [o['_id'] for o in objects]
        q = '_id in %s' % _ids
        t1 = time()

        dups = {o['_id']: dict(o) for o in self.find(collection=collection,
                                                     query=q, fields='~',
                                                     date='~', as_cursor=True)}
        diff = int(time() - t1)
        logger.debug(
            'dup query completed in %s seconds (%s)' % (diff, len(dups)))

        dup_k, snap_k = 0, 0
        inserts = []
        saves = []
        for i, o in enumerate(objects):
            dup = dups.get(o['_id'])
            if dup:
                if o['_hash'] == dup['_hash']:
                    dup_k += 1
                elif o['_end'] is None and autosnap:
                    # set existing objects _end to new objects _start
                    dup['_end'] = o['_start']
                    # update _id, _hash, etc
                    dup = MetriqueObject(**dup)
                    _ids.append(dup['_id'])
                    # insert the new object
                    inserts.append(dup)
                    # replace the existing _end:None object with new values
                    _id = o['_id']
                    saves.append(o)
                    snap_k += 1
                else:
                    o = MetriqueObject(**o)
                    # don't try to set _id
                    _id = o.pop('_id')
                    assert _id == dup['_id']
                    saves.append(o)
            else:
                inserts.append(o)

        if inserts:
            t1 = time()
            self.insert(inserts)
            diff = int(time() - t1)
            logger.debug('%s inserts in %s seconds' % (len(inserts), diff))
        if saves:
            t1 = time()
            self.save(saves)
            diff = int(time() - t1)
            logger.debug('%s saved in %s seconds' % (len(saves), diff))
        logger.debug('%s existing objects snapshotted' % snap_k)
        logger.debug('%s duplicates not re-saved' % dup_k)
        return sorted(map(unicode, _ids))


class MongoDBContainer(MetriqueContainer):
    config = None
    config_key = 'mongodb'
    config_file = DEFAULT_CONFIG
    default_fields = {'_start': 1, '_end': 1, '_oid': 1}
    default_sort = [('_start', -1)]
    db = None
    name = None
    VALID_SHARE_ROLES = ['read', 'readWrite', 'dbAdmin', 'userAdmin']
    CUBE_OWNER_ROLES = ['readWrite', 'dbAdmin', 'userAdmin']

    def __init__(self, name, objects=None, proxy=None, batch_size=None,
                 host=None, port=None, username=None, password=None,
                 auth=None, ssl=None, ssl_certificate=None,
                 read_preference=None,
                 replica_set=None, tz_aware=None, write_concern=None,
                 db=None, config_file=None, config_key=None,
                 version=None, **kwargs):
        '''
        Accept additional kwargs, but ignore them.
        '''
        if not HAS_PYMONGO:
            raise RuntimeError('`pip install pymongo` 2.6+ required')

        super(MongoDBContainer, self).__init__(name=name,
                                               objects=objects,
                                               version=version)

        options = dict(auth=auth,
                       batch_size=batch_size,
                       host=host,
                       name=name,
                       db=db,
                       password=password,
                       port=port,
                       read_preference=read_preference,
                       replica_set=replica_set,
                       ssl=ssl,
                       ssl_certificate=ssl_certificate,
                       tz_aware=tz_aware,
                       username=username,
                       write_concern=write_concern)
        # set to None because these are passed to proxy
        # which sets defaults accordingly
        defaults = dict(auth=None,
                        batch_size=None,
                        host=None,
                        name=self.name,
                        db=None,
                        password=None,
                        port=None,
                        read_preference=None,
                        replica_set=None,
                        ssl=None,
                        ssl_certificate=None,
                        tz_aware=None,
                        username=None,
                        write_concern=None)
        self.config = self.config or {}
        self.config_file = config_file or self.config_file
        self.config_key = config_key or MongoDBContainer.config_key
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                section_only=True,
                                update=self.config)
        self.config['db'] = self.config['db'] or defaults.get('username')

        # if we get proxy, should we update .config with proxy.config?
        if proxy:
            self._proxy = proxy

    @property
    def proxy(self):
        if not getattr(self, '_proxy', None):
            name = self.config.get('name')
            self._proxy = MongoDBProxy(collection=name,
                                       config_file=self.config_file,
                                       **self.config)
        return self._proxy

    def persist(self, objects=None, autosnap=True):
        objects = objects or self.values()
        self._ensure_base_indexes()
        return self.proxy.upsert(collection=self.name, objects=objects,
                                 autosnap=autosnap)

    def _ensure_base_indexes(self):
        _cube = self.proxy.get_collection()
        _cube.ensure_index('_oid', background=False)
        _cube.ensure_index('_hash', background=False)
        _cube.ensure_index([('_start', -1), ('_end', -1)],
                           background=False)
        _cube.ensure_index([('_end', -1)],
                           background=False)

    def share(self, with_user, roles=None, db=None):
        '''
        Give cube access rights to another user
        '''
        with_user = validate_username(with_user, self.RESTRICTED_COLLECTIONS)
        roles = validate_roles(roles or ['read'], self.VALID_SHARE_ROLES)
        _cube = self.proxy.get_db(db)
        logger.info(
            '[%s] Sharing cube with %s (%s)' % (_cube, with_user, roles))
        result = _cube.add_user(name=with_user, roles=roles,
                                userSource=with_user)
        return result

    def rename(self, new_name=None, new_db=None, drop_target=False,
               collection=None, db=None):
        '''
        Rename a cube.

        :param new_name: new cube name
        :param new_db: new cube db (admin privleges required!)
        :param collection: cube name
        :param db: username of cube db
        '''
        if not (new_name or new_db):
            raise ValueError("must set either/or new_name or new_db")
        new_name = new_name or collection or self.name
        if not new_name:
            raise ValueError("new_name is not set!")
        _cube = self.proxy.get_collection(db, collection)
        if new_db:
            _from = _cube.full_name
            _to = '%s.%s' % (new_db, new_name)
            self.proxy.get_db('admin').command(
                'renameCollection', _from, to=_to, dropTarget=drop_target)
            # don't touch the new collection until after attempting
            # the rename; collection would otherwise be created
            # empty automatically then the rename fails because
            # target already exists.
            _new_db = self.proxy.get_db(new_db)
            result = bool(new_name in _new_db.collection_names())
        else:
            logger.info('[%s] Renaming cube -> %s' % (_cube, new_name))
            _cube.rename(new_name, dropTarget=drop_target)
            db = self.proxy.get_db(db)
            result = bool(new_name in db.collection_names())
        if collection is None and result:
            self.name = new_name
        return result

    def remove(self, query, date=None, collection=None, db=None):
        '''
        Remove objects from a cube.

        :param query: `pql` query to filter sample query with
        :param collection: cube name
        :param db: username of cube db
        '''
        spec = self._parse_query(query, date)
        _cube = self.proxy.get_collection(db, collection)
        logger.info("[%s] Removing objects (%s): %s" % (_cube, date, query))
        result = _cube.remove(spec)
        return result

    def aggregate(self, pipeline, collection=None, db=None):
        '''
        Run a pql mongodb aggregate pipeline on remote cube

        :param pipeline: The aggregation pipeline. $match, $project, etc.
        :param collection: cube name
        :param db: username of cube db
        '''
        _cube = self.proxy.get_collection(db, collection)
        result = _cube.aggregate(pipeline)
        return result

    def history(self, query, by_field=None, date_list=None, collection=None,
                db=None):
        '''
        Run a pql mongodb based query on the given cube and return back the
        aggregate historical counts of matching results.

        :param query: The query in pql
        :param by_field: Which field to slice/dice and aggregate from
        :param date: list of dates that should be used to bin the results
        :param collection: cube name
        :param db: username of cube db
        '''
        query = '%s and _start < %s and (_end >= %s or _end == None)' % (
                query, max(date_list), min(date_list))
        spec = self._parse_query(query)

        pipeline = [
            {'$match': spec},
            {'$group': {
                '_id': '$%s' % by_field if by_field else '_id',
                'starts': {'$push': '$_start'},
                'ends': {'$push': '$_end'}}}]
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

    def deptree(self, field, oids, date=None, level=None, collection=None,
                db=None):
        '''
        Dependency tree builder. Recursively fetchs objects that
        are children of the initial set of parent object ids provided.

        :param field: Field that contains the 'parent of' data
        :param oids: Object oids to build depedency tree for
        :param date: date (metrique date range) that should be queried.
                    If date==None then the most recent versions of the
                    objects will be queried.
        :param level: limit depth of recursion
        :param collection: cube name
        :param db: username of cube db
        '''
        # FIXME check
        if not level or level < 1:
            level = 1
        if isinstance(oids, basestring):
            oids = [s.strip() for s in oids.split(',')]
        checked = set(oids)
        fringe = oids
        loop_k = 0
        _cube = self.proxy.get_collection(db, collection)
        while len(fringe) > 0:
            if level and loop_k == abs(level):
                break
            query = '_oid in %s and %s != None' % (fringe, field)
            spec = self._parse_query(query, date)
            fields = {'_id': -1, '_oid': 1, field: 1}
            docs = _cube.find(spec, fields=fields)
            fringe = set([oid for doc in docs for oid in doc[field]])
            fringe = filter(lambda oid: oid not in checked, fringe)
            checked |= set(fringe)
            loop_k += 1
        return sorted(checked)

    def user_register(self, username=None, password=None):
        '''
        Register new user.

        :param user: Name of the user you're managing
        :param password: Password (plain text), if any of user
        '''
        password = password or self.config.get('password')
        username = username or self.config.get('username')
        if username and not password:
            raise RuntimeError('must specify password!')
        username = validate_username(username, self.RESTRICTED_COLLECTIONS)
        password = validate_password(password)
        logger.info('Registering new user %s' % username)
        db = self.proxy.get_db(username)
        db.add_user(username, password,
                    roles=self.CUBE_OWNER_ROLES)
        spec = self._parse_query('user == "%s"' % username)
        result = db.system.users.find(spec).count()
        return bool(result)

    def user_remove(self, username, clear_db=False):
        username = username or self.config.get('username')
        username = validate_username(username, self.RESTRICTED_COLLECTIONS)
        logger.info('Removing user %s' % username)
        db = self.proxy.get_db(username)
        db.remove_user(username)
        if clear_db:
            db.drop_database(username)
        spec = self._parse_query('user == "%s"' % username)
        result = not bool(db.system.users.find(spec).count())
        return result
