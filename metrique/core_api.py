#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

# FIXME: add to *Container a 'sync' command which will export
# across the network all data, persist to some other container
# and enable future 'delta' syncs.

'''
metrique.core_api
~~~~~~~~~~~~~~~~~

This module containes metrique's core data objects and data
object containers.

MetriqueObject is python Mapping object which comes equipped with:

 * a simple architecture-neautral hashing alorithm that automatically
   produces consistent object identifiers (_hash) for the object's
   current value state.
 * an automatic, historical versioning id (_id) generator which
   provides for a convenient mechanism for transparently storing
   historical object states over time for easy querying, later.

MetriqueContainer is a indexed python Mapping container for local and
remote storage of MetriqueObjects, which also comes equipped with:
 * default data normalizers
 * simple, sane, extensible typecasting and data conversion
 * full support for multiple object storage backends, including:
  * SQLite (default)
  * PostgreSQL
 * local and remote querying, using a very simple and consistent query syntax


Things to improve going forward:

 * Performance. Will look into Cython for next release.
 * support local querying using MPL parser

'''

from __future__ import unicode_literals, absolute_import

import logging
logger = logging.getLogger('metrique')

from collections import Mapping, MutableMapping
from copy import copy
from datetime import datetime, date
from inspect import isclass
from itertools import groupby
import os
from operator import add
import re
from time import time
from types import NoneType
import warnings

from metrique._version import __version__
from metrique.utils import utcnow, jsonhash, load, autoschema
from metrique.utils import dt2ts, configure, to_encoding
from metrique.utils import is_null, is_array, is_defined
from metrique.result import Result

ETC_DIR = os.environ.get('METRIQUE_ETC')
CACHE_DIR = os.environ.get('METRIQUE_CACHE') or '/tmp'
DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metrique.json')
HASH_EXCLUDE_KEYS = ('_hash', '_id', '_start', '_end', '__v__', 'id')
IMMUTABLE_OBJ_KEYS = set(['_oid', '_hash', '_id', 'id'])


def gen_id(_oid, _start, _end=None):
    # if the object is 'current value' without _end,
    # use just str of _oid (DEFAULT)
    if _oid is None:
        raise ValueError("_oid must be defined!")
    elif _end:
        _oid = '%s:%s' % (_oid, _start)
    else:
        _oid = to_encoding(_oid)
    return _oid


def metrique_object(_oid, _id=None, _hash=None, _start=None, _end=None,
                    _e=None, _v=None, id=None, __v__=None, **kwargs):
    '''
    Function which takes a dictionary (Mapping) object as input
    and returns return back a metrique object.

    Special meta property are added to each object::
        _oid: ...
        _start: ...
        ...
        FIXME
    '''
    # NOTE: we completely ignore incoming 'id' keys!
    # id is RESERVED and ALWAYS expected to be 'autoincrement'
    # upon insertion into DB (though, its optional, depending
    # on backend storage behaivor).
    if id:
        warnings.warn('non-null "id" keys detected, ignoring them!')

    _e = dict(_e or {})  # expecting a dict with copy() atr
    _v = int(_v or 0)

    if not isinstance(_start, float):
        _start = dt2ts(_start) if _start else utcnow(as_datetime=False)
    assert _start is not None, "_start (%s) must be set!" % _start

    if not isinstance(_end, float):
        _end = dt2ts(_end) if _end else None

    _err_msg = "_end(%s) must be >= _start(%s) or None!" % (_end, _start)
    assert _end is None or bool(_end >= _start), _err_msg

    # these meta fields are used to generate unique object _hash
    kwargs['_oid'] = _oid
    kwargs['_v'] = _v
    kwargs['_id'] = gen_id(_oid, _start, _end)  # ignore passed in _id
    # generate unique, consistent object _hash based on 'frozen' obj contents
    # FIXME: make _hash == None valid
    #kwargs['_hash'] = jsonhash(kwargs) if _hash else None
    kwargs['_hash'] = jsonhash(kwargs)

    # add some additional non-hashable meta data
    kwargs['_start'] = _start
    kwargs['_end'] = _end
    kwargs['__v__'] = __v__ or __version__
    kwargs['_e'] = _e
    return kwargs


# FIXME: all objects should have the SAME keys;
# if an object is added with fewer keys, it should
# have the missing keys added with null values
class MetriqueContainer(MutableMapping):
    '''
    :param name: name of container
    :param db: db to map container to
    :param schema: container object schema (type definition)
    :param version: container object version
    :param objects: list of objects to add to container upon init
    :param proxy: inialized backend data storage proxy
    :param proxy_config: config to pass to proxy upon proxy init
    :param batch_size: override of the number of objects to process at once
    :param config: base config template to use during init
    :param config_file: config file to load
    :param config_key: config sub section (dict key) to use load
    :param cache_dir: overide of default cache path
    :param autotable: bool to automatically issue 'create' command to
                        storage proxy, if set

    Additional kwargs are accepted, but ignored.

    Essentially, cubes are data made from a an object id indexed (_oid)
    dictionary (keys) or dictionary "objects" (values)

    All objects are expected to contain a `_oid` key value property. This
    property should be unique per individual "object" defined. But any
    container can hold zero, one or more objects with the same _oid value.

    For example, if we are storing logs, we might consider each log line a
    separate "object" since those log lines should never change in the
    future and give each a unique `_oid`. Or if we are storing data about
    'meta objects' of some sort, say 'github repo issues' for example, we
    might have objects with _oids of
    `%(username)s_%(reponame)s_%(issuenumber)s`.

    Field names (object dict keys), upon adding them to the container
    will always be normalized so they consist of lower-case alphanumeric
    and (single) underscore characters only.

    Containers are by default in-memory, but configured with a 'SQLite'
    disk-based storage proxy, which (nearly) fully supports all
    enhanced querying and historical upserting functionality that metrique
    provides.

    Object's storeage within a given container are expected to be
    homogenious.

    A object schema can optionally be defined upon initialization that
    defines object.field types and whether the items are in single or
    'container' (array) form.

    This schema can be defined by defining the 'schema' kwarg, as such::
        schema = dict(
            summary = {'type': unicode},
            keywords = {'type': unicode, container: True},
            created = {'type': datetime.datetime},
        )

    Types currently supported are as follows:

        * null: None, NaN, NaT
        * numerical: int, float, long
        * string: str, unicode
        * bool: True, False
        * array: list, tuple, set
        * mapping: dict, Mapping
        * datetime:
            * epoch (as float, int, long)
            * datetime.datetime, datetime.date
            * date string, parsable by dateutils.parse

    #FIXME
    It is also possible to define 'variants' within schema::
        schema = dict(
        ...
        age = {...,
        variants: {
            'is_old': lambda(v, o) : True if v > 40 else False}},
        is_old = {'type': bool},
        )

    If no schema is pre-defined, a schema will be automatically
    generated based on the first object added!

    Example container config file (~/.metrique/metrique.json)::
    {
        "container": {
            "proxy": {
                "dialect": "postgresql",
                "port": 5432,
                "host": "127.0.0.1",
                "password": "_super_secure_",
                "username": "metrique",
                "db": "metrique"
            }
        }
    '''
    _key_map = None
    _object_cls = None
    _proxy_cls = None
    _proxy = None
    config = None
    config_file = DEFAULT_CONFIG
    config_key = 'container'
    db = None
    name = None
    proxy_config_key = 'proxy'
    store = None
    version = 0
    HASH_EXCLUDE_KEYS = tuple(HASH_EXCLUDE_KEYS)
    RESTRICTED_KEYS = ('id', '_id', '_hash', '_start', '_end',
                       '_v', '__v__', '_e')
    FIELDS_RE = re.compile('[\W]+')
    SPACE_RE = re.compile('\s+')
    UNDA_RE = re.compile('_+')

    def __init__(self, name=None, db=None, schema=None, version=None,
                 objects=None, proxy=None, proxy_config=None,
                 batch_size=None, config=None, config_file=None,
                 config_key=None, cache_dir=None, autotable=None,
                 **kwargs):
        # null name -> anonymous table; no native ability to persist
        options = dict(autotable=autotable,
                       cache_dir=cache_dir,
                       batch_size=batch_size,
                       name=None,
                       schema=schema,
                       version=int(version or 0))

        defaults = dict(autotable=True,
                        cache_dir=CACHE_DIR,
                        batch_size=999,
                        name=name,
                        schema={},
                        version=0)

        # if config is passed in, set it, otherwise start
        # with class assigned default or empty dict
        self.config = copy(config or MetriqueContainer.config or {})
        self.config_file = config_file or MetriqueContainer.config_file
        self.config_key = config_key or MetriqueContainer.config_key
        # load defaults + set args passed in
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                section_only=True,
                                update=self.config)

        self.name = self.config.get('name') or MetriqueContainer.name
        self.version = (self.config.get('version') or
                        MetriqueContainer.version)

        proxy_config = dict(proxy_config or {})
        proxy_config.setdefault('db', db)
        proxy_config.setdefault('table', self.name)
        proxy_config.setdefault('config_file', self.config_file)
        self.config.setdefault(self.proxy_config_key, {}).update(proxy_config)

        if self._object_cls is None:
            self._object_cls = metrique_object

        if self._proxy_cls is None:
            from metrique.sqlalchemy import SQLAlchemyProxy
            self._proxy_cls = SQLAlchemyProxy
        self._proxy = proxy

        # init and update internal store with passed in objects, if any
        self.store = copy(MetriqueContainer.store or {})
        self._update(objects)

    def __getitem__(self, key):
        '''
        Support getting index slices or individiual objects. Expected
        _id index values.
        '''
        if isinstance(key, slice):
            keys = sorted(self.store.keys())[key]
            return [dict(self.store[i]) for i in keys]
        else:
            key = to_encoding(key)
            return dict(self.store[key])

    def __setitem__(self, key, value):
        '''
        Add (set) object value. key being used to set is irrelevant
        and ignored!
        '''
        self.add(value)

    def __delitem__(self, key):
        key = to_encoding(key)
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __contains__(self, key):
        key = to_encoding(key)
        return key in self.store.keys()

    def __repr__(self):
        return repr(self.store)

    def _add_variants(self, key, value, schema):
        ''' also possible to define some function that takes
            current value and creates a new value from it
        '''
        variants = schema.get('variants')
        obj = {}
        if variants:
            for _key, func in variants.iteritems():
                _value = func(value, self.store)
                obj.update({_key: _value})
        return obj

    @property
    def _ids(self):
        return sorted(self.store.keys())

    @property
    def _oids(self):
        return sorted({o['_oid'] for o in self.store.itervalues()})

    def _parse_query(self, query=None, fields=None, date=None,
                     alias=None, distinct=None, limit=None):
        return self.proxy._parse_query(table=self.name, query=query,
                                       fields=fields, date=date, alias=alias,
                                       distinct=distinct, limit=limit)

    def _normalize_container(self, value, schema):
        container = bool(schema.get('container'))
        _is_list = isinstance(value, list)
        if container and not _is_list:
            # NORMALIZE to empty list []
            return list(value) if value else []
        elif not container and _is_list:
            raise ValueError(
                "expected single value, got list (%s)" % value)
        else:
            return value

    def _unwrap(self, value):
        if type(value) is buffer:
            # unwrap/convert the aggregated string 'buffer'
            # objects to string
            value = to_encoding(value)
            # FIXME: this might cause issues if the buffered
            # text has " quotes...
            value = value.replace('"', '').strip()
            if not value:
                value = None
            else:
                value = value.split('\n')
        return value

    def _convert(self, value, schema=None):
        schema = schema or {}
        convert = schema.get('convert')
        container = schema.get('container')
        try:
            if value is None:
                return None
            elif convert and container:
                value = map(convert, value)
            elif convert:
                value = convert(value)
            else:
                pass
        except Exception:
            logger.error("convert Failed: %s(value=%s, container=%s)" % (
                convert.__name__, value, container))
            raise
        return value

    def _normalize_key(self, key):
        key = to_encoding(key).lower()
        if key != '__v__':
            # skip internal metrique version field
            key = self.SPACE_RE.sub('_', key)
            key = self.FIELDS_RE.sub('',  key)
            key = self.UNDA_RE.sub('_',  key)
        return key

    def _normalize_keys(self, obj):
        return {self._normalize_key(k): v for k, v in obj.iteritems()}

    def _prep_object(self, obj):
        obj = self._normalize_keys(obj)
        schema = self.schema
        if not schema:
            # build schema off normalized key version
            _obj = self._normalize_keys(obj)
            # cache a key map from old key->normalized keys
            self._key_map = {k: self._normalize_key(k) for k in obj.iterkeys()}
            # in the case we don't have a schema already defined, we need to
            # build on now; all objects are assumed to have the SAME SCHEMA!
            schema = autoschema([_obj], exclude_keys=self.RESTRICTED_KEYS)
            self.config['schema'] = schema

        # optimization; lookup in local scope
        kmap = self._key_map or {}
        for key, value in obj.items():
            # map original key to normalized key, if normal map exists
            _key = kmap.get(key) or key
            _schema = schema.get(_key) or {}
            try:
                value = self._prep_value(value, schema=_schema)
            except Exception as e:
                # make sure the value contents are loggable
                _value = to_encoding(value)
                obj.setdefault('_e', {})
                msg = 'prep(key=%s, value=%s) failed: %s' % (_key, _value, e)
                logger.error(msg)
                # set error field with original values
                obj['_e'].update({_key: value})

                # FIXME: should we leave original as-is? if not of correct
                # type, etc, this might cause problems

                # normalize invalid value to None
                value = None
            obj[key] = value
            variants = self._add_variants(_key, value, _schema)
            obj.update(variants)
        obj['_v'] = self.version
        obj = self._object_cls(**obj)
        return obj

    def _prep_value(self, value, schema):
        # NOTE: if we fail anywhere in here, no changes made here will
        # be 'saved'; buffer's for example will remain buffers, etc.
        # PERFORMANCE NOTES
        # 26 seconds for 450k values, baseline; none of the folling are run
        value = self._unwrap(value)
        # +2 seconds (28s)
        value = self._normalize_container(value, schema)
        # +12 seconds (40s)
        value = self._convert(value, schema)
        # +6 seconds (46s)
        value = self._typecast(value, schema)
        # +10 seconds (56s)
        return value

    def _typecast(self, value, schema):
        _type = schema.get('type')
        container = schema.get('container')
        if container:
            value = self._type_container(value, _type)
        else:
            value = self._type_single(value, _type)
        return value

    def _type_container(self, value, _type):
        ' apply type to all values in the list '
        if value is None:
            # normalize null containers to empty list
            return []
        elif not isinstance(value, list):
            raise ValueError("expected list type, got: %s" % type(value))
        else:
            return sorted(self._type_single(item, _type) for item in value)

    def _type_single(self, value, _type):
        ' apply type to the single value '
        if value is None or _type in (None, NoneType):
            # don't convert null values
            # default type is the original type if none set
            pass
        elif isinstance(value, _type):  # or values already of correct type
            # normalize all dates to epochs
            value = dt2ts(value) if _type in [datetime, date] else value
        else:
            if _type in (datetime, date):
                # normalize all dates to epochs
                value = dt2ts(value)
            elif _type in (unicode, str):
                # make sure all string types are properly unicoded
                value = to_encoding(value)
            else:
                try:
                    value = _type(value)
                except Exception:
                    value = to_encoding(value)
                    logger.error("typecast failed: %s(value=%s)" % (
                        _type.__name__, value))
                    raise
        return value

    def _update(self, objects):
        if is_null(objects):
            pass
        elif is_array(objects, except_=False):
            [self.add(x) for x in tuple(objects)]
        elif isinstance(objects, MetriqueContainer):
            [self.add(o) for o in objects.itervalues()]
        else:
            raise ValueError(
                "objs must be None, a list, tuple, dict or MetriqueContainer")

    def add(self, obj):
        obj = self._prep_object(obj)
        # objects are stored indexed by _id
        self.store[obj['_id']] = obj

    def autotable(self):
        name = self.config.get('name')
        # if we have a table already, we want only to load
        # a corresponding sqla.Table instance so our ORM
        # works as expected; if no table and autotable:True,
        # create the table too.
        create = self.config.get('autotable')
        if name in self.proxy.meta_tables:
            logger.warn('autotable "%s": already exists' % name)
            result = True
        else:
            if name in self.proxy.db_tables:
                # no reason to create the table again...
                # but we still want to load the table class into metadata
                create = False
            self.proxy.autotable(schema=self.schema, name=name,
                                 create=create)
            logger.warn('autotable "%s": (create=%s): OK' % (name, create))
            result = True
        return result

    def clear(self):
        self.store = {}

    def columns(self):
        return self.proxy.db_columns

    def df(self):
        '''Return a pandas dataframe (metrique.result.Result) from objects'''
        return Result(self.store.values())

    def extend(self, objs):
        logger.debug('extending container by %s objs...' % len(objs))
        s = time()
        [self.add(i) for i in objs]
        diff = time() - s
        k = len(objs)
        rate = (k / diff) if k > 0 else 0
        logger.debug('... extended container by %s objs in %ss at %.2f/s' % (
            len(objs), int(diff), rate))

    def flush(self, objects=None, batch_size=None, **kwargs):
        ''' flush objects stored in self.container or those passed in'''
        batch_size = batch_size or self.config.get('batch_size')
        # if we're flushing these from self.store, we'll want to
        # pop them later.
        if objects:
            from_store = False
        else:
            from_store = True
            objects = self.itervalues()
        # sort by _oid for grouping by _oid below
        objects = sorted(objects, key=lambda x: x['_oid'])
        batch, _ids = [], []
        # batch in groups with _oid, since upsert's delete
        # all _oid rows when autosnap=False!
        for key, group in groupby(objects, lambda x: x['_oid']):
            _grouped = list(group)
            if len(batch) + len(_grouped) > batch_size:
                logger.debug("Upserting %s objects" % len(batch))
                _ = self.upsert(objects=batch, **kwargs)
                logger.debug("... done upserting %s objects" % len(batch))
                _ids.extend(_)
                # start a new batch
                batch = _grouped
            else:
                # extend existing batch, since still will be < batch_size
                batch.extend(_grouped)
        else:
            if batch:
                # get the last batch too
                logger.debug("Upserting last batch of %s objects" % len(batch))
                _ = self.upsert(objects=batch, **kwargs)
                _ids.extend(_)
            logger.debug("... Finished upserting all objects!")

        if from_store:
            for _id in _ids:
                # try to pop the _id's flushed from store; warn / ignore
                # the KeyError if they're not there
                try:
                    self.store.pop(_id)
                except KeyError:
                    logger.warn(
                        "failed to pop {} from self.store!".format(_id))
        return sorted(_ids)

    def find(self, query=None, fields=None, date=None, sort=None,
             descending=False, one=False, raw=False, limit=None,
             as_cursor=False, scalar=False, default_fields=True):
        return self.proxy.find(table=self.name, query=query, fields=fields,
                               date=date, sort=sort, descending=descending,
                               one=one, raw=raw, limit=limit,
                               as_cursor=as_cursor, scalar=scalar,
                               default_fields=default_fields)

    def filter(self, where):
        if not isinstance(where, Mapping):
            raise ValueError("where must be a dict")
        else:
            result = []
            for obj in self.store.itervalues():
                found = False
                for k, v in where.iteritems():
                    if obj.get(k, '') == v:
                        found = True
                    else:
                        found = False
                if found:
                    result.append(obj)
        return result

    @property
    def fields(self):
        return sorted({k for o in self.store.itervalues()
                       for k in o.iterkeys()})

    @staticmethod
    def load(*args, **kwargs):
        ''' wrapper for utils.load automated data loader '''
        return load(*args, **kwargs)

    def itervalues(self):
        for v in self.store.itervalues():
            yield dict(v)

    def ls(self):
        raise NotImplementedError

    def pop(self, key):
        key = to_encoding(key)
        return self.store.pop(key)

    @property
    def proxy(self):
        if self._proxy is None or isclass(self._proxy):
            self.proxy_init()
        # make sure we aways have the current schema definition applied
        self._proxy.config['schema'] = self.schema
        return self._proxy

    @property
    def proxy_config(self):
        self.config.setdefault(self.proxy_config_key, {})
        return self.config[self.proxy_config_key]

    def proxy_init(self):
        is_defined(self.name, "name can not be null!")
        config = self.proxy_config
        # make sure we pass along the current schema definition
        config['schema'] = self.schema
        if self._proxy is None:
            self._proxy = self._proxy_cls
        # else: _proxy is a proxy_cls
        self._proxy = self._proxy(**config)

    def objects(self):
        return self.store.values()

    def values(self):
        return [dict(v) for v in self.store.itervalues()]

    @property
    def schema(self):
        return self.config.get('schema')

    def count(self, query=None, date=None):
        '''
        Run a query on the given cube and return only
        the count of resulting matches.

        :param query: The query in pql
        :param date: date (metrique date range) that should be queried
                    If date==None then the most recent versions of the
                    objects will be queried.
        :param collection: cube name
        :param owner: username of cube owner
        '''
        return self.proxy.count(table=self.name, query=query, date=date)

    def deptree(self, field, oids, date=None, level=None):
        return self.proxy.deptree(table=self.name, field=field,
                                  oids=oids, date=date, level=level)

    def distinct(self, fields, query=None, date='~'):
        date = date or '~'
        query = self._parse_query(query=query, date=date, fields=fields,
                                  alias='anon_x', distinct=True)
        ret = [r[0] for r in self.proxy.session_auto.execute(query)]
        if ret and isinstance(ret[0], list):
            ret = map(lambda v: v or [], ret)
            ret = reduce(add, ret, [])
        return sorted(set(ret))

    def drop(self, quiet=True):
        result = self.proxy.drop(tables=self.name, quiet=quiet)
        return result

    @property
    def exists(self):
        return self.proxy.exists(self.name)

    def get_last_field(self, field):
        return self.proxy.get_last_field(table=self.name, field=field)

    def index(self, fields, name=None, **kwargs):
        '''
        Build a new index on a cube.

        Examples:
            + index('field_name')

        :param fields: A single field or a list of (key, direction) pairs
        :param name: (optional) Custom name to use for this index
        :param collection: cube name
        :param owner: username of cube owner
        '''
        return self.proxy.index(fields=fields, name=name, table=self.name,
                                **kwargs)

    def index_list(self):
        '''
        List all cube indexes

        :param collection: cube name
        :param owner: username of cube owner
        '''
        return self.proxy.index_list()

    def insert(self, objects):
        return self.proxy.insert(table=self.name, objects=objects)

    def share(self, with_user, roles=None):
        '''
        Give cube access rights to another user
        '''
        return self.proxy.share(table=self.name, with_user=with_user,
                                roles=roles)

    def upsert(self, objects=None, autosnap=None):
        objects = objects or self
        return self.proxy.upsert(table=self.name, objects=objects,
                                 autosnap=autosnap)

    def user_register(self, username=None, password=None):
        password = password or self.config.get('password')
        username = username or self.config.get('username')
        return self.proxy.user_register(table=self.name, username=username,
                                        password=password)

    def user_disable(self, username):
        return self.proxy.user_disable(table=self.name, username=username)
