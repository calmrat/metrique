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
'''

from __future__ import unicode_literals, absolute_import

import logging
logger = logging.getLogger('metrique')

from collections import Mapping, MutableMapping
from copy import deepcopy
from datetime import datetime, date
from functools import partial
from inspect import isclass
import os
from operator import add

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    logger.warn('pandas module is not installed!')

import re

from time import time
from types import NoneType
import warnings

from metrique._version import __version__
from metrique.utils import utcnow, jsonhash, load, autoschema
from metrique.utils import batch_gen, dt2ts, configure, to_encoding
from metrique.utils import is_empty, is_true, is_null
from metrique import parse

ETC_DIR = os.environ.get('METRIQUE_ETC')
CACHE_DIR = os.environ.get('METRIQUE_CACHE') or '/tmp'
DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metrique.json')
HASH_EXCLUDE_KEYS = ('_hash', '_id', '_start', '_end', '__v__', 'id')


class MetriqueObject(MutableMapping):
    FIELDS_RE = re.compile('[\W]+')
    SPACE_RE = re.compile('\s+')
    UNDA_RE = re.compile('_+')
    IMMUTABLE_OBJ_KEYS = set(['_hash', '_id', 'id'])
    TIMESTAMP_OBJ_KEYS = set(['_end', '_start'])
    _VERSION = 0
    HASH_EXCLUDE_KEYS = tuple(HASH_EXCLUDE_KEYS)

    def __init__(self, _oid, _id=None, _hash=None, _start=None, _end=None,
                 _e=None, _v=None, id=None, _schema=None, **kwargs):
        if _oid is None:
            raise RuntimeError("_oid can not be None!")
        # NOTE: we completely ignore incoming 'id' keys!
        # id is RESERVED and ALWAYS expected to be 'autoincrement'
        # upon insertion into DB.
        if not is_empty(id, except_=False):
            warnings.warn('non-null "id" keys detected, ignoring them!')
        _start = dt2ts(_start) or utcnow(as_datetime=False)
        _end = dt2ts(_end)
        # FIXME: _end and _start should be dt or ts depending on as_datetime
        self.store = {
            '_oid': _oid,
            '_id': None,  # ignore passed in _id
            '_hash': None,  # ignore passed in _hash
            '_start': _start,
            '_end': _end,
            '_v': _v or self._VERSION,
            '__v__': __version__,
            '_e': _e,
        }
        self._schema = deepcopy(_schema or {})
        self.update(kwargs)
        self._re_hash()

    def __getitem__(self, key):
        key = self.__keytransform__(key)
        return self.store[key]

    def __setitem__(self, key, value):
        key = self.__keytransform__(key)
        self.update({key: value})
        self._re_hash()

    def __delitem__(self, key):
        self.pop(key)
        return None

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __repr__(self):
        return repr(self.store)

    def __hash__(self):
        return hash(self['_id'])

    def __keytransform__(self, key):
        key = to_encoding(key)
        key = key.lower()
        if key != '__v__':
            # skip our internal metrique version field
            key = self.SPACE_RE.sub('_', key)
            key = self.FIELDS_RE.sub('',  key)
            key = self.UNDA_RE.sub('_',  key)
        return key

    def _gen_id(self):
        _oid = self.store.get('_oid')
        assert _oid is not None
        if self.store.get('_end'):
            _start = self.store.get('_start')
            # if the object at the exact start/oid is later
            # updated, it's possible to just save(upsert=True)
            _id = ':'.join(map(str, (_oid, dt2ts(_start))))
        else:
            # if the object is 'current value' without _end,
            # use just str of _oid
            _id = _oid
        return unicode(_id)

    def _gen_hash(self):
        o = deepcopy(self.store)
        keys = set(o.keys())
        [o.pop(k) for k in self.HASH_EXCLUDE_KEYS if k in keys]
        return jsonhash(o)

    def _re_hash(self):
        # FIXME: validate all meta fields; make sure typed
        # correctly?
        self._validate_start_end()
        # _id depends on _hash
        # so first, _hash, then _id
        self.store['_hash'] = self._gen_hash()
        self.store['_id'] = self._gen_id()

    def _validate_start_end(self):
        _start = self.get('_start')
        if _start is None:
            raise ValueError("_start (%s) must be set!" % _start)
        _end = self.get('_end')
        # make sure we have the right type... float epoch
        _start = dt2ts(_start)
        _end = dt2ts(_end)
        if _end and _end < _start:
            raise ValueError(
                "_end (%s) is before _start (%s)!" % (_end, _start))
        self.store['_start'] = _start
        self.store['_end'] = _end

    def as_dict(self, pop=None):
        store = deepcopy(self.store)
        if pop:
            [store.pop(key, None) for key in pop]
        return store

    def pop(self, key):
        key = self.__keytransform__(key)
        is_true(key not in self.IMMUTABLE_OBJ_KEYS, '%s is immutable!' % key)
        value = self.store.pop(key)
        # _start and _end are simply 'reset' to dfault values if pop/deleted
        if key in self.TIMESTAMP_OBJ_KEYS:
            if key == '_start':
                self.store[key] = utcnow()
            else:
                self.store[key] = None
        self._re_hash()
        return value

    def setdefault(self, key, default):
        key = self.__keytransform__(key)
        if key not in self.store:
            self.update({key, default})
        return self

    def _normalize_container(self, value, schema):
        container = schema.get('container')
        is_list = isinstance(value, (list, tuple, set))
        if container and not is_list:
            # NORMALIZE to empty list []
            return [value] if value else []
        elif not container and is_list:
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
                _convert = partial(convert)
                value = map(_convert, value)
            elif convert:
                value = convert(value)
            else:
                value = value
        except Exception:
            logger.error("convert Failed: %s(value=%s, container=%s)" % (
                convert.__name__, value, container))
            raise
        return value

    def _prep_value(self, value, schema):
        value = self._unwrap(value)
        value = self._normalize_container(value, schema)
        value = self._convert(value, schema)
        value = self._typecast(value, schema)
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
        elif not isinstance(value, (list, tuple)):
            raise ValueError("expected list type, got: %s" % type(value))
        else:
            return sorted(self._type_single(item, _type) for item in value)

    def _type_single(self, value, _type):
        ' apply type to the single value '
        if is_null(value, except_=False):
            value = None
        elif _type in [None, NoneType]:
            # don't convert null values
            # default type is the original type if none set
            pass
        elif is_empty(value, except_=False):
            # fixme, rather leave as "empty" type? eg, list(), int(), etc.
            value = None
        elif isinstance(value, _type):  # or values already of correct type
            # normalize all dates to epochs
            value = dt2ts(value) if _type in [datetime, date] else value
        else:
            if _type in [datetime, date]:
                # normalize all dates to epochs
                value = dt2ts(value)
            elif _type in [unicode, str]:
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

    def update(self, obj):
        for key, value in obj.iteritems():
            key = self.__keytransform__(key)
            if key in self.IMMUTABLE_OBJ_KEYS:
                warnings.warn(
                    'attempted update of immutable key detected: %s' % key)
                continue
            elif key in self.TIMESTAMP_OBJ_KEYS:
                # ensure normalized timestamp
                value = dt2ts(value)
            elif key == '_e':  # _e is expected to be dict
                value = None if not value else dict(value)
                is_true(isinstance(value, (dict, MutableMapping)),
                        '_e must be dict, got %s' % type(value))
            else:
                schema = self._schema.get(key) or {}
                try:
                    value = self._prep_value(value, schema=schema)
                except Exception as e:
                    value = to_encoding(value)
                    self.store['_e'] = self.store['_e'] or {}
                    msg = 'prep(key=%s, value=%s) failed: %s' % (key, value, e)
                    logger.error(msg)
                    # set error field with original values
                    # set fallback value to None
                    self.store['_e'].update({key: value})
                    value = None
                self._add_variant(key, value, schema)
            self.store[key] = value
        self._re_hash()

    def _add_variant(self, key, value, schema):
        ''' also possible to define some function that takes
            current value and creates a new value from it
        '''
        variants = schema.get(key, {}).get('variants')
        if variants:
            for _key, func in variants.iteritems():
                _value = func(value)
            self.store[_key] = _value
        else:
            return


# FIXME: all objects should have the SAME keys;
# if an object is added with fewer keys, it should
# have the missing keys added with null values
class MetriqueContainer(MutableMapping):
    '''
    Essentially, cubes are data made from a an object id indexed (_oid)
    dictionary (keys) or dictionary "objects" (values)

    All objects are expected to contain a `_oid` key value property. This
    property should be unique per individual "object" defined.

    For example, if we are storing logs, we might consider each log line a
    separate "object" since those log lines should never change in the future
    and give each a unique `_oid`. Or if we are storing data about
    'meta objects' of some sort, say 'github repo issues' for example, we
    might have objects with _oids of
    `%(username)s_%(reponame)s_%(issuenumber)s`.

    Optionally, objects can contain the following additional meta-properties:
        * _start - datetime when the object state was set
        * _end - datetime when the object state changed to a new state

    Field names (object dict keys) must consist of alphanumeric and underscore
    characters only.

    Field names are partially normalized automatically:
        * non-alphanumeric characters are removed
        * spaces converted to underscores
        * letters are lowercased

    Property values are normalized to some extent automatically as well:
        * empty strings -> None

    '''
    _object_cls = None
    _proxy_cls = None
    _proxy = None
    config = None
    config_file = DEFAULT_CONFIG
    config_key = 'container'
    db = None
    default_fields = {'_start': 1, '_end': 1, '_oid': 1}
    name = None
    proxy_config_key = 'proxy'
    store = None
    version = 0
    HASH_EXCLUDE_KEYS = tuple(HASH_EXCLUDE_KEYS)
    RESTRICTED_KEYS = ('id', '_id', '_hash', '_start', '_end',
                       '_v', '__v__', '_e')

    def __init__(self, name=None, db=None, schema=None, version=None,
                 objects=None, proxy=None, proxy_config=None,
                 batch_size=None, config=None, config_file=None,
                 config_key=None, cache_dir=None, autotable=True,
                 **kwargs):
        '''
        Accept additional kwargs, but ignore them.
        '''
        # null name -> anonymous table; no native ability to persist
        options = dict(autotable=autotable,
                       cache_dir=cache_dir,
                       batch_size=batch_size,
                       default_fields=None,
                       name=None,
                       schema=schema,
                       version=int(version or 0))

        defaults = dict(autotable=True,
                        cache_dir=CACHE_DIR,
                        batch_size=999,
                        default_fields=MetriqueContainer.default_fields,
                        name=name,
                        schema={},
                        version=0)

        # if config is passed in, set it, otherwise start
        # with class assigned default or empty dict
        self.config = deepcopy(config or MetriqueContainer.config or {})
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
        proxy_config.setdefault('schema', self.schema)
        proxy_config.setdefault('config_file', self.config_file)
        self.config.setdefault(self.proxy_config_key, {}).update(proxy_config)

        if self._object_cls is None:
            self._object_cls = MetriqueObject

        if self._proxy_cls is None:
            from metrique.sqlalchemy import SQLAlchemyProxy
            self._proxy_cls = SQLAlchemyProxy
        self._proxy = proxy

        # init and update internal store with passed in objects, if any
        self.store = deepcopy(MetriqueContainer.store or {})
        self._update(objects)

    def _update(self, objects):
        if objects is None:
            pass
        elif isinstance(objects, (list, tuple)):
            [self.add(x) for x in objects]
        elif isinstance(objects, (dict, MutableMapping)):
            self.update(objects)
        elif isinstance(objects, MetriqueContainer):
            [self.add(x) for x in objects.values()]
        else:
            raise ValueError(
                "objs must be None, a list, tuple, dict or MetriqueContainer")

    def __getitem__(self, key):
        if isinstance(key, slice):
            keys = sorted(self.store.keys())[key]
            return [dict(self.store[i]) for i in keys]
        else:
            key = to_encoding(key)
            return dict(self.store[key])

    def __setitem__(self, key, value):
        o = self._object_cls(**value)
        self.store[o['_id']] = o

    def __delitem__(self, key):
        self.pop(key)
        return None

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __contains__(self, key):
        key = to_encoding(key)
        return key in self.store.keys()

    def __repr__(self):
        return repr(self.store)

    def _apply_default_fields(self, fields):
        if not fields:
            return {}
        else:
            for k, v in self.default_fields.iteritems():
                fields[k] = v if k not in fields else fields[k]
            return fields

    def _encode(self, obj):
        if isinstance(obj, self._object_cls):
            pass
        elif isinstance(obj, (Mapping)):
            if self.version > obj.get('_v', 0):
                obj['_v'] = self.version
            obj = self._object_cls(_schema=self.schema, **obj)
        else:
            raise TypeError(
                "object values must be dict-like; got %s" % type(obj))
        return obj

    @property
    def _exists(self):
        raise NotImplementedError("FIXME")

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

    def add(self, obj):
        obj = self._encode(obj)
        _id = obj['_id']
        self.store[_id] = obj

    def autotable(self):
        name = self.config.get('name')
        # if we have a table already, we want only to load
        # a corresponding sqla.Table instance so our ORM
        # works as expected; if no table and autotable:True,
        # create the table too.
        create = self.config.get('autotable')
        if name in self._proxy.meta_tables:
            logger.warn('autotable "%s": already exists' % name)
            result = True
        else:
            if self.schema:
                if name in self._proxy.db_tables:
                    # no reason to create the table again...
                    # but we still want to load the table class into metadata
                    create = False
                self._proxy.autotable(schema=self.schema, name=name,
                                      create=create)
                logger.warn('autotable "%s": (create=%s): OK' % (name, create))
                result = True
            else:
                logger.warn(
                    'autotable "%s": FAIL; no schema; store is empty' % name)
                result = False
        return result

    def clear(self):
        self.store = {}

    def df(self):
        '''Return a pandas dataframe from objects'''
        if not HAS_PANDAS:
            raise RuntimeError("`pip install pandas` required")
        return pd.DataFrame(self.store)

    def extend(self, objs):
        logger.debug('Extending container by %s objs...' % len(objs))
        s = time()
        [self.add(i) for i in objs]
        diff = time() - s
        logger.debug('... extended container by %s objs in %ss at %.2f/s' % (
            len(objs), int(diff), len(objs) / diff))

    def flush(self, objects=None, batch_size=None, **kwargs):
        objects = objects or self.values()
        batch_size = batch_size or self.config.get('batch_size')
        _ids = []
        # get store converted as table instances
        for batch in batch_gen(objects, batch_size):
            _ = self.persist(objects=batch, **kwargs)
            _ids.extend(_)
        keys = self._ids
        [self.store.pop(_id) for _id in _ids if _id in keys]
        return sorted(_ids)

    def find(self, query=None, fields=None, date=None, sort=None,
             descending=False, one=False, raw=False, limit=None,
             as_cursor=False, scalar=False):
        fields = self._apply_default_fields(fields)
        return self.proxy.find(table=self.name, query=query, fields=fields,
                               date=date, sort=sort, descending=descending,
                               one=one, raw=raw, limit=limit,
                               as_cursor=as_cursor, scalar=scalar)

    def filter(self, where):
        if not isinstance(where, (dict, MutableMapping)):
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
        raise NotImplementedError("Subclasses should implement this.")

    def persist(self, objects=None, autosnap=None):
        objects = objects or self
        return self.upsert(objects=objects, autosnap=autosnap)

    def pop(self, key):
        key = to_encoding(key)
        return self.store.pop(key)

    @property
    def proxy(self):
        if self._proxy is None or isclass(self._proxy):
            self.proxy_init()
        self.autotable()
        return self._proxy

    @property
    def proxy_config(self):
        self.config.setdefault(self.proxy_config_key, {})
        return self.config[self.proxy_config_key]

    def proxy_init(self):
        is_true(self.name, "name can not be null!")
        if self._proxy is None:
            self._proxy = self._proxy_cls
        # else: _proxy is a proxy_cls
        self._proxy = self._proxy(**self.proxy_config)

    def objects(self):
        return self.store.values()

    def values(self):
        return [dict(v) for v in self.store.itervalues()]

    @property
    def schema(self):
        schema = self.config.get('schema')
        if not schema and self.store:
            # if we didn't get an expclit schema definition to use,
            # autogenerate the schema from the store content, if any
            values = self.store.values()
            if hasattr(self._proxy, 'autoschema'):
                schema = self._proxy.autoschema(values)
            else:
                schema = autoschema(values,
                                    exclude_keys=self.RESTRICTED_KEYS)
            self.config['schema'] = schema
        return schema

    def count(self, query=None, date=None):
        '''
        Run a pql mongodb based query on the given cube and return only
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
        fields = parse.parse_fields(fields)
        query = self._parse_query(query=query, date=date, fields=fields,
                                  alias='anon_x', distinct=True)
        ret = [r[0] for r in self.proxy.session_auto.execute(query)]
        if ret and isinstance(ret[0], list):
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
        :param background: MongoDB should create in the background
        :param collection: cube name
        :param owner: username of cube owner
        '''
        return self.proxy.index(self.name, fields=fields, name=name, **kwargs)

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

    def upsert(self, objects, autosnap=None):
        return self.proxy.upsert(table=self.name, objects=objects,
                                 autosnap=autosnap)

    def user_register(self, username=None, password=None):
        password = password or self.config.get('password')
        username = username or self.config.get('username')
        return self.proxy.user_register(table=self.name, username=username,
                                        password=password)

    def user_disable(self, username):
        return self.proxy.user_disable(table=self.name, username=username)
