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
**Python/MongoDB data warehouse and information platform**

metrique is used to bring data from any number of arbitrary
sources into unified data collections that supports
transparent historical version snapshotting, advanced
ad-hoc server-side querying, including (mongodb)
aggregations and (mongodb) mapreduce, along with client
side querying and analysis with the support of an array
of scientific computing python libraries, such as ipython,
pandas, numpy, matplotlib, and more.

The main client interface is `metrique.pyclient`

A simple example of how one might interact with metrique is
demonstrated below. In short, we import one of the many
pre-defined metrique cubes -- `osinfo_rpm` -- in this case.
Then get all the objects which that cube is built to extract --
a full list of installed RPMs on the current host system. Followed
up by persisting those objects to an external `metriqued` host.
And finishing with some querying and simple charting of the data.

    >>> from metrique import pyclient
    >>> g = pyclient(cube="osinfo_rpm")
    >>> g.get_objects()  # get information about all installed RPMs
    >>> 'Total RPMs: %s' % len(g.objects)
    >>> 'Example Object:', g.objects[0]
        {'_oid': 'dhcp129-66.brq.redhat.com__libreoffice-ure-4.1.4.2[...]',
         '_start': 1390619596.0,
         'arch': 'x86_64',
         'host': 'bla.host.com',
         'license': '(MPLv1.1 or LGPLv3+) and LGPLv3 and LGPLv2+ and[...]',
         'name': 'libreoffice-ure',
         'nvra': 'libreoffice-ure-4.1.4.2-2.fc20.x86_64',
         'os': 'linux',
         'packager': 'Fedora Project',
         'platform': 'x86_64-redhat-linux-gnu',
         'release': '2.fc20',
         'sourcepackage': None,
         'sourcerpm': 'libreoffice-4.1.4.2-2.fc20.src.rpm',
         'summary': 'UNO Runtime Environment',
         'version': '4.1.4.2'
    }
    >>> _ids = osinfo_rpm.get_objects(flush=True)  # persist to mongodb
    >>> df = osinfo_rpm.find(fields='license')
    >>> threshold = 5
    >>> license_k = df.groupby('license').apply(len)
    >>> license_k.sort()
    >>> sub = license_k[license_k >= threshold]
    >>> # shorten the names a bit
    >>> sub.index = [i[0:20] + '...' if len(i) > 20 else i for i in sub.index]
    >>> sub.plot(kind='bar')
    ... <matplotlib.axes.AxesSubplot at 0x6f77ad0>

.. note::
    example date ranges: 'd', '~d', 'd~', 'd~d'
.. note::
    valid date format: '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
'''

from __future__ import unicode_literals

import logging
logger = logging.getLogger('metrique')

from collections import MutableMapping
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from getpass import getuser
from inspect import isclass
try:
    from lockfile import LockFile
    HAS_LOCKFILE = True
except ImportError:
    HAS_LOCKFILE = False
import os
from operator import itemgetter, add

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    logger.warn('pandas module is not installed!')

try:
    import pql
    HAS_PQL = True
except ImportError:
    HAS_PQL = False
    logger.warn('pql module is not installed!')

try:
    from pymongo import MongoClient, MongoReplicaSetClient
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

try:
    import psycopg2
    psycopg2  # avoid pep8 'imported, not used' lint error
    HAS_PSYCOPG2 = True
except ImportError:
    logger.warn('psycopg2 not installed!')
    HAS_PSYCOPG2 = False

import re
import random

try:
    import simplejson as json
except ImportError:
    import json

try:
    from sqlalchemy import create_engine, MetaData, Table
    from sqlalchemy import Index, Column, Integer, DateTime
    from sqlalchemy import Float, BigInteger, Boolean, UnicodeText
    from sqlalchemy import TypeDecorator
    from sqlalchemy import select, insert, update, desc
    from sqlalchemy import inspect
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.sql.expression import func

    import sqlalchemy.dialects.postgresql as pg
    from sqlalchemy.dialects.postgresql import ARRAY, JSON

    # for 2.7 to ensure all strings are unicode
    class CoerceUTF8(TypeDecorator):
        """Safely coerce Python bytestrings to Unicode
        before passing off to the database."""

        impl = UnicodeText

        def process_bind_param(self, value, dialect):
            if isinstance(value, unicode):
                pass
            else:
                value = str(value).decode('utf-8')
            return value

        def python_type(self):
            return unicode

    class JSONTyped(TypeDecorator):
        impl = JSON

        def python_type(self):
            # FIXME: should this be dict? or rather json
            # and parser would check for json type?
            return dict

    class JSONTypedLite(TypeDecorator):
        impl = UnicodeText

        def process_bind_param(self, value, dialect):
            if value:
                if isinstance(value, (list, tuple, set)):
                    value = list(value)
                return to_encoding(json.dumps(value))
            else:
                return None

        def process_result_value(self, value, dialect):
            if value:
                return json.loads(value)
            else:
                return {}

        def python_type(self):
            return unicode

    HAS_SQLALCHEMY = True
    TYPE_MAP = {
        None: CoerceUTF8,
        type(None): CoerceUTF8,
        int: Integer,
        float: Float,
        long: BigInteger,
        str: CoerceUTF8,
        unicode: CoerceUTF8,
        bool: Boolean,
        datetime: DateTime,
        list: JSONTypedLite, tuple: JSONTypedLite, set: JSONTypedLite,
        dict: JSONTypedLite, MutableMapping: JSONTypedLite,
    }

    sqla_metadata = MetaData()
    sqla_Base = declarative_base(metadata=sqla_metadata)

except ImportError:
    logger.warn('sqlalchemy not installed!')
    HAS_SQLALCHEMY = False
    TYPE_MAP = {}
    CoerceUTF8 = None
finally:
    RESERVED_WORDS = {'end'}

from time import time

from metrique import __version__
from metrique.utils import get_cube, utcnow, jsonhash, load_config, load
from metrique.utils import batch_gen, ts2dt, dt2ts, configure, to_encoding
from metrique.utils import debug_setup, is_null, is_true, str2list
from metrique import parse
from metrique.result import Result

# if HOME environment variable is set, use that
# useful when running 'as user' with root (supervisord)
ETC_DIR = os.environ.get('METRIQUE_ETC')
CACHE_DIR = os.environ.get('METRIQUE_CACHE') or '/tmp'
DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metrique.json')

HASH_EXCLUDE_KEYS = ('_hash', '_id', '_start', '_end', '__v__', 'id')


# FIXME: default _as_datetime -> False (epoch is default)
class MetriqueObject(MutableMapping):
    FIELDS_RE = re.compile('[\W]+')
    SPACE_RE = re.compile('\s+')
    UNDA_RE = re.compile('_+')
    IMMUTABLE_OBJ_KEYS = set(['_hash', '_id', '_oid', 'id'])
    TIMESTAMP_OBJ_KEYS = set(['_end', '_start'])
    _VERSION = 0

    def __init__(self, _oid, _id=None, _hash=None, _start=None, _end=None,
                 _e=None, _v=None, _as_datetime=True, **kwargs):
        self._as_datetime = _as_datetime
        if _oid is None:
            raise RuntimeError("_oid can not be None!")
        _start = _start or utcnow(as_datetime=_as_datetime)
        _start = ts2dt(_start) if _as_datetime else dt2ts(_start)
        _e = _e if _e is not None else {}
        self.store = {
            '_oid': _oid,
            '_id': None,  # ignore passed in _id
            '_hash': None,  # ignore passed in _hash
            '_start': _start,
            '_end': _end or None,
            '_v': _v or MetriqueObject._VERSION,
            '__v__': __version__,
            '_e': _e,
        }
        self.update(kwargs)
        self._re_hash()

    def as_dict(self, pop=None):
        store = deepcopy(self.store)
        if pop:
            [store.pop(key, None) for key in pop]
        return store

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
        [o.pop(k) for k in HASH_EXCLUDE_KEYS if k in keys]
        return jsonhash(o)

    def _re_hash(self):
        # FIXME: validate all meta fields; make sure typed
        # correctly?
        self._validate_start_end()
        # _id depends on _hash
        # so first, _hash, then _id
        self.store['_hash'] = self._gen_hash()
        self.store['_id'] = self._gen_id()

    def update(self, obj):
        for key, value in obj.iteritems():
            key = self.__keytransform__(key)
            if key in self.IMMUTABLE_OBJ_KEYS:
                key = '__%s' % key  # don't overwrite, but archive
            elif key in self.TIMESTAMP_OBJ_KEYS:
                # ensure normalized timestamp
                value = ts2dt(value) if self._as_datetime else dt2ts(value)
            elif key == '_e':  # _e is expected to be dict
                value = {} if value is None else dict(value)
                is_true(isinstance(value, (dict, MutableMapping)),
                        '_e must be dict, got %s' % type(value))
            else:
                pass

            if isinstance(value, str):
                value = unicode(value, 'utf8')
            elif is_null(value, except_=False):
                # Normalize empty strings and NaN/NaT objects to None
                # NaN objects do not equal themselves...
                value = None
            else:
                pass
            self.store[key] = value
        self._re_hash()

    def _validate_start_end(self):
        _start = self.get('_start')
        if _start is None:
            raise ValueError("_start (%s) must be set!" % _start)
        _end = self.get('_end')
        _start = ts2dt(_start)
        _end = ts2dt(_end)
        if _end and _end < _start:
            raise ValueError(
                "_end (%s) is before _start (%s)!" % (_end, _start))

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
    INVALID_USERNAME_RE = re.compile('[^a-zA-Z_]')
    RESTRICTED_NAMES = []
    _object_cls = MetriqueObject
    _proxy_kwargs = None
    _table = None
    _version = 0
    config = None
    config_file = DEFAULT_CONFIG
    config_key = 'metrique'
    default_fields = None
    name = None
    store = None

    def __init__(self, name, _version=0, objects=None,
                 cache_dir=CACHE_DIR, proxy=None, proxy_kwargs=None,
                 batch_size=999, config=None, config_key=None):
        is_true(name, "name argument must be non-null")
        self.name = unicode(name)
        self._version = int(_version or self._version or 0)
        self.default_fields = deepcopy(self.default_fields) or {}
        self.store = self.store or {}
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

        options = dict(cache_dir=cache_dir,
                       batch_size=batch_size)

        defaults = dict(cache_dir=CACHE_DIR,
                        batch_size=999)

        # if config is passed in, set it, otherwise start
        # with class assigned default or empty dict
        self.config = deepcopy(config) or self.config or {}
        self.config_key = config_key or MetriqueContainer.config_key
        # load defaults + set args passed in
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                section_only=True,
                                update=self.config)

        cache_dir = self.config.get('cache_dir')
        suffix = '.sqlite'
        fname = '%s%s' % (self.name, suffix)
        persist_path = os.path.join(cache_dir, fname)
        self._persist_path = persist_path

        self._proxy_kwargs = deepcopy(proxy_kwargs or {})
        if not proxy:
            proxy = SQLAlchemyProxy(db=self.name)
        self.set_proxy(proxy, quiet=True)

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

    def _encode(self, item):
        if isinstance(item, self._object_cls):
            pass
        elif isinstance(item, (MutableMapping, dict)):
            if self._version > item.get('_v', 0):
                item['_v'] = self._version
            item = self._object_cls(**item)
        else:
            raise TypeError(
                "object values must be dict-like; got %s" % type(item))
        return item

    def _apply_default_fields(self, fields):
        for k, v in self.default_fields.iteritems():
            fields[k] = v if not k in fields else fields[k]

    def add(self, item):
        item = self._encode(item)
        _id = item['_id']
        self.store[_id] = item

    def clear(self):
        self.store = {}

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
        table = self.proxy.get_table(self.name)
        return self.proxy.count(table=table, query=query, date=date)

    def extend(self, items):
        [self.add(i) for i in items]

    def df(self):
        '''Return a pandas dataframe from objects'''
        if not HAS_PANDAS:
            raise RuntimeError("`pip install pandas` required")
        return pd.DataFrame(self.store)

    @property
    def _exists(self):
        raise NotImplementedError("FIXME")

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

    def ls(self):
        raise NotImplementedError("Subclasses should implement this.")

    @property
    def _ids(self):
        return sorted(self.store.keys())

    @property
    def _oids(self):
        return sorted({o['_oid'] for o in self.store.itervalues()})

    def persist(self, objects=None, autosnap=True):
        objects = objects or self.values()
        name = self.name
        proxy = self.proxy
        schema = proxy.autoschema(objects)
        with LockFile(self._persist_path):
            if self._table is None:
                table = proxy.ensure_table(name, schema)
            else:
                table = self._table
            return proxy.upsert(table=table, objects=objects,
                                autosnap=autosnap)

    def pop(self, key):
        key = to_encoding(key)
        return self.store.pop(key)

    @property
    def proxy(self):
        _proxy = getattr(self, '_proxy', None)
        if not _proxy:
            config_key = self.config_key
            config_file = self.config_file
            self._proxy = SQLAlchemyProxy(config_key=config_key,
                                          config_file=config_file,
                                          **self.config)
        return self._proxy

    def set_proxy(self, proxy=None, quiet=False, **kwargs):
        _kwargs = deepcopy(self._proxy_kwargs)
        _kwargs.update(kwargs)
        _kwargs.setdefault('config_file', self.config_file)
        if proxy is not None:
            if isclass(proxy):
                self._proxy = proxy(**_kwargs)
            else:
                self._proxy = proxy
        elif quiet:
            pass
        else:
            raise RuntimeError("proxy can not be null!")
        return None

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
        elif self.INVALID_USERNAME_RE.search(username):
            raise ValueError(
                "Invalid username '%s'; "
                "lowercase, ascii alpha [a-z_] characters only!" % username)
        elif username in self.RESTRICTED_NAMES:
            raise ValueError(
                "username '%s' is not permitted" % username)
        else:
            return username.lower()

    def _validate_roles(self, roles):
        if isinstance(roles, basestring):
            roles = [roles]
        else:
            roles = list(roles)
        roles = set(map(str, roles))
        if not roles <= set(self.VALID_SHARE_ROLES):
            raise ValueError("invalid roles %s, try: %s" % (
                roles, self.VALID_SHARE_ROLES))
        return sorted(roles)

    def values(self):
        return self.store.values()


class MetriqueFactory(type):
    def __call__(cls, cube=None, name=None, backends=None, *args, **kwargs):
        name = name or cube
        if cube:
            cls = get_cube(cube=cube, name=name, init=False, backends=backends)
        _type = type.__call__(cls, name=name, *args, **kwargs)
        return _type


class BaseClient(object):
    '''
    Low level client API which provides baseline functionality, including
    methods for loading data from csv and json, loading metrique client
    cubes, config file loading and logging setup.

    Additionally, some common operation methods are provided for
    operations such as loading a HTTP uri and determining currently
    configured username.

    :cvar name: name of the cube
    :cvar config: local cube config object

    If cube is specified as a kwarg upon initialization, the specific cube
    class will be located and returned, assuming its available in sys.path.

    If the cube fails to import, RuntimeError will be raised.

    Example usage::

        >>> import pyclient
        >>> c = pyclient(cube='git_commit')
            <type HTTPClient(...)>

        >>> z = pyclient()
        >>> z.get_cube(cube='git_commit')
            <type HTTPClient(...)>

    '''
    config_file = DEFAULT_CONFIG
    config_key = None
    global_config_key = 'metrique'
    mongodb_config_key = 'mongodb'
    sqlalchemy_config_key = 'sqlalchemy'
    name = None
    _version = None
    config = None
    _objects = None
    _proxy = None
    __metaclass__ = MetriqueFactory

    def __init__(self, name=None, config_file=None, config=None,
                 config_key=None, cube_pkgs=None, cube_paths=None,
                 debug=None, log_file=None, log2file=None, log2stdout=None,
                 workers=None, log_dir=None, cache_dir=None, etc_dir=None,
                 tmp_dir=None, container=None, container_kwargs=None,
                 proxy=None, proxy_kwargs=None):
        '''
        :param cube_pkgs: list of package names where to search for cubes
        :param cube_paths: Additional paths to search for client cubes
        :param debug: turn on debug mode logging
        :param log_file: filename for logs
        :param log2file: boolean - log output to file?
        :param logstout: boolean - log output to stdout?
        :param workers: number of workers for threaded operations
        '''
        super(BaseClient, self).__init__()

        # default cube name is username unless otherwise provided
        self.name = name or self.name or getuser()

        # cube class defined name
        self._cube = type(self).name

        # set default config value as dict (from None set cls level)
        self.config_file = config_file or self.config_file

        options = dict(cache_dir=cache_dir,
                       cube_pkgs=cube_pkgs,
                       cube_paths=cube_paths,
                       debug=debug,
                       etc_dir=etc_dir,
                       log_dir=log_dir,
                       log_file=log_file,
                       log2file=log2file,
                       log2stdout=log2stdout,
                       tmp_dir=tmp_dir,
                       workers=workers)

        defaults = dict(cache_dir=CACHE_DIR,
                        cube_pkgs=['cubes'],
                        cube_paths=[],
                        debug=None,
                        etc_dir=os.environ.get('METRIQUE_ETC', ''),
                        log_file='metrique.log',
                        log_dir=os.environ.get('METRIQUE_LOGS', ''),
                        log2file=True,
                        log2stdout=False,
                        tmp_dir=os.environ.get('METRIQUE_TMP', ''),
                        workers=2)

        # if config is passed in, set it, otherwise start
        # with class assigned default or empty dict
        self.config = deepcopy(config) or self.config or {}
        self.config_key = config_key or self.global_config_key
        # load defaults + set args passed in
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                update=self.config)

        level = self.gconfig.get('debug')
        log2stdout = self.gconfig.get('log2stdout')
        log_format = None
        log2file = self.gconfig.get('log2file')
        log_dir = self.gconfig.get('log_dir', '')
        log_file = self.gconfig.get('log_file', '')
        log_file = os.path.join(log_dir, log_file)
        debug_setup(logger='metrique', level=level, log2stdout=log2stdout,
                    log_format=log_format, log2file=log2file,
                    log_dir=log_dir, log_file=log_file)
        self._container_kwargs = deepcopy(container_kwargs or {})
        self.set_container(container)
        # FIXME: set default proxy to container's proxy if set?
        self._proxy_kwargs = deepcopy(proxy_kwargs or {})
        self.set_proxy(proxy, quiet=True)

    # 'container' is alias for 'objects'
    @property
    def container(self):
        return self._objects

    @container.setter
    def container(self, value):
        self.set_container(value=value)

    @container.deleter
    def container(self):
        # replacing existing container with a new, empty one
        self._objects = self.set_container()

    @property
    def objects(self):
        return self._objects

    @objects.setter
    def objects(self, value):
        self.set_container(value=value)

    @objects.deleter
    def objects(self):
        # replacing existing container with a new, empty one
        self._objects = self.set_container()

    def get_cube(self, cube, init=True, name=None, copy_config=True,
                 container=None, container_kwargs=None,
                 proxy=None, proxy_kwargs=None, config_file=None,
                 **kwargs):
        '''wrapper for :func:`metrique.utils.get_cube`

        Locates and loads a metrique cube

        :param cube: name of cube to load
        :param init: (bool) initialize cube before returning?
        :param name: override the name of the cube
        :param copy_config: apply config of calling cube to new?
                            Implies init=True.
        :param kwargs: additional :func:`metrique.utils.get_cube`
        '''
        name = name or cube
        config = deepcopy(self.config) if copy_config else {}
        config_file = config_file or self.config_file
        container = container or type(self.objects)
        container_kwargs = container_kwargs or self._container_kwargs
        proxy_kwargs = proxy_kwargs or self._proxy_kwargs
        return get_cube(cube=cube, init=init, name=name, config=config,
                        config_file=config_file,
                        container=container, container_kwargs=container_kwargs,
                        proxy=proxy, proxy_kwargs=proxy_kwargs, **kwargs)

    def set_container(self, container=None, value=None, _version=None,
                      **kwargs):
        _version = _version or getattr(self, '_version', 0)
        _kwargs = deepcopy(self._container_kwargs)
        _kwargs.update(kwargs)
        _kwargs.setdefault('config_file', self.config_file)
        if container:
            if isinstance(container, basestring):
                # load the container from globals()
                _container = globals().get(container)
                if container:
                    container = _container
                else:
                    raise RuntimeError(
                        "Invalid container class: %s" % container)

            if isclass(container):
                # FIXME: check it's specifically a MetriqueContainer class...
                self._objects = container(name=self.name, _version=_version,
                                          **_kwargs)
            elif isinstance(container, MetriqueContainer):
                self._objects = container
            else:
                raise RuntimeError("Invalid container class: %s" % container)

        else:
            cache_dir = self.gconfig.get('cache_dir')
            name = self.name
            self._objects = MetriqueContainer(name=name, objects=value,
                                              _version=_version,
                                              cache_dir=cache_dir)
        return self._objects

    def set_proxy(self, proxy=None, quiet=False, **kwargs):
        _kwargs = deepcopy(self._proxy_kwargs)
        _kwargs.update(kwargs)
        _kwargs.setdefault('config_file', self.config_file)
        if proxy is not None:
            if isclass(proxy):
                self._proxy = proxy(**_kwargs)
            else:
                self._proxy = proxy
        elif quiet:
            pass
        else:
            raise RuntimeError("proxy can not be null!")
        return None

    @property
    def proxy(self):
        # FIXME: return back container proxy if local proxy not set?
        return self._proxy

    @property
    def gconfig(self):
        return self.config.get(self.global_config_key) or {}

    def get_objects(self, flush=False, autosnap=True, **kwargs):
        '''
        Main API method for sub-classed cubes to override for the
        generation of the objects which are to (potentially) be added
        to the cube (assuming no duplicates)
        '''
        if flush:
            return self.objects.flush(autosnap=autosnap, **kwargs)
        return self

    @property
    def lconfig(self):
        return self.config.get(self.config_key) or {}

    def load_config(self, path):
        return load_config(path)

    # ############################## Backends #################################
    def mongodb(self, cached=True, owner=None, name=None,
                config_file=None, config_key=None, **kwargs):
        # return cached unless kwargs are set, cached is False
        # or there isn't already an instance cached available
        _mongodb = getattr(self, '_mongodb', None)
        config_file = config_file or self.config_file
        config_key = config_key or self.mongodb_config_key
        config = configure(options=kwargs,
                           config_file=config_file,
                           section_key=config_key,
                           section_only=True)
        if kwargs or not (_mongodb and cached):
            owner = owner or getuser()
            name = name or self.name
            self._mongodb = MongoDBProxy(owner=owner, collection=name,
                                         config_file=config_file,
                                         **config)
            self.set_proxy(self._mongodb)
        return self._mongodb

    def sqlalchemy(self, cached=True, owner=None, table=None,
                   config_file=None, config_key=None, **kwargs):
        # return cached unless kwargs are set, cached is False
        # or there isn't already an instance cached available
        _sqlalchemy = getattr(self, '_sqlalchemy', None)
        config_file = config_file or self.config_file
        config_key = config_key or self.sqlalchemy_config_key
        config = configure(options=kwargs,
                           config_file=config_file,
                           section_key=config_key,
                           section_only=True)
        if kwargs or not (_sqlalchemy and cached):
            owner = owner or getuser()
            table = table or self.name
            self._sqlalchemy = SQLAlchemyProxy(owner=owner, table=table,
                                               config_file=config_file,
                                               **config)
            self.set_proxy(self._sqlalchemy)
        return self._sqlalchemy


# ################################ MONGODB ###################################
class MongoDBProxy(object):
    '''
        :param auth: Enable authentication
        :param batch_size: The number of objs save at a time
        :param password: mongodb password
        :param username: mongodb username
        :param host: mongodb host(s) to connect to
        :param owner: mongodb owner user database cube is in
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

    SSL_PEM = os.path.join(ETC_DIR, 'metrique.pem')

    def __init__(self, host=None, port=None, username=None, password=None,
                 auth=None, ssl=None, ssl_certificate=None,
                 index_ensure_secs=None, read_preference=None,
                 replica_set=None, tz_aware=None, write_concern=None,
                 owner=None, collection=None,
                 config_file=None, config_key=None, **kwargs):
        if not HAS_PYMONGO:
            raise RuntimeError('`pip install pymongo` 2.6+ required')

        options = dict(auth=auth,
                       collection=collection,
                       host=host,
                       owner=owner,
                       password=password,
                       port=port,
                       read_preference=read_preference,
                       replica_set=replica_set,
                       ssl=ssl,
                       ssl_certificate=ssl_certificate,
                       tz_aware=tz_aware,
                       username=username,
                       write_concern=write_concern,
                       )
        defaults = dict(auth=False,
                        collection=None,
                        host='127.0.0.1',
                        owner=None,
                        password='',
                        port=27017,
                        read_preference='NEAREST',
                        replica_set=None,
                        ssl=False,
                        ssl_certificate=self.SSL_PEM,
                        tz_aware=True,
                        username=getuser(),
                        write_concern=0,
                        )
        self.config = self.config or {}
        self.config_file = config_file or self.config_file
        self.config_key = config_key or MongoDBProxy.config_key
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                section_only=True,
                                update=self.config)

        # default owner == username if not set
        self.config['owner'] = self.config['owner'] or defaults.get('username')

    def __getitem__(self, query):
        return self.find(query=query, fields=self.default_fields,
                         date='~', merge_versions=False,
                         sort=self.default_sort)

    @property
    def proxy(self):
        _proxy = getattr(self, '_proxy', None)
        if not _proxy:
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

    def get_db(self, owner=None):
        owner = owner or self.config.get('owner')
        if not owner:
            raise RuntimeError("[%s] Invalid db!" % owner)
        try:
            return self.proxy[owner]
        except OperationFailure as e:
            raise RuntimeError("unable to get db! (%s)" % e)

    def get_collection(self, owner=None, name=None):
        name = name or self.config.get('collection')
        if not name:
            raise RuntimeError("collection name can not be null!")
        _cube = self.get_db(owner)[name]
        return _cube

    def set_collection(self, name):
        if not name:
            raise RuntimeError("collection name can not be null!")
        self.config['name'] = name

    def __repr__(self):
        owner = self.config.get('owner')
        name = self.config.get('collection')
        return '%s(owner="%s", name="%s">)' % (
            self.__class__.__name__, owner, name)

    # ######################## User API ################################


class MongoDBContainer(MetriqueContainer):
    _objects = None
    config = None
    config_key = 'mongodb'
    config_file = DEFAULT_CONFIG
    default_fields = {'_start': 1, '_end': 1, '_oid': 1}
    default_sort = [('_start', -1)]
    owner = None
    name = None
    INDEX_ENSURE_SECS = 60 * 60
    RESTRICTED_NAMES = ['admin', 'local', 'system']
    VALID_SHARE_ROLES = ['read', 'readWrite', 'dbAdmin', 'userAdmin']
    CUBE_OWNER_ROLES = ['readWrite', 'dbAdmin', 'userAdmin']

    def __init__(self, name, objects=None, proxy=None, batch_size=None,
                 host=None, port=None, username=None, password=None,
                 auth=None, ssl=None, ssl_certificate=None,
                 index_ensure_secs=None, read_preference=None,
                 replica_set=None, tz_aware=None, write_concern=None,
                 owner=None, config_file=None, config_key=None,
                 _version=None, **kwargs):
        if not HAS_PYMONGO:
            raise RuntimeError('`pip install pymongo` 2.6+ required')

        super(MongoDBContainer, self).__init__(name=name,
                                               objects=objects,
                                               _version=_version)

        options = dict(auth=auth,
                       batch_size=batch_size,
                       host=host,
                       index_ensure_secs=index_ensure_secs,
                       name=name,
                       owner=owner,
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
                        index_ensure_secs=self.INDEX_ENSURE_SECS,
                        name=self.name,
                        owner=None,
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
        self.config['owner'] = self.config['owner'] or defaults.get('username')

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

    def flush(self, autosnap=True, batch_size=None, fast=True,
              name=None, owner=None, **kwargs):
        '''
        Persist a list of objects to MongoDB.

        Returns back a list of object ids saved.

        :param objects: list of dictionary-like objects to be stored
        :param name: cube name
        :param owner: username of cube owner
        :param start: ISO format datetime to apply as _start
                      per object, serverside
        :param autosnap: rotate _end:None's before saving new objects
        :returns result: _ids saved
        '''
        self._ensure_base_indexes()

        batch_size = batch_size or self.config.get('batch_size')
        _cube = self.proxy.get_collection(owner, name)
        _ids = []
        for batch in batch_gen(self.store.values(), batch_size):
            _ = self._flush(_cube=_cube, objects=batch, autosnap=autosnap,
                            fast=fast, name=name, owner=owner)
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
               name=None, owner=None):
        olen = len(objects)
        if olen == 0:
            logger.debug("No objects to flush!")
            return []
        logger.info("[%s] Flushing %s objects" % (_cube, olen))

        objects, dup_ids = self._filter_dups(_cube, objects)
        # remove dups from instance object container
        [self.store.pop(_id) for _id in set(dup_ids)]

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
            # aren't in self.store, so ignore them
        # FIXME: this is redundant if we call super().flush(), which
        # we should
        [self.store.pop(_id) for _id in _ids if _id in self.store]
        logger.debug(
            "[%s] %s objects remaining" % (_cube, len(self.store)))
        # FIXME: this is redundant? ... if we call super().flush(), which we
        # should...
        return _ids

    def _filter_end_null_dups(self, _cube, objects):
        # filter out dups which have null _end value
        _hashes = [o['_hash'] for o in objects if o['_end'] is None]
        if _hashes:
            spec = self.proxy.parse_query('_hash in %s' % _hashes, date=None)
            return set(_cube.find(spec).distinct('_id'))
        else:
            return set()

    def _filter_end_not_null_dups(self, _cube, objects):
        # filter out dups which have non-null _end value
        tups = [(o['_id'], o['_hash']) for o in objects
                if o['_end'] is not None]
        _ids, _hashes = zip(*tups) if tups else [], []
        if _ids:
            spec = self.proxy.parse_query(
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

        spec = self.proxy.parse_query('_id in %s' % _ids, date=None)
        _objs = _cube.find(spec, {'_hash': 0})
        k = _objs.count()
        logger.debug(
            '[%s] %s of %s objects need to be rotated' % (_cube, k, olen))
        if k == 0:  # nothing to rotate...
            return objects
        for o in _objs:
            # _end of existing obj where _end:None should get new's _start
            # look this up in the instance objects mapping
            _start = self.store[o['_id']]['_start']
            o['_end'] = _start
            o = self._object_cls(_v=self._version, **o)
            objects.append(o)
        return objects

    def _ensure_base_indexes(self, ensure_time=90):
        _cube = self.proxy.get_collection()
        s = ensure_time
        ensure_time = int(s) if s and s != 0 else 90
        _cube.ensure_index('_oid', background=False, cache_for=s)
        _cube.ensure_index('_hash', background=False, cache_for=s)
        _cube.ensure_index([('_start', -1), ('_end', -1)],
                           background=False, cache_for=s)
        _cube.ensure_index([('_end', -1)],
                           background=False, cache_for=s)

    @property
    def _exists(self):
        return self.name in self.proxy.ls()

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

    def keys(self, sample_size=1):
        return self.sample_fields(sample_size=sample_size)

    def values(self, sample_size=1):
        return self.sample_docs(sample_size=sample_size)

    # FIXME: add 'user_update_roles()...
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
        username = self._validate_username(username)
        password = self._validate_password(password)
        logger.info('Registering new user %s' % username)
        db = self.proxy.get_db(username)
        db.add_user(username, password,
                    roles=self.CUBE_OWNER_ROLES)
        spec = self.parse_query('user == "%s"' % username)
        result = db.system.users.find(spec).count()
        return bool(result)

    def user_remove(self, username, clear_db=False):
        username = username or self.config.get('username')
        username = self._validate_username(username)
        logger.info('Removing user %s' % username)
        db = self.proxy.get_db(username)
        db.remove_user(username)
        if clear_db:
            db.drop_database(username)
        spec = self.parse_query('user == "%s"' % username)
        result = not bool(db.system.users.find(spec).count())
        return result

    # ######################## Cube API ################################
    def ls(self, startswith=None, owner=None):
        '''
        List all cubes available to the calling client.

        :param startswith: string to use in a simple "startswith" query filter
        :returns list: sorted list of cube names
        '''
        # FIXME: move this to Proxy?
        # FIXME: this only shows cubes in users db,
        #        get all dbs and collections in each db

        db = self.proxy.get_db(owner)
        cubes = db.collection_names(include_system_collections=False)
        startswith = unicode(startswith or '')
        cubes = [name for name in cubes if name.startswith(startswith)]
        RCN = self.RESTRICTED_NAMES
        not_restricted = lambda c: all(not c.startswith(name) for name in RCN)
        cubes = filter(not_restricted, cubes)
        logger.info('[%s] Listing available cubes starting with "%s")' % (
            owner, startswith))
        return sorted(cubes)

    def share(self, with_user, roles=None, owner=None):
        '''
        Give cube access rights to another user
        '''
        with_user = self._validate_username(with_user)
        roles = self._validate_roles(roles or ['read'])
        _cube = self.proxy.get_db(owner)
        logger.info(
            '[%s] Sharing cube with %s (%s)' % (_cube, with_user, roles))
        result = _cube.add_user(name=with_user, roles=roles,
                                userSource=with_user)
        return result

    def drop(self, collection=None, owner=None):
        '''
        Drop (delete) cube.

        :param quiet: ignore exceptions
        :param owner: username of cube owner
        :param collection: cube name
        '''
        _cube = self.proxy.get_collection(owner, collection)
        logger.info('[%s] Dropping cube' % _cube)
        name = _cube.name
        _cube.drop()
        db = self.proxy.get_db(owner)
        result = not bool(name in db.collection_names())
        return result

    def index_list(self, collection=None, owner=None):
        '''
        List all cube indexes

        :param collection: cube name
        :param owner: username of cube owner
        '''
        _cube = self.proxy.get_collection(owner, collection)
        logger.info('[%s] Listing indexes' % _cube)
        result = _cube.index_information()
        return result

    def index(self, key_or_list, name=None, collection=None, owner=None,
              **kwargs):
        '''
        Build a new index on a cube.

        Examples:
            + ensure_index('field_name')
            + ensure_index([('field_name', 1), ('other_field_name', -1)])

        :param key_or_list: A single field or a list of (key, direction) pairs
        :param name: (optional) Custom name to use for this index
        :param background: MongoDB should create in the background
        :param collection: cube name
        :param owner: username of cube owner
        '''
        _cube = self.proxy.get_collection(owner, collection)
        logger.info('[%s] Writing new index %s' % (_cube, key_or_list))
        s = self.config.get('index_ensure_secs')
        kwargs['cache_for'] = kwargs.get('cache_for', s)
        if name:
            kwargs['name'] = name
        result = _cube.ensure_index(key_or_list, **kwargs)
        return result

    def index_drop(self, index_or_name, collection=None, owner=None):
        '''
        Drops the specified index on this cube.

        :param index_or_name: index (or name of index) to drop
        :param collection: cube name
        :param owner: username of cube owner
        '''
        _cube = self.proxy.get_collection(owner, collection)
        logger.info('[%s] Droping index %s' % (_cube, index_or_name))
        result = _cube.drop_index(index_or_name)
        return result

    # ####### SAVE/REMOVE ########

    def rename(self, new_name=None, new_owner=None, drop_target=False,
               collection=None, owner=None):
        '''
        Rename a cube.

        :param new_name: new cube name
        :param new_owner: new cube owner (admin privleges required!)
        :param collection: cube name
        :param owner: username of cube owner
        '''
        if not (new_name or new_owner):
            raise ValueError("must set either/or new_name or new_owner")
        new_name = new_name or collection or self.name
        if not new_name:
            raise ValueError("new_name is not set!")
        _cube = self.proxy.get_collection(owner, collection)
        if new_owner:
            _from = _cube.full_name
            _to = '%s.%s' % (new_owner, new_name)
            self.proxy.get_db('admin').command(
                'renameCollection', _from, to=_to, dropTarget=drop_target)
            # don't touch the new collection until after attempting
            # the rename; collection would otherwise be created
            # empty automatically then the rename fails because
            # target already exists.
            _new_db = self.proxy.get_db(new_owner)
            result = bool(new_name in _new_db.collection_names())
        else:
            logger.info('[%s] Renaming cube -> %s' % (_cube, new_name))
            _cube.rename(new_name, dropTarget=drop_target)
            db = self.proxy.get_db(owner)
            result = bool(new_name in db.collection_names())
        if collection is None and result:
            self.name = new_name
        return result

    # ####################### ETL API ##################################
    def remove(self, query, date=None, collection=None, owner=None):
        '''
        Remove objects from a cube.

        :param query: `pql` query to filter sample query with
        :param collection: cube name
        :param owner: username of cube owner
        '''
        spec = self.parse_query(query, date)
        _cube = self.proxy.get_collection(owner, collection)
        logger.info("[%s] Removing objects (%s): %s" % (_cube, date, query))
        result = _cube.remove(spec)
        return result

    # ####################### Query API ################################
    def aggregate(self, pipeline, collection=None, owner=None):
        '''
        Run a pql mongodb aggregate pipeline on remote cube

        :param pipeline: The aggregation pipeline. $match, $project, etc.
        :param collection: cube name
        :param owner: username of cube owner
        '''
        _cube = self.proxy.get_collection(owner, collection)
        result = _cube.aggregate(pipeline)
        return result

    def count(self, query=None, date=None, collection=None, owner=None):
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
        _cube = self.proxy.get_collection(owner, collection)
        spec = self.parse_query(query, date)
        result = _cube.find(spec).count()
        return result

    def find(self, query=None, fields=None, date=None, sort=None, one=False,
             raw=False, explain=False, merge_versions=False, skip=0,
             limit=0, as_cursor=False, collection=None, owner=None):
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
        :param owner: username of cube owner
        '''
        _cube = self.proxy.get_collection(owner, collection)
        spec = self.parse_query(query, date)
        fields = parse.parse_fields(fields, as_dict=True) or None

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

    def history(self, query, by_field=None, date_list=None, collection=None,
                owner=None):
        '''
        Run a pql mongodb based query on the given cube and return back the
        aggregate historical counts of matching results.

        :param query: The query in pql
        :param by_field: Which field to slice/dice and aggregate from
        :param date: list of dates that should be used to bin the results
        :param collection: cube name
        :param owner: username of cube owner
        '''
        query = '%s and _start < %s and (_end >= %s or _end == None)' % (
                query, max(date_list), min(date_list))
        spec = self.parse_query(query)

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
        :param collection: cube name
        :param owner: username of cube owner
        '''
        # FIXME check
        if not level or level < 1:
            level = 1
        if isinstance(oids, basestring):
            oids = [s.strip() for s in oids.split(',')]
        checked = set(oids)
        fringe = oids
        loop_k = 0
        _cube = self.proxy.get_collection(owner, collection)
        while len(fringe) > 0:
            if level and loop_k == abs(level):
                break
            query = '_oid in %s and %s != None' % (fringe, field)
            spec = self.parse_query(query, date)
            fields = {'_id': -1, '_oid': 1, field: 1}
            docs = _cube.find(spec, fields=fields)
            fringe = set([oid for doc in docs for oid in doc[field]])
            fringe = filter(lambda oid: oid not in checked, fringe)
            checked |= set(fringe)
            loop_k += 1
        return sorted(checked)

    def distinct(self, field, query=None, date=None,
                 collection=None, owner=None):
        '''
        Return back a distinct (unique) list of field values
        across the entire cube dataset

        :param field: field to get distinct token values from
        :param collection: cube name
        :param owner: username of cube owner
        '''
        _cube = self.proxy.get_collection(owner, collection)
        if query:
            spec = self.parse_query(query, date)
            result = _cube.find(spec).distinct(field)
        else:
            result = _cube.distinct(field)
        return result

    def sample_fields(self, sample_size=None, query=None, date=None,
                      collection=None, owner=None):
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
        :param owner: username of cube owner
        :returns list: sorted list of fields
        '''
        docs = self.sample_docs(sample_size=sample_size, query=query,
                                date=date, collection=collection, owner=owner)
        result = sorted({k for d in docs for k in d.iterkeys()})
        return result

    def sample_docs(self, sample_size=None, query=None, date=None,
                    collection=None, owner=None):
        '''
        Take a randomized sample of documents from a cube.

        :param sample_size: number of random documents to query
        :param query: `pql` query to filter sample query with
        :param collection: cube name
        :param owner: username of cube owner
        :returns list: sorted list of fields
        '''
        sample_size = sample_size or 1
        spec = self.parse_query(query, date)
        _cube = self.proxy.get_collection(owner, collection)
        docs = _cube.find(spec)
        n = docs.count()
        if n <= sample_size:
            docs = list(docs)
        else:
            to_sample = sorted(set(random.sample(xrange(n), sample_size)))
            docs = [docs[i] for i in to_sample]
        return docs

    def _date_pql_string(self, date):
        '''
        Generate a new pql date query component that can be used to
        query for date (range) specific data in cubes.

        :param date: metrique date (range) to apply to pql query

        If date is None, the resulting query will be a current value
        only query (_end == None)

        The tilde '~' symbol is used as a date range separated.

        A tilde by itself will mean 'all dates ranges possible'
        and will therefore search all objects irrelevant of it's
        _end date timestamp.

        A date on the left with a tilde but no date on the right
        will generate a query where the date range starts
        at the date provide and ends 'today'.
        ie, from date -> now.

        A date on the right with a tilde but no date on the left
        will generate a query where the date range starts from
        the first date available in the past (oldest) and ends
        on the date provided.
        ie, from beginning of known time -> date.

        A date on both the left and right will be a simple date
        range query where the date range starts from the date
        on the left and ends on the date on the right.
        ie, from date to date.
        '''
        if date is None:
            return '_end == None'
        if date == '~':
            return ''

        before = lambda d: '_start <= date("%f")' % ts2dt(d)
        after = lambda d: '(_end >= date("%f") or _end == None)' % ts2dt(d)
        split = date.split('~')
        # replace all occurances of 'T' with ' '
        # this is used for when datetime is passed in
        # like YYYY-MM-DDTHH:MM:SS instead of
        #      YYYY-MM-DD HH:MM:SS as expected
        # and drop all occurances of 'timezone' like substring
        split = [re.sub('\+\d\d:\d\d', '', d.replace('T', ' ')) for d in split]
        if len(split) == 1:
            # 'dt'
            return '%s and %s' % (before(split[0]), after(split[0]))
        elif split[0] == '':
            # '~dt'
            return before(split[1])
        elif split[1] == '':
            # 'dt~'
            return after(split[0])
        else:
            # 'dt~dt'
            return '%s and %s' % (before(split[1]), after(split[0]))

    def _query_add_date(self, query, date):
        '''
        Take an existing pql query and append a date (range)
        limiter.

        :param query: pql query
        :param date: metrique date (range) to append
        '''
        date_pql = self._date_pql_string(date)
        if query and date_pql:
            return '%s and %s' % (query, date_pql)
        return query or date_pql

    def parse_query(self, query, date=None):
        '''
        Given a pql based query string, parse it using
        pql.SchemaFreeParser and return the resulting
        pymongo 'spec' dictionary.

        :param query: pql query
        '''
        if not HAS_PQL:
            raise RuntimeError("`pip install pql` required")
        _subpat = re.compile(' in \([^\)]+\)')
        _q = _subpat.sub(' in (...)', query) if query else query
        logger.debug('Query: %s' % _q)
        query = self._query_add_date(query, date)
        if not query:
            return {}
        if not isinstance(query, basestring):
            raise TypeError("query expected as a string")
        pql_parser = pql.SchemaFreeParser()
        try:
            spec = pql_parser.parse(query)
        except Exception as e:
            raise SyntaxError("Invalid Query (%s)" % str(e))
        return spec


# ################################ SQL ALCHEMY ###############################
class SQLAlchemyProxy(object):
    config = None
    config_key = 'sqlalchemy'
    config_file = DEFAULT_CONFIG
    _engine = None
    _session = None
    _sessionmaker = None
    _Base = None
    _meta = None
    _table = None
    RESERVED_WORDS = None
    TYPE_MAP = None

    def __init__(self, db, debug=None, cache_dir=None,
                 config_key=None, config_file=None, dialect=None,
                 driver=None, host=None, port=None,
                 username=None, password=None,
                 connect_args=None, shared_base=False, **kwargs):
        if not HAS_SQLALCHEMY:
            raise RuntimeError('`pip install sqlalchemy` required')
        self.RESERVED_WORDS = deepcopy(RESERVED_WORDS)
        self.TYPE_MAP = deepcopy(TYPE_MAP)

        is_true(db, 'db can not be null')
        self._cache_dir = cache_dir or CACHE_DIR

        options = dict(
            connect_args=connect_args,
            db=db,
            dialect=dialect,
            debug=debug,
            driver=driver,
            host=host,
            password=password,
            port=None,
            username=username,
        )
        defaults = dict(
            connect_args=None,
            db=None,
            debug=logging.INFO,
            dialect=None,
            driver=None,
            host=None,
            password=None,
            port=None,
            username=getuser(),
        )
        self.config = self.config or {}
        self.config_file = config_file or self.config_file
        self.config_key = config_key or SQLAlchemyProxy.config_key
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                section_only=True,
                                update=self.config)
        self._debug_setup_sqlalchemy_logging()

        self.engine_init()
        self.session_init()
        self.base_init(shared=shared_base)

    def autoschema(self, objects, fast=True):
        is_true(objects, 'object samples can not be null')
        objects = objects if isinstance(objects, (list, tuple)) else [objects]
        schema = defaultdict(dict)
        for o in objects:
            for k, v in o.iteritems():
                # FIXME: option to check rigerously all objects
                # consistency; raise exception if values are of
                # different type given same key, etc...
                if k in schema or k in HASH_EXCLUDE_KEYS:
                    continue
                _type = type(v)
                if _type in (list, tuple, set):
                    schema[k]['container'] = True
                    # FIXME: if the first object happens to be null
                    # we auto set to UnicodeText type...
                    # (default for type(None))
                    # but this isn't always going to be accurate...
                    if len(v) > 1:
                        _t = type(v[0])
                    else:
                        _t = type(None)
                    schema[k]['type'] = _t
                else:
                    schema[k]['type'] = _type
            if fast is True:  # finish after first sample
                break
        return schema

    def base_init(self, shared=False, bind=None):
        is_true(self._Base is None, 'Base is already defined.')
        bind = bind or self.engine
        if shared:
            Base = sqla_Base
        else:
            meta = MetaData()
            Base = declarative_base(metadata=meta)
        if bind:
            Base.metadata.bind = bind
        self._Base = Base
        return self._Base

    def columns(self, table, reflect=False):
        table = self.get_table(table)
        if reflect:
            columns = [c for c in table.columns]
        else:
            columns = sorted(c.name for c in table.columns)
        return columns

    def drop_tables(self, tables):
        _i = self.get_inspector()
        _tables = _i.get_table_names()
        if tables is True:
            tables = _tables
        else:
            tables = [t for t in _tables if t in tables]
        if not tables:
            logger.warn("No tables found to drop")
            return
        logger.warn("Permanently dropping %s" % tables)
        [Table(n, self._Base.metadata, autoload=True).drop() for n in tables]
        self.base_init()

    def _debug_setup_sqlalchemy_logging(self):
        level = self.config.get('debug')
        debug_setup(logger='sqlalchemy', level=level)

    # ######################## DB API ##################################
    @property
    def engine(self):
        if not self._engine:
            self.engine_init()
        return self._engine

    def engine_dispose(self):
        if self._engine:
            # dispose
            self._engine.dispose()
            return True
        else:
            return None

    def engine_init(self, **kwargs):
        self.engine_dispose()
        db = self.config.get('db')
        host = self.config.get('host')
        port = self.config.get('port')
        driver = self.config.get('driver')
        dialect = self.config.get('dialect')
        username = self.config.get('username')
        password = self.config.get('password')
        connect_args = self.config.get('connect_args')
        uri = self.get_engine_uri(db=db, host=host, port=port,
                                  driver=driver, dialect=dialect,
                                  username=username, password=password,
                                  connect_args=connect_args)
        if dialect is None or re.search('sqlite', uri):
            uri, _kwargs = self._sqla_sqlite3(uri)
        elif re.search('teiid', uri):
            uri, _kwargs = self._sqla_teiid(uri)
        elif re.search('postgresql', uri):
            uri, _kwargs = self._sqla_postgresql(uri)
        else:
            raise NotImplementedError("Unsupported engine: %s" % uri)
        _kwargs.update(kwargs)
        self._engine = create_engine(uri, echo=False, **_kwargs)
        self._sessionmaker = sessionmaker(bind=self._engine)
        return self._engine

    def ensure_table(self, name, schema, force=False):
        is_true(name, 'table name must be defined')
        is_true(schema, 'schema name must be defined')
        name = str(name)  # Table() expects str()
        engine = self.engine
        meta = self._Base.metadata
        schema = deepcopy(schema)  # don't mutate original instance

        # maybe we already have the table?
        _table = self.get_table(name)
        if _table is not None:
            return _table

        # try to reflect the table
        try:
            _table = Table(name, meta, autoload=True, autoload_replace=True,
                           extend_existing=True, autoload_with=engine)
            logger.debug("Successfully refected table: %s" % name)
        except Exception as e:
            logger.debug("Failed to reflect table %s: %s" % (name, e))
            _table = None

        if _table is None:
            _table = self._schema2table(name=name, schema=schema)

        if _table is not None:
            logger.debug("Creating Tables on %s" % engine)
            meta.create_all(engine)
            _table = self.get_table(name)
        return _table

    def exec_transaction(self, func, **kwargs):
        isolation_level = kwargs.get('isolation_level')
        engine = self.proxy.get_engine(isolation_level=isolation_level)
        result = None
        with engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    result = func(connection, transaction, **kwargs)
                except:
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                    result = result
        engine.dispose()
        return result

    def exists(self, table):
        return table in self.proxy.ls()

    @staticmethod
    def _gen_id(context):
        obj = context.current_parameters
        _oid = obj.get('_oid')
        assert _oid is not None
        if obj.get('_end'):
            _start = obj.get('_start')
            # if the object at the exact start/oid is later
            # updated, it's possible to just save(upsert=True)
            _id = ':'.join(map(str, (_oid, dt2ts(_start))))
        else:
            # if the object is 'current value' without _end,
            # use just str of _oid
            _id = _oid
        return unicode(_id)

    @staticmethod
    def _gen_hash(context):
        o = deepcopy(context.current_parameters)
        keys = set(o.iterkeys())
        [o.pop(k) for k in HASH_EXCLUDE_KEYS if k in keys]
        return jsonhash(o)

    def get_engine_uri(self, db=None, host=None, port=None, driver=None,
                       dialect=None, username=None, password=None,
                       connect_args=None):
        db = db or self.config.get('db')
        host = host or self.config.get('host') or '127.0.0.1'
        port = port or self.config.get('port') or 5432
        driver = driver or self.config.get('driver')
        dialect = dialect or self.config.get('dialect')
        username = username or self.config.get('username')
        password = password or self.config.get('password')
        is_true(db, 'db can not be null')
        is_true(bool(dialect in [None, 'postgresql', 'sqlite', 'teiid']),
                'invalid dialect: %s' % dialect)
        if dialect and driver:
            dialect = '%s+%s' % (dialect, driver)
        elif dialect:
            pass
        else:
            dialect = 'sqlite'
            dialect = dialect.replace('://', '')

        if dialect == 'sqlite':
            # db is expected to be an absolute path to where
            # sqlite db will be saved
            db = os.path.join(self._cache_dir, '%s.sqlite' % db)
            uri = _uri = '%s:///%s' % (dialect, db)
        else:
            if username and password:
                u_p = '%s:%s@' % (username, password)
                _u_p = '%s:XXX@' % username  # hide password from log
            elif username:
                _u_p = u_p = '%s@' % username
            else:
                _u_p = u_p = ''

            uri = '%s://%s%s:%s/%s' % (dialect, u_p, host, port, db)
            _uri = '%s://%s%s:%s/%s' % (dialect, _u_p, host, port, db)
            if connect_args:
                args = ['%s=%s' % (k, v) for k, v in connect_args.iteritems()]
                args = '?%s' % '&'.join(args)
                uri += args
                _uri += args
        return uri

    def get_table(self, table):
        is_true(table is not None, 'table must be defined!')
        if isinstance(table, Table):
            return table
        else:
            tables = self.get_tables()
            table = tables.get(table)
            return table

    def get_tables(self):
        return self._Base.metadata.tables

    def get_inspector(self, engine=None):
        engine = engine or self.engine
        return inspect(engine)

    def _load_sql(self, sql):
        # load sql kwargs from instance config
        engine = self.get_engine()
        rows = engine.execute(sql)
        objects = [dict(row) for row in rows]
        engine.dispose()
        return objects

    def _parse_fields(self, table, fields=None, reflect=False, **kwargs):
        table = self.get_table(table)
        fields = parse.parse_fields(fields)
        if fields in ([], {}):
            fields = [c.name for c in table.columns]
        if reflect:
            fields = [c for c in table.columns if c.name in fields]
        return fields

    def _parse_query(self, table, query=None, fields=None, date=None,
                     alias=None, distinct=None, limit=None):
        table = self.get_table(table)
        parser = parse.SQLAlchemyMQLParser(table)
        query = parser.parse(query=query, date=date,
                             fields=fields, distinct=distinct,
                             alias=alias)
        return query

    @property
    def proxy(self):
        return self.engine

    def _rows2dicts(self, rows):
        return [self._row2dict(r) for r in rows]

    def _row2dict(self, row):
        return dict(row)

    def session_dispose(self):
        self._session.close()
        del self._session
        self._session = None

    def session_init(self, autoflush=False, autocommit=False,
                     expire_on_commit=True, cached=True, **kwargs):
        if not (self._engine and self._sessionmaker):
            raise RuntimeError("engine is not initiated")
        self._session = self._sessionmaker(
            autoflush=autoflush, autocommit=autocommit,
            expire_on_commit=expire_on_commit,
            bind=self.engine, **kwargs)
        return self._session

    @property
    def session(self):
        if not self._session:
            self.session_init()
        return self._session

    def _sqla_sqlite3(self, uri, isolation_level="READ UNCOMMITTED"):
        isolation_level = isolation_level or "READ UNCOMMITTED"
        kwargs = dict(isolation_level=isolation_level)
        return uri, kwargs

    def _sqla_teiid(self, uri, version=None, isolation_level="AUTOCOMMIT"):
        uri = re.sub('^.*://', 'postgresql+psycopg2://', uri)
        # version normally comes "'Teiid 8.5.0.Final'", which sqlalchemy
        # failed to parse
        version = version or (8, 2)
        r_none = lambda *i: None
        pg.base.PGDialect.description_encoding = str('utf8')
        pg.base.PGDialect._check_unicode_returns = lambda *i: True
        pg.base.PGDialect._get_server_version_info = lambda *i: version
        pg.base.PGDialect.get_isolation_level = lambda *i: isolation_level
        pg.base.PGDialect._get_default_schema_name = r_none
        pg.psycopg2.PGDialect_psycopg2.set_isolation_level = r_none
        return self._sqla_postgresql(uri=uri, version=version,
                                     isolation_level=isolation_level)

    def _sqla_postgresql(self, uri, version=None,
                         isolation_level="READ COMMITTED"):
        '''
        expected uri form:
        postgresql+psycopg2://%s:%s@%s:%s/%s' % (
            username, password, host, port, vdb)
        '''
        isolation_level = isolation_level or "READ COMMITTED"
        kwargs = dict(isolation_level=isolation_level)
        # override default dict and list column types
        types = {list: ARRAY, tuple: ARRAY, set: ARRAY,
                 dict: JSONTyped, MutableMapping: JSONTyped}
        self.TYPE_MAP.update(types)
        return uri, kwargs

    def _schema2table(self, name, schema):
        is_true(name, "table name must be defined!")
        is_true(schema, "schema must be defined!")
        logger.debug("Attempting to create table: %s..." % name)

        __repr__ = lambda s: '%s(%s)' % (
            s.__tablename__,
            ', '.join(['%s=%s' % (k, v) for k, v in s.__dict__.iteritems()
                      if k != '_sa_instance_state']))

        _ignore_keys = set(['_id', '_hash'])
        __init__ = lambda s, kw: [setattr(s, k, v) for k, v in kw.iteritems()
                                  if k not in _ignore_keys]
        defaults = {
            '__tablename__': name,
            '__table_args__': ({'extend_existing': True}),
            'id': Column('id', Integer, primary_key=True),
            # FIXME: use hybrid properties? for _id and _hash?
            '_id': Column(CoerceUTF8, nullable=False,
                          onupdate=self._gen_id,
                          default=self._gen_id,
                          unique=True,
                          index=True),
            '_hash': Column(CoerceUTF8, nullable=False,
                            onupdate=self._gen_hash,
                            default=self._gen_hash,
                            index=True),
            '_start': Column(DateTime, default=utcnow(),
                             index=True, nullable=False),
            '_end': Column(DateTime, default=None, index=True,
                           nullable=True),
            '_v': Column(Integer, default=0),
            '__v__': Column(CoerceUTF8, default=__version__,
                            onupdate=lambda x: __version__),
            '_e': Column(self.TYPE_MAP[dict], default={}),
            '__init__': __init__,
            '__repr__': __repr__,
        }

        schema_items = schema.items()
        for k, v in schema_items:
            __type = v.get('type')
            if __type is None:
                __type = type(None)
            _type = TYPE_MAP.get(__type)
            if v.get('container', False):
                # FIXME: alternative association table implementation?
                # FIXME: requires postgresql+psycopg2
                _list_type = self._list_type
                if _list_type is ARRAY:
                    _list_type = _list_type(_type)
                schema[k] = Column(_list_type)
            else:
                if k == '_oid':
                    # in case _oid is defined in the schema,
                    # make sure we index it and it's unique
                    schema[k] = Column(_type, nullable=False, index=True,
                                       unique=False)
                else:
                    quote = False
                    if k in self.RESERVED_WORDS:
                        # FIXME: This isn't working!
                        # FIXME: Does the name actually have to include
                        # quotes!?
                        #_k = '"%s"' % k
                        quote = True
                    #else:
                    #    _k = k
                    schema[k] = Column(_type, name=k, quote=quote)
        defaults.update(schema)

        # in case _oid isn't set yet, default to big int column
        defaults.setdefault('_oid', Column(BigInteger, nullable=False,
                                           index=True, unique=False))

        _cube = type(str(name), (self._Base,), defaults)
        return _cube

    # ######################## Cube API ################################
    def count(self, table, query=None, date=None):
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
        sql_count = select([func.count()])
        query = self._parse_query(table=table, query=query, date=date,
                                  fields='id', alias='anon_x')
        if query is not None:
            query = sql_count.select_from(query)
        else:
            table = self.get_table(table)
            query = sql_count
            query = query.select_from(table)
        return self.session.execute(query).scalar()

    def deptree(self, table, field, oids, date=None, level=None):
        '''
        Dependency tree builder. Recursively fetchs objects that
        are children of the initial set of parent object ids provided.

        :param field: Field that contains the 'parent of' data
        :param oids: Object oids to build depedency tree for
        :param date: date (metrique date range) that should be queried.
                    If date==None then the most recent versions of the
                    objects will be queried.
        :param level: limit depth of recursion
        '''
        table = self.get_table(table)
        fringe = str2list(oids)
        checked = set(fringe)
        loop_k = 0
        while len(fringe) > 0:
            if level and loop_k == abs(level):
                break
            query = '_oid in %s' % list(fringe)
            docs = self.find(table=table, query=query, fields=[field],
                             date=date, raw=True)
            fringe = {id for doc in docs for oid in doc[field]
                      if oid not in checked}
            checked |= fringe
            loop_k += 1
        return sorted(checked)

    def distinct(self, table, fields, query=None, date='~'):
        '''
        Return back a distinct (unique) list of field values
        across the entire cube dataset

        :param field: field to get distinct token values from
        :param query: query to filter results by
        '''
        date = date or '~'
        query = self._parse_query(table=table, query=query, date=date,
                                  fields=fields, alias='anon_x',
                                  distinct=True)
        ret = [r[0] for r in self.session.execute(query)]
        if ret and isinstance(ret[0], list):
            ret = reduce(add, ret, [])
        return sorted(set(ret))

    def drop(self, table, quiet=True):
        table = self.get_table(table)
        self.session_dispose()
        result = table.drop(self.engine)
        self.session_init()
        return result

    def find(self, table, query=None, fields=None, date=None, sort=None,
             descending=False, one=False, raw=False, limit=None,
             as_cursor=False, scalar=False):
        is_true(table is not None, 'table must be defined!')
        table = self.get_table(table)
        limit = limit if limit and limit >= 1 else 0
        fields = parse.parse_fields(fields)
        query = self._parse_query(table, query=query, fields=fields,
                                  date=date, limit=limit)
        if sort:
            order_by = parse.parse_fields(fields=sort)[0]
            if descending:
                query = query.order_by(desc(order_by))
            else:
                query = query.order_by(order_by)

        rows = self.session.execute(query)
        if scalar:
            return rows.scalar()
        elif as_cursor:
            return rows
        elif one or limit == 1:
            row = self._row2dict(rows.first())
            # implies raw
            return row
        elif limit > 1:
            rows = rows.fetchmany(limit)
        else:
            rows = rows.fetchall()
        rows = self._rows2dicts(rows)
        if raw:
            return rows
        else:
            return Result(rows, date)

    def get_last_field(self, table, field):
        '''Shortcut for querying to get the last field value for
        a given owner, cube.

        :param field: field name to query
        '''
        is_true(table is not None, 'table must be defined!')
        is_true(field is not None, 'field must be defined!')
        _table = self.get_table(table)
        if table is None:
            last = None
        else:
            last = self.find(table=_table, fields=field, scalar=True,
                             sort=field, limit=1, descending=True, date='~')
        logger.debug("last %s.%s: %s" % (table, field, last))
        return last

    def insert(self, table, objects, commit=True):
        table = self.get_table(table)
        objects = objects if isinstance(objects, (list, tuple)) else [objects]
        objects = MetriqueContainer(to_encoding(utcnow()),
                                    objects=objects).values()
        self.session.execute(table.insert(), objects)
        if commit:
            self.session.commit()

    def ls(self, startswith=None, reflect=False):
        '''
        List all cubes available to the calling client.

        :param startswith: string to use in a simple "startswith" query filter
        :returns list: sorted list of cube names
        '''
        cubes = self.engine.table_names()
        startswith = unicode(startswith or '')
        cubes = sorted(name for name in cubes if name.startswith(startswith))
        logger.info(
            'Listing cubes starting with "%s")' % startswith)
        if reflect:
            return [self.get_table(c) for c in cubes]
        else:
            return cubes

    def upsert(self, table, objects, commit=True, autosnap=None):
        table = self.get_table(table)
        objects = objects if isinstance(objects, (list, tuple)) else [objects]
        objects = MetriqueContainer(to_encoding(utcnow()), objects=objects)
        if autosnap is None:
            # assume autosnap:True if all objects have _end:None
            # otherwise, false (all objects have _end:non-null or
            # a mix of both)
            autosnap = all([o['_end'] is None for o in objects.itervalues()])
            logger.warn('AUTOSNAP auto-set to: %s' % autosnap)

        _ids = objects._ids
        q = '_id in %s' % _ids
        t1 = time()
        dups = {o._id: dict(o) for o in self.find(table=table, query=q,
                                                  fields='~', date='~',
                                                  as_cursor=True)}
        diff = int(time() - t1)
        logger.debug(
            'dup query completed in %s seconds (%s)' % (diff, len(dups)))

        dup_k, snap_k = 0, 0
        inserts = []
        u = update(table)
        for i, o in enumerate(objects.itervalues()):
            dup = dups.get(o['_id'])
            if dup:
                if o['_hash'] == dup['_hash']:
                    dup_k += 1
                elif o['_end'] is None and autosnap:
                    # remove dup primary key, it will get a new one
                    del dup['id']
                    # set existing objects _end to new objects _start
                    dup['_end'] = o['_start']
                    # update _id, _hash, etc
                    dup = MetriqueObject(**dup)
                    _ids.append(dup['_id'])
                    # insert the new object
                    inserts.append(dup)
                    # replace the existing _end:None object with new values
                    _id = o['_id']
                    self.session.execute(
                        u.where(table.c._id == _id).values(**o))
                    snap_k += 1

                else:
                    o = MetriqueObject(**o)
                    # don't try to set _id
                    _id = o.pop('_id')
                    assert _id == dup['_id']
                    self.session.execute(
                        u.where(table.c._id == _id).values(**o))
            else:
                inserts.append(o)

        if inserts:
            t1 = time()
            self.insert(table, inserts)
            diff = int(time() - t1)
            logger.debug('%s inserts in %s seconds' % (len(inserts), diff))
        logger.debug('%s existing objects snapshotted' % snap_k)
        logger.debug('%s duplicates not re-saved' % dup_k)
        if commit:
            self.session.flush()
            self.session.commit()
        return sorted(map(unicode, _ids))


class SQLAlchemyContainer(MetriqueContainer):
    __indexes = None
    _objects = None
    config = None
    config_file = DEFAULT_CONFIG
    config_key = 'sqlalchemy'
    name = None
    VALID_SHARE_ROLES = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
    default_fields = {'_start': 1, '_end': 1, '_oid': 1}
    _proxy_kwargs = None
    _table = None

    def __init__(self, name, objects=None,
                 proxy=None, proxy_kwargs=None,
                 schema=None, batch_size=None, cache_dir=None,
                 config_file=None, config_key=None, debug=None,
                 _version=None, autoreflect=True, **kwargs):
        if not HAS_SQLALCHEMY:
            raise RuntimeError('`pip install sqlalchemy` required')

        super(SQLAlchemyContainer, self).__init__(name=name,
                                                  objects=objects,
                                                  _version=_version)

        options = dict(
            batch_size=batch_size,
            cache_dir=cache_dir,
            debug=debug,
            schema=schema,
        )
        # set defaults to None for proxy related args
        # since proxy will apply its' defaults if None
        defaults = dict(
            batch_size=10000,
            cache_dir=CACHE_DIR,
            debug=None,
            schema=None,
        )
        self.config = self.config or {}
        self.config_file = config_file or self.config_file
        config_key = config_key or SQLAlchemyContainer.config_key
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=config_key,
                                section_only=True,
                                update=self.config)

        self._proxy_kwargs = deepcopy(proxy_kwargs or {})
        self.set_proxy(proxy)

        if autoreflect:
            self.ensure_table(schema=schema, name=name)

        self.__indexes = self.__indexes or {}

    def add(self, item, pop_id=True):
        # id is reserved as sql primary key
        # let sqlalchemy add 'id'; doc should already have _oid set
        # and _id will be set automatically
        if pop_id and 'id' in item:
            item.pop('id')
        super(SQLAlchemyContainer, self).add(item)

    def distinct(self, fields, query=None, date='~'):
        date = date or '~'
        fields = parse.parse_fields(fields)
        query = self._parse_query(query=query, date=date, fields=fields,
                                  alias='anon_x', distinct=True)
        ret = [r[0] for r in self.proxy.session.execute(query)]
        if ret and isinstance(ret[0], list):
            ret = reduce(add, ret, [])
        return sorted(set(ret))

    def drop(self, quiet=True):
        result = self.proxy.drop(table=self._table, quiet=quiet)
        self._table = None
        return result

    def deptree(self, field, oids, date=None, level=None):
        return self.proxy.deptree(table=self.name, field=field,
                                  oids=oids, date=date, level=level)

    def ensure_table(self, name=None, schema=None, force=False):
        if self._table is None:
            is_true(self.store, 'no objects available to sample schema from')
            name = name or self.name
            schema = self.proxy.autoschema(self.store.values())
            self._table = self.proxy.ensure_table(
                table=name, schema=schema, force=force)
            setattr(self, name, self._table)  # named alias for table class
        return self._table

    @property
    def exists(self):
        return self.proxy.exists(self.name)

    def get_last_field(self, field):
        return self.proxy.get_last_field(self.name, field=field)

    def _index_default_name(self, columns, name=None):
        if name:
            return name
        elif isinstance(columns, basestring):
            return columns
        elif isinstance(columns, (list, tuple)):
            return '_'.join(columns)
        else:
            raise ValueError(
                "unable to get default name from columns: %s" % columns)

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
        s = self.config.get('index_ensure_secs')
        name = self._index_default_name(fields, name)
        fields = parse.parse_fields(fields, reflect=True)
        engine = self.proxy.get_engine()
        if name not in self.__indexes or (time() - self.__indexes[name]) <= s:
            index = Index(name, *fields)
            logger.info('Writing new index %s: %s' % (name, fields))
            result = index.create(engine)
            self.__indexes[name] = time()
        engine.dispose()
        return result

    def index_list(self):
        '''
        List all cube indexes

        :param collection: cube name
        :param owner: username of cube owner
        '''
        logger.info('Listing indexes')
        engine = self.proxy.get_engine()
        _i = self.inspect()
        ix = [i for n in _i.get_table_names() for i in _i.get_indexes(n)]
        engine.dispose()
        return ix

    def insert(self):
        return insert(self._table)

    @property
    def proxy(self):
        _proxy = getattr(self, '_proxy', None)
        if not _proxy:
            config_key = self.config_key
            config_file = self.config_file
            self._proxy = SQLAlchemyProxy(config_key=config_key,
                                          config_file=config_file,
                                          **self.config)
        return self._proxy

    def share(self, with_user, table=None, roles=None):
        '''
        Give cube access rights to another user
        '''
        table = table or self.name
        with_user = self._validate_username(with_user)
        roles = self._validate_roles(roles or ['SELECT'])
        logger.info(
            'Sharing cube with %s (%s)' % (with_user, roles))
        roles = ','.join(roles)
        sql = 'GRANT %s ON %s TO %s' % (roles, table, with_user)
        session = self.proxy.get_session()
        session.connection().connection.set_isolation_level(0)
        session.execute(sql)
        return True

    def update(self, objects, commit=True, autosnap=None):
        return self.proxy.update(self.name, objects=objects, commit=commit,
                                 autosnap=autosnap)

    def user_register(self, username=None, password=None):
        # FIXME: enable setting roles at creation time...
        password = password or self.config.get('password')
        username = username or self.config.get('username')
        if username and not password:
            raise RuntimeError('must specify password!')
        u = self._validate_username(username)
        p = self._validate_password(password)
        logger.info('Registering new user %s' % u)
        # FIXME: make a generic method which runs list of sql statements
        sql = []
        sql.append("CREATE USER %s WITH PASSWORD '%s';" % (u, p))
        sql.append("create database %s WITH OWNER %s;" % (u, u))
        session = self.proxy.get_session()
        session.connection().connection.set_isolation_level(0)
        [session.execute(s) for s in sql]
        return True

    def user_disable(self, username=None):
        username = username or self.config.get('username')
        logger.info('Disabling existing user %s' % username)
        session = self.proxy.get_session()
        u = update('pg_database')
        #update pg_database set datallowconn = false where datname = 'applogs';
        sql = u.where(
            "datname = '%s'" % username).values({'datallowconn': 'false'})
        session.execute(sql)
        session.commit()
        return True
