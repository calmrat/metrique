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

from __future__ import unicode_literals, absolute_import

from collections import defaultdict
from getpass import getuser
import logging
logger = logging.getLogger('metrique')

from copy import deepcopy
from inspect import isclass
import os

from time import time

from metrique.utils import get_cube, load_config, configure, gimport
from metrique.utils import debug_setup, is_true

ETC_DIR = os.environ.get('METRIQUE_ETC')
CACHE_DIR = os.environ.get('METRIQUE_CACHE')
LOG_DIR = os.environ.get('METRIQUE_LOGS')
TMP_DIR = os.environ.get('METRIQUE_TMP')
DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metrique.json')
HASH_EXCLUDE_KEYS = ('_hash', '_id', '_start', '_end', '__v__', 'id')


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
    config = None
    config_file = DEFAULT_CONFIG
    config_key = 'metrique'
    global_config_key = 'metrique'
    #mongodb_config_key = 'mongodb'
    #sqlalchemy_config_key = 'metrique'
    container_config_key = 'container'
    proxy_config_key = 'proxy'
    name = None
    fields = None
    _container = None
    _container_cls = None
    _proxy = None
    _proxy_cls = None
    _schema_keys = ('type', 'container')
    __metaclass__ = MetriqueFactory

    def __init__(self, name=None, db=None, config_file=DEFAULT_CONFIG,
                 config=None, config_key='metrique', cube_pkgs=None,
                 cube_paths=None, debug=None, log_file='metrique.log',
                 log2file=True, log2stdout=False, workers=2,
                 log_dir=LOG_DIR, cache_dir=CACHE_DIR, etc_dir=ETC_DIR,
                 tmp_dir=TMP_DIR, container=None, container_config=None,
                 proxy=None, proxy_config=None, version=0, schema=None):
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

        self.name = name or self.name or BaseClient.name

        # cube class defined name
        # FIXME: this is ugly... and not obvious...
        # only used currently in sqldata.Generic
        self._cube = type(self).name

        options = dict(cache_dir=cache_dir,
                       cube_pkgs=cube_pkgs,
                       cube_paths=cube_paths,
                       db=db,
                       debug=debug,
                       etc_dir=etc_dir,
                       log_dir=log_dir,
                       log_file=log_file,
                       log2file=log2file,
                       log2stdout=log2stdout,
                       name=self.name,
                       schema=schema,
                       tmp_dir=tmp_dir,
                       version=version,
                       workers=workers)

        defaults = dict(cache_dir=CACHE_DIR,
                        cube_pkgs=['cubes'],
                        cube_paths=[],
                        db=getuser(),
                        debug=None,
                        etc_dir=ETC_DIR,
                        log_file='metrique.log',
                        log_dir=LOG_DIR,
                        log2file=True,
                        log2stdout=False,
                        name=None,
                        schema={},
                        tmp_dir=TMP_DIR,
                        version=0,
                        workers=2)

        # FIXME: update os.environ LOG_DIR, ETC_DIR, etc to config'd value

        # if config is passed in, set it, otherwise start
        # with class assigned default or empty dict
        self.config = deepcopy(config or BaseClient.config or {})
        self.config_file = config_file or BaseClient.config_file
        self.config_key = config_key or BaseClient.global_config_key
        # load defaults + set args passed in
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                update=self.config)

        level = self.lconfig.get('debug')
        log2stdout = self.lconfig.get('log2stdout')
        log_format = None
        log2file = self.lconfig.get('log2file')
        log_dir = self.lconfig.get('log_dir')
        log_file = self.lconfig.get('log_file')
        debug_setup(logger='metrique', level=level, log2stdout=log2stdout,
                    log_format=log_format, log2file=log2file,
                    log_dir=log_dir, log_file=log_file)

        container_config = dict(container_config or {})
        container_config.setdefault('name', self.name)
        container_config.setdefault('schema', schema)
        self.config[self.container_config_key].update(container_config)

        proxy_config = dict(proxy_config or {})
        proxy_config.setdefault('table', self.name)
        self.config[self.proxy_config_key].update(proxy_config)

        if isinstance(proxy, basestring):
            proxy = gimport(proxy)
        self._proxy = proxy

        if isinstance(container, basestring):
            container = gimport(container)
        self._container = container

        if self._container_cls is None:
            from metrique.core_api import MetriqueContainer
            self._container_cls = MetriqueContainer
        if self._proxy_cls is None:
            from metrique.sqlalchemy import SQLAlchemyProxy
            self._proxy_cls = SQLAlchemyProxy

    @property
    def container(self):
        if self._container is None or isclass(self._container):
            self.container_init()
        return self._container

    @container.setter
    def container(self, value):
        self.container_init(value=value)

    @container.deleter
    def container(self):
        # replacing existing container with a new, empty one
        self._container = self.container_init()

    @property
    def objects(self):
        return self.container

    @objects.setter
    def objects(self, value):
        self.container = value

    @objects.deleter
    def objects(self):
        del self.container

    def get_cube(self, cube, init=True, name=None, copy_config=True, **kwargs):
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
        config_file = self.config_file
        container = type(self.objects)
        container_config = deepcopy(self.container_config)
        proxy = type(self.proxy)
        proxy_config = deepcopy(self.proxy_config)
        return get_cube(cube=cube, init=init, name=name, config=config,
                        config_file=config_file, container=container,
                        container_config=container_config,
                        proxy=proxy, proxy_config=proxy_config, **kwargs)

    def _get_schema(self, except_=False):
        # schema might have been passed in as kwarg and stored in the config
        # or subclasses might define schema (and more) within self.fields attr
        _schema = (self.container_config.get('schema') or
                   getattr(self, 'fields') or {})
        print self.container_config.get('schema')
        print getattr(self, 'fields')
        # in the case we derived our schema from 'fields' attr, we must filter
        # out anything we're not specifically interested in
        schema = defaultdict(dict)
        for field, meta in _schema.iteritems():
            for k, v in meta.iteritems():
                schema[field].setdefault(k, {})
                if k in self._schema_keys:
                    schema[field][k] = v
        except_ and is_true(schema, 'schema not defined!')
        return schema

    def container_init(self, value=None, **kwargs):
        config = self.container_config
        # try to get schema from any of the known places
        # it could be derived from
        schema = self._get_schema()
        config.update(dict(schema=schema))
        config.update(kwargs)
        if self._container is None:
            self._container = self._container_cls
        msg = "Invalid container: %s" % self._container
        if isclass(self._container):
            self._container = self._container(objects=value, **config)
        is_true(isinstance(self._container, self._container_cls), msg)
        return self._container

    @property
    def proxy(self):
        if self._proxy is None:
            self.proxy_init()
        return self._proxy

    def proxy_init(self, **kwargs):
        config = self.proxy_config
        config.update(kwargs)
        if self._proxy is None:
            self._proxy = self._proxy_cls
        msg = "Invalid proxy: %s" % self._proxy
        if isclass(self._proxy):
            self._proxy = self._proxy(**config)
        is_true(isinstance(self._proxy, self._proxy_cls), msg)
        return self._proxy

    @property
    def gconfig(self):
        if self.config:
            return self.config.get(self.global_config_key) or {}
        else:
            return {}

    def get_objects(self, flush=False, autosnap=True, **kwargs):
        '''
        Main API method for sub-classed cubes to override for the
        generation of the objects which are to (potentially) be added
        to the cube (assuming no duplicates)
        '''
        logger.debug('Running get_objects(flush=%s, autosnap=%s, %s)' % (
                     flush, autosnap, kwargs))
        if flush:
            s = time()
            result = self.objects.flush(autosnap=autosnap, **kwargs)
            diff = time() - s
            logger.debug("Flush complete (%ss)" % int(diff))
            return result
        else:
            return self

    @property
    def container_config(self):
        if self.config:
            return self.config.get(self.container_config_key) or {}
        else:
            return {}

    @property
    def proxy_config(self):
        if self.config:
            return self.config.get(self.proxy_config_key) or {}
        else:
            return {}

    @property
    def lconfig(self):
        if self.config:
            return self.config.get(self.config_key) or {}
        else:
            return {}

    def load_config(self, path):
        return load_config(path)
