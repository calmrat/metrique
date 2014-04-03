#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

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

from collections import Mapping, MutableMapping
from copy import deepcopy
import glob
import logging
import os
import pandas as pd
import re
import shlex
import signal
import subprocess
import urllib

from metrique.utils import get_cube, utcnow, jsonhash, dt2ts, load_config
from metrique.utils import rupdate

logger = logging.getLogger(__name__)

# if HOME environment variable is set, use that
# useful when running 'as user' with root (supervisord)
ETC_DIR = os.environ.get('METRIQUE_ETC')
DEFAULT_CONFIG = os.path.join(ETC_DIR, 'metrique.json')

FIELDS_RE = re.compile('[\W]+')
SPACE_RE = re.compile('\s+')
UNDA_RE = re.compile('_')

HASH_EXCLUDE_KEYS = ['_hash', '_id', '_start', '_end']
IMMUTABLE_OBJ_KEYS = set(['_hash', '_id', '_oid'])
TIMESTAMP_OBJ_KEYS = set(['_end', '_start'])


class MetriqueObject(Mapping):
    def __init__(self, _oid, strict=False, **kwargs):
        self._strict = strict
        self.store = {
            '_oid': _oid,
            '_id': None,
            '_hash': None,
            '_start': utcnow(),
            '_end': None,
        }
        self._update(kwargs)
        self._re_hash()

    def _update(self, obj):
        for key, value in obj.iteritems():
            key = self.__keytransform__(key)
            if key in IMMUTABLE_OBJ_KEYS:
                if self._strict:
                    raise KeyError("%s is immutable" % key)
                else:
                    #logger.debug("%s is immutable; not setting" % key)
                    continue
            if key in TIMESTAMP_OBJ_KEYS and value is not None:
                # ensure normalized timestamp
                value = dt2ts(value)
            if isinstance(value, str):
                value = unicode(value, 'utf8')
            if value == '' or value != value:
                # Normalize empty strings and NaN objects to None
                # NaN objects do not equal themselves...
                value = None
            self.store[key] = value

    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __repr__(self):
        return repr(self.store)

    def __hash__(self):
        return hash(self['_id'])

    def __keytransform__(self, key):
        key = key.lower()
        key = SPACE_RE.sub('_', key)
        key = FIELDS_RE.sub('',  key)
        key = UNDA_RE.sub('_',  key)
        return key

    def _gen_id(self):
        _oid = self.store.get('_oid')
        if self.store['_end']:
            _start = self.store.get('_start')
            # if the object at the exact start/oid is later
            # updated, it's possible to just save(upsert=True)
            _id = ':'.join(map(str, (_oid, _start)))
        else:
            # if the object is 'current value' without _end,
            # use just str of _oid
            _id = unicode(_oid)
        return _id

    def _gen_hash(self):
        o = deepcopy(self.store)
        keys = set(o.iterkeys())
        [o.pop(k) for k in HASH_EXCLUDE_KEYS if k in keys]
        return jsonhash(o)

    def _validate_start_end(self):
        _start = self.get('_start')
        if _start is None:
            raise ValueError("_start (%s) must be set!" % _start)
        _end = self.get('_end')
        if _end and _end < _start:
            raise ValueError(
                "_end (%s) is before _start (%s)!" % (_end, _start))

    def _re_hash(self):
        # object is 'current value' continuous
        # so update _start to reflect the time when
        # object's current state was (re)set
        self._validate_start_end()
        # _id depends on _hash
        # so first, _hash, then _id
        self.store['_hash'] = self._gen_hash()
        self.store['_id'] = self._gen_id()

    def as_dict(self, pop=None):
        store = deepcopy(self.store)
        if pop:
            [store.pop(key, None) for key in pop]
        return store


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
    def __init__(self, objects=None):
        self.store = {}
        if objects is None:
            pass
        elif isinstance(objects, (list, tuple)):
            [self.add(x) for x in objects]
        elif isinstance(objects, (dict, Mapping)):
            self.update(objects)
        elif isinstance(objects, MetriqueContainer):
            self.store = objects
        else:
            raise ValueError(
                "objs must be None, a list, tuple, dict or MetriqueContainer")

    def __getitem__(self, key):
        return dict(self.store[key])

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __contains__(self, item):
        return item in self.store

    def __repr__(self):
        return repr(self.store)

    def _convert(self, item):
        if isinstance(item, MetriqueObject):
            pass
        elif isinstance(item, (Mapping, dict)):
            item = MetriqueObject(**item)
        else:
            raise TypeError(
                "object values must be dict-like; got %s" % type(item))
        return item

    def add(self, item):
        item = self._convert(item)
        _id = item['_id']
        self.store[_id] = item

    def extend(self, items):
        [self.add(i) for i in items]

    def df(self):
        '''Return a pandas dataframe from objects'''
        return pd.DataFrame(tuple(self.store))


class MetriqueFactory(type):
    def __call__(cls, cube=None, name=None, *args, **kwargs):
        name = name or cube
        if cube:
            cls = get_cube(cube=cube, name=name, init=False)
        return type.__call__(cls, name=name, *args, **kwargs)


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
    default_config_file = DEFAULT_CONFIG
    name = None
    config = None
    _objects = None
    __metaclass__ = MetriqueFactory

    def __init__(self, name, config_file=None, config=None,
                 cube_pkgs=None, cube_paths=None, debug=None,
                 log_file=None, log2file=None, log2stdout=None,
                 workers=None, log_dir=None, cache_dir=None,
                 etc_dir=None, tmp_dir=None):
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
        # set default config value as dict (from None set cls level)
        self.default_config_file = config_file or self.default_config_file
        options = dict(cube_pkgs=cube_pkgs,
                       cube_paths=cube_paths,
                       debug=debug,
                       log_file=log_file,
                       log2file=log2file,
                       log2stdout=log2stdout,
                       workers=workers,
                       log_dir=log_dir,
                       cache_dir=cache_dir,
                       etc_dir=etc_dir,
                       tmp_dir=tmp_dir)
        defaults = dict(cube_pkgs=['cubes'],
                        cube_paths=[],
                        debug=None,
                        log_file='metrique.log',
                        log2file=True,
                        log2stdout=False,
                        workers=2,
                        etc_dir=os.environ.get('METRIQUE_ETC', ''),
                        log_dir=os.environ.get('METRIQUE_LOGS', ''),
                        tmp_dir=os.environ.get('METRIQUE_TMP', ''),
                        cache_dir=os.environ.get('METRIQUE_CACHE', ''))

        # if config is passed in, set it, otherwise start
        # with class assigned default or empty dict
        self.config = deepcopy(config) or self.config or {}

        if config:
            # we're going to use the passed in config;
            # make sure we apply any args passed in though
            self.config.setdefault('metrique', {})
            [self.config['metrique'].update({k: v})
             for k, v in options.iteritems() if v is not None]
        else:
            # load defaults + set args passed in
            self.configure('metrique', options, defaults, config_file)

        # cube class defined name
        self._cube = type(self).name

        # set name if passed in, but don't overwrite default if not
        self.name = name or self.name

        # keep logging local to the cube so multiple
        # cubes can independently log without interferring
        # with each others logging.
        self.debug_setup()

        self._objects = MetriqueContainer()

    def _debug_set_level(self, logger, level):
        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level is True:
            logger.setLevel(logging.DEBUG)
        else:
            level = int(level)
            logger.setLevel(level)
        return logger

    def debug_setup(self, logger=None, level=None):
        '''
        Local object instance logger setup.

        Verbosity levels are determined as such::

            if level in [-1, False]:
                logger.setLevel(logging.WARN)
            elif level in [0, None]:
                logger.setLevel(logging.INFO)
            elif level in [True, 1, 2]:
                logger.setLevel(logging.DEBUG)

        If (level == 2) `logging.DEBUG` will be set even for
        the "root logger".

        Configuration options available for customized logger behaivor:
            * debug (bool)
            * log2stdout (bool)
            * log2file (bool)
            * log_file (path)
        '''
        level = level or self.config['metrique'].get('debug')
        log2stdout = self.config['metrique'].get('log2stdout')
        log_format = "%(name)s.%(process)s:%(asctime)s:%(message)s"
        log_format = logging.Formatter(log_format, "%Y%m%dT%H%M%S")

        log2file = self.config['metrique'].get('log2file')
        log_file = self.config['metrique'].get('log_file', '')
        log_dir = self.config['metrique'].get('log_dir', '')
        log_file = os.path.join(log_dir, log_file)

        logger = logger or logging.getLogger('metrique')
        logger.propagate = 0
        logger.handlers = []
        if log2file and log_file:
            hdlr = logging.FileHandler(log_file)
            hdlr.setFormatter(log_format)
            logger.addHandler(hdlr)
        else:
            log2stdout = True
        if log2stdout:
            hdlr = logging.StreamHandler()
            hdlr.setFormatter(log_format)
            logger.addHandler(hdlr)
        logger = self._debug_set_level(logger, level)

    def configure(self, section_key, options, defaults, config_file=None):
        if not section_key:
            raise ValueError("section_key can't be null")
        # load the config options from disk, if path provided
        config_file = config_file or self.default_config_file
        if config_file:
            raw_config = self.load_config(config_file)
            section = raw_config.get(section_key, {})
            if not isinstance(section, dict):
                # convert mergeabledict (anyconfig) to dict of dicts
                section = section.convert_to(section)
            defaults = rupdate(defaults, section)
        # set option to value passed in, if any
        for k, v in options.iteritems():
            v = v if v is not None else defaults[k]
            section[unicode(k)] = v
        self.config.setdefault(section_key, {})
        self.config[section_key] = rupdate(self.config[section_key], section)

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
        config = self.config if copy_config else {}
        config_file = self.default_config_file
        return get_cube(cube=cube, init=init, name=name, config=config,
                        config_file=config_file, **kwargs)

    def git_clone(self, uri, pull=True):
        '''
        Given a git repo, clone (cache) it locally.

        :param uri: git repo uri
        :param pull: whether to pull after cloning (or loading cache)
        '''
        cache_dir = self.config['metrique'].get('cache_dir')
        # make the uri safe for filesystems
        _uri = "".join(x for x in uri if x.isalnum())
        repo_path = os.path.expanduser(os.path.join(cache_dir, _uri))
        if not os.path.exists(repo_path):
            from_cache = False
            logger.info(
                'Locally caching git repo [%s] to [%s]' % (uri, repo_path))
            cmd = 'git clone %s %s' % (uri, repo_path)
            self._sys_call(cmd)
        else:
            from_cache = True
            logger.info(
                'GIT repo loaded from local cache [%s])' % (repo_path))
        if pull and not from_cache:
            os.chdir(repo_path)
            cmd = 'git pull'
            self._sys_call(cmd)
        return repo_path

    @property
    def objects(self):
        return self._objects

    @objects.setter
    def objects(self, value):
        self._objects = MetriqueContainer(value)

    @objects.deleter
    def objects(self):
        # replacing existing container with a new, empty one
        self._objects = MetriqueContainer()

    def load(self, path, filetype=None, as_dict=True, raw=False,
             retries=None, **kwargs):
        '''Load multiple files from various file types automatically.

        Supports glob paths, eg::

            path = 'data/*.csv'

        Filetypes are autodetected by common extension strings.

        Currently supports loadings from:
            * csv (pd.read_csv)
            * json (pd.read_json)

        :param path: path to config json file
        :param filetype: override filetype autodetection
        :param kwargs: additional filetype loader method kwargs
        '''
        # kwargs are for passing ftype load options (csv.delimiter, etc)
        # expect the use of globs; eg, file* might result in fileN (file1,
        # file2, file3), etc
        if not isinstance(path, basestring):
            # assume we're getting a raw dataframe
            df = path
            if not isinstance(df, pd.DataFrame):
                raise ValueError("loading raw values must be DataFrames")
        elif re.match('https?://', path):
            _path, headers = self.urlretrieve(path, retries)
            logger.debug('Saved %s to tmp file: %s' % (path, _path))
            try:
                df = self._load_file(_path, filetype, as_dict=False, **kwargs)
            finally:
                os.remove(_path)
        else:
            path = re.sub('^file://', '', path)
            path = os.path.expanduser(path)
            datasets = glob.glob(os.path.expanduser(path))
            # buid up a single dataframe by concatting
            # all globbed files together
            df = [self._load_file(ds, filetype, as_dict=False, **kwargs)
                  for ds in datasets]
            if df:
                df = pd.concat(df)

        if not hasattr(df, 'empty') or df.empty:
            raise ValueError("not data extracted!")

        if raw:
            return df.to_dict()
        # FIXME: rename to transform
        if as_dict:
            return df.T.to_dict().values()
        else:
            return df

    def _load_file(self, path, filetype, as_dict=True, **kwargs):
        if not filetype:
            # try to get file extension
            filetype = path.split('.')[-1]
        if filetype in ['csv', 'txt']:
            result = self._load_csv(path, as_dict=as_dict, **kwargs)
        elif filetype in ['json']:
            result = self._load_json(path, as_dict=as_dict, **kwargs)
        else:
            raise TypeError("Invalid filetype: %s" % filetype)
        if as_dict:
            return result.T.as_dict.values()
        else:
            return result

    def _load_csv(self, path, as_dict=True, **kwargs):
        # load the file according to filetype
        return pd.read_csv(path, **kwargs)

    def _load_json(self, path, as_dict=True, **kwargs):
        return pd.read_json(path, **kwargs)

    def load_config(self, path):
        return load_config(path)

    @staticmethod
    def _sys_call(self, cmd, sig=None, sig_func=None, quiet=True):
        if not quiet:
            logger.debug(cmd)
        if isinstance(cmd, basestring):
            cmd = re.sub('\s+', ' ', cmd)
            cmd = cmd.strip()
            cmd = shlex.split(cmd)
        if sig and sig_func:
            signal.signal(sig, sig_func)
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        if not quiet:
            logger.debug(output)
        return output

    def urlretrieve(self, uri, saveas=None, retries=3):
        '''urllib.urlretrieve wrapper'''
        retries = int(retries) if retries else 3
        while retries:
            try:
                _path, headers = urllib.urlretrieve(uri, saveas)
            except Exception as e:
                retries -= 1
                logger.warn(
                    'Failed getting %s: %s (retry:%s)' % (
                        uri, e, retries))
                continue
            else:
                break
        return _path, headers
