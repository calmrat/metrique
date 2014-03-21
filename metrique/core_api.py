#!/usr/bin/env python
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
    >>> g = pyclient(cube="osinfo_rpm"")
    >>> g.get_objects()  # get information about all installed RPMs
    >>> 'Total RPMs: %s' % len(objects)
    >>> 'Example Object:', objects[0]
        {'_oid': 'dhcp129-66.brq.redhat.com__libreoffice-ure-4.1.4.2[...]',
         '_start': 1390619596.0,
         'arch': 'x86_64',
         'host': 'dhcp129-66.brq.redhat.com',
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
    >>> # connect to metriqued host to save the objects
    >>> config_file = '~/.metrique/etc/metrique.json'  # default location
    >>> m = pyclient(config_file=config_file)
    >>> osinfo_rpm = m.get_cube('osinfo_rpm')
    >>> osinfo_rpm.cube_register()  # (run once) register the new cube with the
    >>> ids = osinfo_rpm.extract()  # alias for get_objects + save_objects
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

from collections import Mapping, MutableMapping
from copy import copy
import glob
import logging
import os
import pandas as pd
import re
import urllib

from metrique.config import Config
from metrique.utils import get_cube, utcnow, jsonhash, dt2ts

logger = logging.getLogger(__name__)

FIELDS_RE = re.compile('[\W]+')
SPACE_RE = re.compile('\s+')
UNDA_RE = re.compile('_')

HASH_EXCLUDE_KEYS = ['_hash', '_id', '_start', '_end']
IMMUTABLE_OBJ_KEYS = set(['_hash', '_id', '_oid'])
TIMESTAMP_OBJ_KEYS = set(['_end', '_start'])


class MetriqueObject(Mapping):
    def __init__(self, _oid, strict=False, touch=False, **kwargs):
        self._strict = strict
        self._touch = touch
        self.store = {
            '_oid': _oid,
            '_id': None,
            '_hash': None,
            '_start': None,
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
                    return
            if key in TIMESTAMP_OBJ_KEYS:
                # ensure normalized timestamp
                value = dt2ts(value)
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
            _id = str(_oid)
        return _id

    def _gen_hash(self):
        o = copy(self.store)
        keys = set(o.keys())
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
        if not self.store.get('_start') or self._touch:
            self.store['_start'] = utcnow()
        self._validate_start_end()
        # _id depends on _hash
        # so first, _hash, then _id
        self.store['_hash'] = self._gen_hash()
        self.store['_id'] = self._gen_id()

    def as_dict(self, pop=None):
        store = copy(self.store)
        if pop:
            [store.pop(key, None) for key in pop]
        return store


class MetriqueContainer(MutableMapping):
    def __init__(self, objects=None):
        self.store = {}
        if objects is None:
            pass
        elif isinstance(objects, (list, tuple)):
            [self.add(x) for x in objects]
        elif isinstance(objects, (dict, Mapping)):
            # FIXME: should this be self.update(objects)?
            self.store.update(objects)
        elif isinstance(objects, MetriqueContainer):
            self.store = objects
        else:
            raise ValueError(
                "objects must be a list, tuple, dict or pandas.DataFrame")

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


class BaseClient(object):
    '''
    Low level client API which provides baseline functionality, including
    methods for loading data from csv and json, loading metrique client
    cubes, config file loading and logging setup.

    Essentially, cubes are data made from a list of dicts.

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

    Additionally, some common operation methods are provided for
    operations such as loading a HTTP uri and determining currently
    configured username.

    :cvar name: name of the cube
    :cvar defaults: cube default property container (cube specific meta-data)
    :cvar fields: cube fields definitions
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
    name = None
    defaults = None
    fields = None
    config = None
    _objects = None

    def __new__(cls, *args, **kwargs):
        if 'cube' in kwargs and kwargs['cube']:
            cls = get_cube(cube=kwargs['cube'], init=False)
        else:
            cls = cls
        return object.__new__(cls)

    def __init__(self, config_file=None, name=None, **kwargs):
        # don't assign to {} in class def, define here to avoid
        # multiple pyclient objects linking to a shared dict
        if self.defaults is None:
            self.defaults = {}
        if self.fields is None:
            self.fields = {}
        if self.config is None:
            self.config = {}

        self._config_file = config_file or Config.default_config

        # all defaults are loaded, unless specified in
        # metrique_config.json
        self.set_config(**kwargs)

        # cube class defined name
        self._cube = type(self).name

        # set name if passed in, but don't overwrite default if not
        self.name = name or self.name

        # keep logging local to the cube so multiple
        # cubes can independently log without interferring
        # with each others logging.
        self.debug_setup()

        self._objects = MetriqueContainer()

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

####################### data loading api ###################
    def load(self, path, filetype=None, as_dict=True, raw=False, **kwargs):
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
        if re.match('https?://', path):
            _path, headers = self.urlretrieve(path)
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

        if df.empty:
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

    def urlretrieve(self, uri, saveas=None):
        '''urllib.urlretrieve wrapper'''
        return urllib.urlretrieve(uri, saveas)

#################### misc ##################################
    def debug_setup(self):
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
            * logstdout (bool)
            * log2file (bool)
            * logfile (path)
        '''
        level = self.config.debug
        logstdout = self.config.logstdout
        log_format = "%(name)s.%(process)s:%(asctime)s:%(message)s"
        log_format = logging.Formatter(log_format, "%Y%m%dT%H%M%S")

        logfile = self.config.logfile
        logdir = os.environ.get("METRIQUE_LOGS")
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        logfile = os.path.join(logdir, logfile)

        logger = logging.getLogger()
        logger.handlers = []
        if logstdout:
            hdlr = logging.StreamHandler()
            hdlr.setFormatter(log_format)
            logger.addHandler(hdlr)
        if self.config.log2file and logfile:
            hdlr = logging.FileHandler(logfile)
            hdlr.setFormatter(log_format)
            logger.addHandler(hdlr)
        self._debug_set_level(logger, level)

    def _debug_set_level(self, logger, level):
        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)
        return logger

    def get_cube(self, cube, init=True, name=None, **kwargs):
        '''wrapper for :func:`metrique.utils.get_cube`

        Locates and loads a metrique cube

        :param cube: name of cube to load
        :param init: (bool) initialize cube before returning?
        :param name: override the name of the cube
        :param kwargs: additional :func:`metrique.utils.get_cube`
        '''
        config = copy(self.config)
        # don't apply the name to the current obj, but to the object
        # we get back from get_cube
        return get_cube(cube=cube, init=init, config=config,
                        name=name, **kwargs)

    def get_property(self, property, field=None, default=None):
        '''Lookup cube defined property (meta-data):

            1. First try to use the field's property, if defined.
            2. Then try to use the default property, if defined.
            3. Then use the default for when neither is found.
            4. Or return None, if no default is defined.

        :param property: property key name
        :param field: (optional) specific field to query first
        :param default: default value to return if [field.]property not found
        '''
        try:
            return self.fields[field][property]
        except KeyError:
            try:
                return self.defaults[property]
            except (TypeError, KeyError):
                return default

    def set_config(self, config=None, **kwargs):
        '''Try to load a config file and handle when its not available

        :param config: config file or :class:`metrique.jsonconf.JSONConf`
        :param kwargs: additional config key:value pairs to store
        '''
        if type(config) is type(Config):
            self._config_file = config.config_file
        else:
            self._config_file = config or self._config_file
            self.config = Config(config_file=self._config_file)
        self.config.update(kwargs)
