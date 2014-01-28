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

:copyright: 2013 "Chris Ward" <cward@redhat.com>
:license: GPLv3, see LICENSE for more details
:sources: https://github.com/drpoovilleorg/metrique

.. note::
    example date ranges: 'd', '~d', 'd~', 'd~d'
.. note::
    valid date format: '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
'''

from collections import MutableSequence
from copy import copy
import cPickle
import gc
from functools import partial
import glob
import logging
import os
import pandas as pd
import re
import requests
import simplejson as json
import urllib

from metrique import query_api, user_api, cube_api
from metrique import regtest as regression_test
from metrique.config import Config
from metrique.utils import json_encode, get_cube
from metriqueu.utils import utcnow

# setup default root logger, but remove default StreamHandler (stderr)
# Handlers will be added upon __init__()
logging.basicConfig()
root_logger = logging.getLogger()
[root_logger.removeHandler(hdlr) for hdlr in root_logger.handlers]
BASIC_FORMAT = "%(name)s:%(asctime)s:%(message)s"
FILETYPES = {'csv': pd.read_csv, 'json': pd.read_json}
fields_re = re.compile('[\W]+')
space_re = re.compile('\s+')
unda_re = re.compile('_')


class BaseCube(MutableSequence):
    '''
    Default 'cube' container model. Essentially, this object is made from
    a list of dicts. The underlying object inherits from python's
    MutableSequence collection object (aka, list).

    All objects are expected to contain a `_oid` key value property. This
    property should be unique per individual "object" defined. For example,
    if we are storing logs, we might consider each log line a separate
    "object" since those log lines should never change in the future and give
    each a unique `_oid`. Or if we are storing data about 'meta objects' of
    some sort, say 'github repo issues' for example, we might have objects
    with _oids of '%(username)s_%(reponame)s_%(issuenumber)s'.

    Optionally, objects can contain the following additional meta-properties:
        * _start - datetime when the object state was set
        * _end - datetime when the object state changed to a new state

    Field names (object dict keys) must consist of alphanumeric and underscore
    characters only.

    Field names are normalized automatically:
        * non-alphanumeric characters are removed
        * spaces converted to underscores
        * letters are lowercased

    Property values are normalized to some extent automatically as well:
        * empty strings -> None

    '''
    _objects = []

    def __init__(self, name, objects=None):
        if name:
            self.name = name
        if objects:
            self.objects = objects

    def __delitem__(self, name):
        del self.objects[name]

    def __getitem__(self, name):
        return self.objects[name]

    def __setitem__(self, name, obj):
        self.objects[name] = obj

    def insert(self, name, obj):
        self.objects.insert(name, obj)

    def __len__(self):
        return len(self.objects)

    def __iter__(self):
        return iter(self.objects)

    def __next__(self):
        yield next(self)

    def next(self):
        return self.__next__()

    def __getslice__(self, i, j):
        return self.objects[i:j]

    def __contains__(self, item):
        return item in self.objects

    def __str__(self):
        return str(self.objects)

    def __repr__(self):
        return repr(self.objects)

####################################################################
    @property
    def df(self):
        if self.objects:
            return pd.DataFrame(self.objects)
        else:
            return pd.DataFrame()

    def flush(self):
        del self.objects

    @property
    def objects(self):
        return self._objects

    @objects.setter
    def objects(self, objects):
        # convert from other forms to basic list of dicts
        if objects is None:
            objects = []
        elif isinstance(objects, pd.DataFrame):
            objects = objects.T.to_dict().values()
        elif isinstance(objects, BaseCube):
            objects = objects.objects
        # model check
        if not isinstance(objects, (BaseCube, list, tuple)):
            _t = type(objects)
            raise TypeError("container value must be a list; got %s" % _t)
        if objects:
            if not all([type(o) is dict for o in objects]):
                raise TypeError("object values must be dict")
            if not all([o.get('_oid') is not None for o in objects]):
                raise ValueError("_oid must be defined for all objs")
            self._objects = self._normalize(objects)
        else:
            self._objects = objects

    @objects.deleter
    def objects(self):
        del self._objects
        self._objects = []
        gc.collect()  # be sure we garbage collect any old object refs

    @property
    def oids(self):
        return [o['_oid'] for o in self._objects]

###################### normalization keys/values #################
    def _normalize(self, objects):
        '''
        give all these objects the same _start value (if they
        don't already have one), and more...
        '''
        start = utcnow()
        for i, o in enumerate(objects):
            # normalize fields (alphanumeric characters only, lowercase)
            o = self._obj_fields(o)
            # convert empty strings to None (null)
            o = self._obj_nones(o)
            # add object meta data the metriqued requires be set per object
            o = self._obj_end(o)
            o = self._obj_start(o, start)
            objects[i] = o
        return objects

    def _normalize_fields(self, k):
        k = k.lower()
        k = space_re.sub('_', k)
        k = fields_re.sub('',  k)
        k = unda_re.sub('_',  k)
        return k

    def _obj_fields(self, obj):
        ''' periods and dollar signs are not allowed! '''
        # replace spaces, lowercase keys, remove non-alphanumeric
        # WARNING: only lowers the top level though, at this time!
        return dict((self._normalize_fields(k), v) for k, v in obj.iteritems())

    def _obj_nones(self, obj):
        return dict([(k, None) if v == '' else (k, v) for k, v in obj.items()])

    def _obj_end(self, obj, default=None):
        obj['_end'] = obj.get('_end', default)
        return obj

    def _obj_start(self, obj, default=None):
        _start = obj.get('_start', default)
        obj['_start'] = _start or utcnow()
        return obj


class BaseClient(BaseCube):
    '''
    Low level client API

    Functionality includes methods for loading data from csv and json,
    loading metrique client cubes, config file loading and logging
    setup.

    Additionally, some common operation methods are provided for
    operations such as loading a HTTP uri and determining currently
    configured username.
    '''
    name = None
    # defaults is frequently overrided in subclasses as a property
    defaults = None
    # fields is frequently overrided in subclasses as a property too
    fields = None
    # filename of the data when saved to disk
    saveas = ''
    # a place to put stuff, temporarily...
    _cache = None
    # config properties
    config = {}

    def __new__(cls, *args, **kwargs):
        '''
        Return the specific cube class, if specified. Its
        expected the cube will be available in sys.path.

        If the cube fails to import, just move on.

            >>> import pyclient
            >>> c = pyclient(cube='git_commit')
                <type HTTPClient(...)>
        '''
        if 'cube' in kwargs and kwargs['cube']:
            cls = get_cube(cube=kwargs['cube'], init=False)
        else:
            cls = cls
        return object.__new__(cls)

    def __init__(self, config_file=None, name=None, objects=None, **kwargs):
        # don't assign to {} in class def, define here to avoid
        # multiple pyclient objects linking to a shared dict
        if self.defaults is None:
            self.defaults = {}
        if self.fields is None:
            self.fields = {}
        if self._cache is None:
            self._cache = {}

        self._config_file = config_file or Config.default_config

        # all defaults are loaded, unless specified in
        # metrique_config.json
        self.load_config(**kwargs)

        utc_str = utcnow(as_datetime=True).strftime('%a%b%d%H%m%S')
        # set name if passed in, but don't overwrite default if not
        self.name = name or self.name or utc_str

        self.objects = BaseCube(name=self.name, objects=objects)

        self.config.logdir = os.path.expanduser(self.config.logdir)
        if not os.path.exists(self.config.logdir):
            os.makedirs(self.config.logdir)
        self.config.logfile = os.path.join(self.config.logdir,
                                           self.config.logfile)

        # keep logging local to the cube so multiple
        # cubes can independently log without interferring
        # with each others logging.
        self.debug_set()

####################### data loading api ###################
    def load_files(self, path, filetype=None, **kwargs):
        # kwargs are for passing ftype load options (csv.delimiter, etc)
        # expect the use of globs; eg, file* might result in fileN (file1,
        # file2, file3), etc
        datasets = glob.glob(os.path.expanduser(path))
        for ds in datasets:
            filetype = path.split('.')[-1]
            # buid up a single dataframe by concatting
            # all globbed files together
            self.objects = pd.concat(
                [self.load_file(ds, filetype, **kwargs)
                    for ds in datasets]).T.as_dict().values()
        return self.objects

    def load_file(self, path, filetype, **kwargs):
        if filetype in ['csv', 'txt']:
            return self.load_csv(path, **kwargs)
        elif filetype in ['json']:
            return self.load_json(path, **kwargs)
        else:
            raise TypeError("Invalid filetype: %s" % filetype)

    def load_csv(self, path, **kwargs):
        # load the file according to filetype
        return pd.read_csv(path, **kwargs)

    def load_json(self, path, **kwargs):
        return pd.read_json(path, **kwargs)

#################### misc ##################################
    def debug_set(self, level=None, logstdout=None, logfile=None):
        '''
        if we get a level of 2, we want to apply the
        debug level to all loggers
        '''
        if level is None:
            level = self.config.debug
        if logstdout is None:
            logstdout = self.config.logstdout
        if logfile is None:
            logfile = self.config.logfile

        basic_format = logging.Formatter(BASIC_FORMAT)

        if level == 2:
            self._logger_name = None
            logger = logging.getLogger()
            logger = self._debug_set_level(logger, level)

        self._logger_name = '%s.%s' % ('metrique', self.name)
        logger = logging.getLogger(self._logger_name)
        logger.propagate = 0

        logger.handlers = []  # reset handlers
        if logstdout:
            hdlr = logging.StreamHandler()
            hdlr.setFormatter(basic_format)
            logger.addHandler(hdlr)

        if self.config.log2file and logfile:
            hdlr = logging.FileHandler(logfile)
            hdlr.setFormatter(basic_format)
            logger.addHandler(hdlr)

        logger = self._debug_set_level(logger, level)
        self.logger = logger

    def _debug_set_level(self, logger, level):
        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)
        return logger

    def get_cube(self, cube, init=True, name=None, **kwargs):
        ' wrapper for utils.get_cube(); try to load a cube, pyclient '
        config = copy(self.config)
        # don't apply the name to the current obj, but to the object
        # we get back from get_cube
        return get_cube(cube=cube, init=init, config=config,
                        name=name, **kwargs)

    def load_config(self, config=None, **kwargs):
        ' try to load a config file and handle when its not available '
        if type(config) is type(Config):
            self._config_file = config.config_file
        else:
            self._config_file = config or self._config_file
            self.config = Config(config_file=self._config_file)
        self.config.update(kwargs)

#################### Helper API ############################
    def urlretrieve(self, uri, saveas):
        return urllib.urlretrieve(uri, saveas)

    def whoami(self, auth=False):
        ' quick way of checking the username the instance is working as '
        return self.config['username']


class HTTPClient(BaseClient):
    '''
    This is the main client bindings for metrique http
    rest api.

    The is a base class that clients are expected to
    subclass to build metrique cubes which are designed
    to interact with remote metriqued hosts.

    Currently, the following API methods are exported:

    **User**
        + aboutme: request user profile information
        + login: main interface for authenticating against metriqued
        + passwd: update user password
        + update_profile: update other profile details
        + register: register a new user account
        + remove: (admin) remove an existing user account
        + set_properties: (admin) set non-profile (system) user properties

    **Cube**
        + list_all: list all remote cubes current user has read access to
        + stats: provide statistics and other information about a remote cube
        + sample_fields: sample remote cube object fields names
        + drop: drop (delete) a remote cube
        + export: return back a complete export of a given remote cube
        + register: register a new remote cube
        + update_role: update remote cube access control details
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

    **Regtest**
        + regtest: run a regression test
        + create: create a regression test
        + remove: remove a regression test
        + list: list available regression tests
    '''
    # frequently 'typed' commands have shorter aliases too
    user_aboutme = aboutme = user_api.aboutme
    user_login = login = user_api.login
    user_logout = logout = user_api.logout
    user_passwd = passwd = user_api.update_passwd
    user_update_profile = user_api.update_profile
    user_register = user_api.register
    user_remove = user_api.remove
    user_set_properties = user_api.update_properties

    cube_list_all = cube_api.list_all
    cube_stats = cube_api.stats
    cube_sample_fields = cube_api.sample_fields
    cube_drop = cube_api.drop
    cube_export = cube_api.export
    cube_register = cube_api.register
    cube_update_role = cube_api.update_role

    cube_save = cube_api.save
    cube_rename = cube_api.rename
    cube_remove = cube_api.remove
    cube_index_list = cube_api.list_index
    cube_index = cube_api.ensure_index
    cube_index_drop = cube_api.drop_index

    query_find = find = query_api.find
    query_history = history = query_api.history
    query_deptree = deptree = query_api.deptree
    query_count = count = query_api.count
    query_distinct = distinct = query_api.distinct
    query_sample = sample = query_api.sample
    query_aggregate = aggregate = query_api.aggregate

    regtest = regression_test.regtest
    regtest_create = regression_test.regtest_create
    regtest_remove = regression_test.regtest_remove
    regtest_list = regression_test.regtest_list

    def __init__(self, owner=None, login=None, cube_register=None, **kwargs):
        super(HTTPClient, self).__init__(**kwargs)
        self.owner = owner or self.config.username
        # load a new requests session; for the cookies.
        self._load_session()

        # FIXME: move all the setup below here into _load_session()
        # and in load_session, first run 'logout' etc
        self.logged_in = False

        cube_autoregister = cube_register or self.config.cube_autoregister

        if login is None:
            # login is needed if cube_autoregister is true
            login = self.config.auto_login or cube_autoregister

        if login:
            self.user_login(self.config.username, self.config.password)

        if self.logged_in and cube_autoregister:
            if not self.cube_id in self.cube_list_all():
                self.logger.info("Autoregistering %s" % self.name)
                self.cube_register()

    @property
    def cube_id(self):
        return '__'.join((self.owner, self.name))

    def _build_runner(self, kind, kwargs):
        ''' generic caller for HTTP
            A) POST; use data, not params
            B) otherwise; use params
        '''
        kwargs_json = self._kwargs_json(**kwargs)
        if kind == self.session.post:
            # use data instead of params
            runner = partial(kind, data=kwargs_json)
        else:
            runner = partial(kind, params=kwargs_json)
        return runner

    def _build_urls(self, cmd, api_url):
        ' generic path joininer for http api commands '
        cmd = cmd or ''
        join = os.path.join
        if api_url:
            urls = [join(api_uri, cmd) for api_uri in self.config.api_uris]
        else:
            urls = [join(uri, cmd) for uri in self.config.uris]
        return urls

    def cookiejar_clear(self):
        path = '%s.%s' % (self.config.cookiejar, self.config.username)
        if os.path.exists(path):
            os.remove(path)

    def cookiejar_load(self):
        path = '%s.%s' % (self.config.cookiejar, self.config.username)
        cfd = requests.utils.cookiejar_from_dict
        if os.path.exists(path):
            try:
                with open(path) as cj:
                    cookiejar = cfd(cPickle.load(cj))
            except Exception:
                pass
            else:
                self.session.cookies = cookiejar

    def cookiejar_save(self):
        path = '%s.%s' % (self.config.cookiejar, self.config.username)
        dfc = requests.utils.dict_from_cookiejar
        with open(path, 'w') as f:
            cPickle.dump(dfc(self.session.cookies), f)

    def _delete(self, *args, **kwargs):
        ' requests DELETE; using current session '
        return self._run(self.session.delete, *args, **kwargs)

    def _get(self, *args, **kwargs):
        ' requests GET; using current session '
        return self._run(self.session.get, *args, **kwargs)

    def get_objects(**kwargs):
        raise NotImplementedError

    def extract(self, *args, **kwargs):
        self.get_objects(*args, **kwargs)  # default: stores into self.objects
        self.cube_save()  # default: saves from self.objects
        return

    def get_cmd(self, owner, cube, api_name=None):
        '''
        another helper for building api urls, specifically
        for the case where the api call always requires
        owner and cube; api_name is usually provided,
        if there is a 'command name'; but it's optional.
        '''
        owner = owner or self.owner
        if not owner:
            raise ValueError('owner required!')
        cube = cube or self.name
        if not cube:
            raise ValueError('cube required!')
        if api_name:
            return os.path.join(owner, cube, api_name)
        else:
            return os.path.join(owner, cube)

    def get_last_field(self, field):
        '''
        shortcut for querying to get the last field value for
        a given owner, cube.
        '''
        # FIXME: these "get_*" methods are assuming owner/cube
        # are "None" defaults; ie, that the current instance
        # has self.name set... maybe we should be explicit?
        # pass owner, cube?
        last = self.find(query=None, fields=[field],
                         sort=[(field, -1)], one=True, raw=True)
        if last:
            last = last.get(field)
        self.logger.debug(
            "last %s.%s: %s" % (self.name, field, last))
        return last

    def get_property(self, property, field=None, default=None):
        '''
        First try to use the field's property, if defined
        Then try to use the default property, if defined
        Then use the default for when neither is found
        Or None, if no default is defined

        OBSOLETE: use metriqueu.utils.set_default
        '''
        try:
            return self.fields[field][property]
        except KeyError:
            try:
                return self.defaults[property]
            except (TypeError, KeyError):
                return default

    def _get_response(self, runner, _url, username, password,
                      allow_redirects=True, stream=False):
        ' wrapper for running a metrique api request; get/post/etc '
        # avoids bug in requests-2.0.1 - pass a dict no RequestsCookieJar
        # eg, see: https://github.com/kennethreitz/requests/issues/1744
        dfc = requests.utils.dict_from_cookiejar
        _response = runner(_url, auth=(username, password),
                           cookies=dfc(self.session.cookies),
                           verify=self.config.ssl_verify,
                           allow_redirects=allow_redirects,
                           stream=stream)

        self.session.cookies = _response.cookies
        self.cookiejar_save()

        try:
            _response.raise_for_status()
        except Exception as e:
            content = _response.content
            code = _response.status_code
            content = '[%s] %s\n%s\n%s' % (code, _url, str(e), content)
            self.logger.error(content)
            raise
        return _response

    def _kwargs_json(self, **kwargs):
        ' encode all arguments/parameters as JSON '
        return dict([(k,
                      json.dumps(v, default=json_encode, ensure_ascii=False))
                    for k, v in kwargs.items()])

    def _load_session(self):
        ' load a fresh new requests session; mainly, reset cookies '
        self.session = requests.Session()
        self.cookiejar_load()

    def ping(self, auth=False):
        '''
        global...base api call; all metrique servers will be expected
        to have this method available. auth=True is a quick way to
        test clients credentials.
        '''
        return self._get('ping', auth=auth)

    def _post(self, *args, **kwargs):
        ' requests POST; using current session '
        return self._run(self.session.post, *args, **kwargs)

    def _run(self, kind, cmd, api_url=True,
             allow_redirects=True, full_response=False,
             stream=False, filename=None, **kwargs):
        '''
        wrapper for handling all requests; authentication,
        preparing arguments, calling request, handling
        exceptions, returning results.
        '''
        username = self.config.username
        password = self.config.password

        runner = self._build_runner(kind, kwargs)

        urls = self._build_urls(cmd, api_url)
        for url in urls:
            try:
                _response = self._get_response(runner, url,
                                               username, password,
                                               allow_redirects,
                                               stream)
            except requests.exceptions.ConnectionError:
                self.logger.error("Failed to connect to %s" % url)
                # try the next url available
                continue
            else:
                self.logger.debug("Got response from %s" % url)

            if full_response:
                return _response
            elif stream:
                with open(filename, 'wb') as handle:
                    for block in _response.iter_content(1024):
                        if not block:
                            break
                        handle.write(block)
                return filename
            else:
                try:
                    return json.loads(_response.content)
                except Exception as e:
                    m = getattr(e, 'message')
                    content = '%s\n%s\n%s' % (url, m, _response.content)
                    self.logger.error(content)
                    raise
        else:
            msg = 'Failed to connect to metriqued hosts [%s]' % urls
            raise requests.exceptions.ConnectionError(msg)

    def _save(self, filename, *args, **kwargs):
        ' requests GET of a "file stream" using current session '
        return self._run(self.session.get, stream=True, filename=filename,
                         *args, **kwargs)

    def whoami(self, auth=False):
        ' quick way of checking the username the instance is working as '
        if auth:
            self.user_login()
        else:
            super(HTTPClient, self).whoami()


# import alias
# ATTENTION: this is the main interface for clients!
pyclient = HTTPClient
