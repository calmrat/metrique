#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique
~~~~~~~~
**data warehouse and information platform**

metrique can be used to bring data from arbitrary sources
into an intuitive, data object collection that supports
transparent historical version snapshotting, advanced
ad-hoc server-side querying, including (mongodb)
aggregations and (mongodb) mapreduce, along with client
and serverside python, ipython, pandas, numpy, matplotlib,
and more.

    >>> from metrique import pyclient
    >>> g = pyclient(cube="gitrepo_commit"")
    >>> g.ping()
    pong
    >>> ids = g.extract(uri='https://github.com/drpoovilleorg/metrique.git')
    >>> q = c.query.find('git_commit', 'author, committer_ts')
    >>> q.groupby(['author']).size().plot(kind='barh')
    >>> <matplotlib.axes.AxesSubplot at 0x6f77ad0>

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
BASIC_FORMAT = "%(name)s:%(message)s"
FILETYPES = {'csv': pd.read_csv,
             'json': pd.read_json}
fields_re = re.compile('[\W]+')
space_re = re.compile('\s+')
unda_re = re.compile('_')


class BaseCube(MutableSequence):
    '''
    list of dicts; default 'cube' container model
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
        self.objects = []

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
    Essentially, a cube is a list of dictionaries.
    '''
    name = None
    ' defaults is frequently overrided in subclasses as a property '
    defaults = None
    ' fields is frequently overrided in subclasses as a property too '
    fields = None
    ' filename of the data when saved to disk '
    saveas = ''
    ' a place to put stuff, temporarily... '
    _cache = None

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
        self.load_config()

        # update config object with any additional kwargs
        for k, v in kwargs.items():
            if v is not None:
                self.config[k] = v

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

####################### pandas/hd5 python interface ################
    def load_files(self, path, filetype=None, **kwargs):
        '''
        cache to hd5 on disk
        '''
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

#################### misc #######################
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

    def load_config(self, config=None):
        ' try to load a config file and handle when its not available '
        if type(config) is type(Config):
            self._config_file = config.config_file
        else:
            config_file = config or self._config_file
            self.config = Config(config_file=config_file)
            self._config_file = config_file

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
    subclass to build metrique cubes.


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

    def __init__(self, cube=None, owner=None, **kwargs):
        super(HTTPClient, self).__init__(**kwargs)
        self.owner = owner or self.config.username
        # load a new requests session; for the cookies.
        self._load_session()
        self._auto_login_attempted = False

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

    def _build_url(self, cmd, api_url):
        ' generic path joininer for http api commands '
        cmd = cmd or ''
        if api_url:
            _url = os.path.join(self.config.api_url, cmd)
        else:
            _url = os.path.join(self.config.host_port, cmd)
        return _url

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
        _auto = self.config.auto_login
        # avoids bug in requests-2.0.1 - pass a dict no RequestsCookieJar
        # eg, see: https://github.com/kennethreitz/requests/issues/1744
        dfc = requests.utils.dict_from_cookiejar
        try:
            _response = runner(_url, auth=(username, password),
                               cookies=dfc(self.session.cookies),
                               verify=self.config.ssl_verify,
                               allow_redirects=allow_redirects,
                               stream=stream)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                'Failed to connect (%s). Try http://? or https://?' % _url)

        _attempted = self._auto_login_attempted
        if _response.status_code in [401, 403] and _auto and not _attempted:
            self._auto_login_attempted = True
            # try to login and rerun the request
            self.logger.debug('HTTP 40[13]: going to try to auto re-log-in')
            self.user_login(username, password)
            _response = self._get_response(runner, _url, username, password,
                                           allow_redirects, stream)
        try:
            _response.raise_for_status()
        except Exception as e:
            content = '%s\n%s\n%s' % (_url, str(e), _response.content)
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
        self.logger.debug('Ping!')
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
        _url = self._build_url(cmd, api_url)

        _response = self._get_response(runner, _url,
                                       username, password,
                                       allow_redirects,
                                       stream)
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
                content = '%s\n%s\n%s' % (_url, m, _response.content)
                self.logger.error(content)
                raise

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
