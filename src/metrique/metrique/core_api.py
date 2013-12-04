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
from copy import copy
import cPickle
from functools import partial
import glob
import logging
import os
import pandas as pd
import requests
import simplejson as json
import urllib

from metrique import query_api, user_api, cube_api
from metrique.config import Config
from metrique.utils import json_encode, get_cube
from metriqueu.utils import jsonhash, utcnow

# setup default root logger, but remove default StreamHandler (stderr)
# Handlers will be added upon __init__()
logging.basicConfig()
root_logger = logging.getLogger()
[root_logger.removeHandler(hdlr) for hdlr in root_logger.handlers]
BASIC_FORMAT = "%(name)s:%(message)s"
FILETYPES = {'csv': pd.read_csv,
             'json': pd.read_json}


# BaseClient should have a base-extract (default: in-memory)
# which HTTPClient (etc) should call if the client's config
# indicates 'class_client': 'HTTP' or 'H5' or 'Base'
class BaseClient(object):
    '''
    Essentially, a cube is a list of dictionaries.
    '''
    name = 'NO_NAME'
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

        # set name if passed in, but don't overwrite default if not
        if name is not None:
            self.name = name
        if not self.name:
            raise RuntimeError("cube requires a name")

        # a path to 'journal' file, hdf5 or a dataframe or list of dicts
        if objects:
            # set objects if passed in, but don't overwrite default if not
            self.objects = objects

        self.config.logdir = os.path.expanduser(self.config.logdir)
        if not os.path.exists(self.config.logdir):
            os.makedirs(self.config.logdir)
        self.config.logfile = os.path.join(self.config.logdir,
                                           self.config.logfile)

        # keep logging local to the cube so multiple
        # cubes can independently log without interferring
        # with each others logging.
        self.debug_set()

#################### special cube object handlers #######################
    def __getitem__(self, name):
        return self.objects[name]

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

    def oids(self):
        raise NotImplementedError

    def keys(self):
        return self.objects.keys()

    def items(self):
        return self.objects.items()

    def __str__(self):
        return str(self.objects)

    def __repr__(self):
        return repr(self.objects)

####################### pandas/hd5 python interface ################
    @property
    def df(self):
        return pd.DataFrame(self.objects)

    def load_files(self, path, filetype=None, **kwargs):
        '''
        cache to hd5 on disk
        '''
        # kwargs are for passing ftype load options (csv.delimiter, etc)
        # expect the use of globs; eg, file* might result in fileN (file1,
        # file2, file3), etc
        datasets = glob.glob(os.path.expanduser(path))
        for ds in datasets:
            if os.path.exists(ds):
                filetype = path.split('.')[-1]
                # buid up a single dataframe by concatting
                # all globbed files together
                self.objects = pd.concat(
                    [self._load_file(ds, filetype, **kwargs)
                     for ds in datasets])
            else:
                self.objects = pd.DataFrame()
        return self.objects

    def _load_file(self, path, filetype, **kwargs):
        if filetype in ['csv', 'txt']:
            return self._load_csv(path, **kwargs)
        elif filetype in ['json']:
            return self._load_json(path, **kwargs)
        else:
            raise TypeError("Invalid filetype: %s" % filetype)

    def _load_csv(self, path, **kwargs):
        # load the file into hdf5, according to filetype
        return pd.read_csv(path, **kwargs)

    def _load_json(self, path, **kwargs):
        return pd.read_json(path, **kwargs)

    @property
    def hdf5(self):
        if not self.objects:
            return None
        elif self._cache.get('sync_required', True):
            # FIXME: use hd5py created group (self.name)
            # FIXME: use hd5py to create_dataset()
            path = self.hdf5_path + '.hd5'
            store = pd.HDFStore(path)
            # objects group is pandas dataframe
            fmt = '%a%b%d%H%m%S'
            utcnow_str = utcnow(as_datetime=True).strftime(fmt)
            store['objects/%s' % utcnow_str] = self.df
            self._cache['hdf5_store'] = store
            self._cache['sync_required'] = False
        else:
            store = self._cache.get('hdf5_store')
        return store

    @property
    def hdf5_path(self):
        filename = self.name
        return os.path.join(self.config.hdf5_dir, filename)

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

    def _obj_hash(self, obj):
        o = copy(obj)
        if '_hash' in obj:
            del o['_hash']
        if '_start' in obj:
            del o['_start']
        if '_end' in obj:
            del o['_end']
        if '_id' in obj:
            del o['_id']
        obj['_hash'] = jsonhash(o)
        return obj

    def _obj_end(self, obj, end=None):
        end = end if end else obj.get('_end')
        obj['_end'] = end
        return obj

    def _obj_start(self, obj, start=None):
        start = start if start else obj.get('_start', utcnow())
        obj['_start'] = start
        return obj

    def _obj_id(self, obj):
        obj['_id'] = jsonhash(obj)
        return obj

    def _obj_apply(self, objects, func, **kwargs):
        func = partial(func, **kwargs)
        return map(func, objects)

    @property
    def objects(self):
        ''' always return a list of dicts '''
        return self._cache.get('objects', [])

    @objects.setter
    def objects(self, value):
        self._cache.setdefault('objects', [])
        if isinstance(value, pd.HDFStore):
            value = value['objects']
        if isinstance(value, pd.DataFrame):
            df = value
        else:
            df = pd.DataFrame(value)
        start = utcnow()
        # transpose dataframe's axies before converting to dict
        objects = df.T.to_dict().values()

        objects = self._obj_apply(objects, self._obj_end)
        objects = self._obj_apply(objects, self._obj_start, start=start)
        objects = self._obj_apply(objects, self._obj_hash)
        objects = self._obj_apply(objects, self._obj_id)
        try:
            hashes = set([o['_hash'] for o in self.objects
                          if o['_end'] is None])
        except Exception as e:
            self.logger.error('EXCEPTION: %s' % str(e))
            hashes = set()
        for o in objects:
            if o['_hash'] not in hashes:
                self._cache['objects'].append(o)
        # dependencies of objects should refresh
        self._cache['sync_required'] = True
        #self.hdf5  # sync the hdf5 file
        return self._cache['objects']

    @property
    def objectsi(self):
        return iter(self.objects)

    def save_uri(self, uri, saveas):
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

    cube_activity_import = cube_api.activity_import
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

    def __init__(self, cube=None, owner=None, **kwargs):
        super(HTTPClient, self).__init__(**kwargs)
        self.owner = owner or self.config.username
        # load a new requests session; for the cookies.
        self._load_session()
        self._auto_login_attempted = False

#################### HTTP specific API methods/overrides ###############
    def activity_get(self, ids=None):
        '''
        Returns a dictionary of `id: [(when, field, removed, added)]` kv pairs
        that represent the activity history for the particular ids.
        '''
        raise NotImplementedError(
            'The activity_get method is not implemented in this cube.')

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

    def extract(self, **kwargs):
        self.get_objects(**kwargs)
        return self.cube_save(self.objects)

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
            m = getattr(e, 'message')
            content = '%s\n%s\n%s' % (_url, m, _response.content)
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
