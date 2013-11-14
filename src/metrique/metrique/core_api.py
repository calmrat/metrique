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
import logging
from functools import partial
import os
import requests
import simplejson as json
import urllib

from metrique import query_api, user_api, cube_api
from metrique.config import Config
from metrique.utils import json_encode, get_cube

# setup default root logger, but remove default StreamHandler (stderr)
# Handlers will be added upon __init__()
logging.basicConfig()
root_logger = logging.getLogger()
[root_logger.removeHandler(hdlr) for hdlr in root_logger.handlers]
BASIC_FORMAT = "%(name)s:%(message)s"


class HTTPClient(object):
    '''
    This is the main client bindings for metrique http
    rest api.

    The is a base class that clients are expected to
    subclass to build metrique cubes.


    '''
    name = None
    ' defaults is frequently overrided in subclasses as a property '
    defaults = {}
    ' fields is frequently overrided in subclasses as a property too '
    fields = {}

    # frequently 'typed' commands have shorter aliases too
    user_aboutme = aboutme = user_api.aboutme
    user_login = login = user_api.login
    user_logout = logout = user_api.logout
    user_passwd = passwd = user_api.update_passwd
    user_update_profile = user_api.update_profile
    user_register = user_api.register
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

    def __init__(self, cube=None, config_file=None, owner=None,
                 name=None, **kwargs):
        self._config_file = config_file
        # all defaults are loaded, unless specified in
        # metrique_config.json
        self.load_config()

        # update config object with any additional kwargs
        for k, v in kwargs.items():
            if v is not None:
                self.config[k] = v

        self.owner = owner or self.config.username

        if name:
            # override the cube name
            self.name = name

        self.config.logdir = os.path.expanduser(self.config.logdir)
        if not os.path.exists(self.config.logdir):
            os.makedirs(self.config.logdir)
        self.config.logfile = os.path.join(self.config.logdir,
                                           self.config.logfile)

        # keep logging local to the cube so multiple
        # cubes can independently log without interferring
        # with each others logging.
        self.debug_set()

        # load a new requests session; for the cookies.
        self._load_session()
        self._auto_login_attempted = False
        self._cache = {}

    def __getitem__(self, name):
        if isinstance(name, slice):
            op = 'in'
            one = False
        else:
            op = '=='
            one = True

        name_json = json.dumps(name, default=json_encode, ensure_ascii=False)
        result = self.find('_oid %s %s' % (op, name_json),
                           fields='__all__',
                           raw=True, one=one)
        if not result:
            if op == 'in':
                result = []
            else:
                result = {}
        return result

    def __setitem__(self, name, value):
        raise NotImplementedError

    def __delitem__(self, name):
        raise NotImplementedError

    def __len__(self):
        return self.count()

    def __iter__(self):
        return self

    def insert(self, objs):
        self.save(objs)

    def extend(self, objs):
        self.save(objs)

    def next(self):
        count = self._cache.get('counter', None)
        if count is None:
            k = self._cache['count'] = self.count()
            count = self._cache['counter'] = 0
            self._cache['sort'] = self.config.sort
        else:
            k = self._cache['count']
        if count < k:
            docs = self.find(sort=[('_oid', self._cache['sort'])],
                             skip=count,
                             limit=self.config.batch_size,
                             raw=True,
                             fields='__all__')
            self._cache['counter'] += len(docs)
            # FIXME: return back a generator rather than tuple?
            return docs
        else:
            del self._cache['counter']
            del self._cache['count']
            del self._cache['sort']
            raise StopIteration

    def __getslice__(self, i, j):
        return self.find(sort=[('_oid', self.config.sort)],
                         raw=True, fields='__all__',
                         skip=i, limit=j)

    def __contains__(self, item):
        return bool(self.count('_oid == %s' % item))

    def keys(self):
        i = 0
        size = self.config.batch_size
        for j in xrange(size, self.count(), size):
            objs = self[i:j]
            for o in objs:
                yield o['_oid']
                i = j
        else:
            objs = self[i:self.count()]
            for o in objs:
                yield o['_oid']

    def values(self):
        i = 0
        size = self.config.batch_size
        for j in xrange(size, self.count(), size):
            objs = self[i:j]
            for o in objs:
                yield o
                i = j
        else:
            objs = self[i:self.count()]
            for o in objs:
                yield o

    def items(self):
        i = 0
        size = self.config.batch_size
        for j in xrange(size, self.count(), size):
            objs = self[i:j]
            for o in objs:
                yield o['_oid'], o
                i = j
        else:
            objs = self[i:self.count()]
            for o in objs:
                yield o['_oid'], o

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

    @property
    def current_user(self):
        ' alias for whoami(); returns back username in config.username '
        return self.whoami()

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
        elif not self.name:
            self._logger_name = __name__
            logger = logging.getLogger(self._logger_name)
            logger.propagate = 0
        else:
            self._logger_name = '%s.%s' % (__name__, self.name)
            logger = logging.getLogger(self._logger_name)
            logger.propagate = 0

        # reset handlers
        logger.handlers = []

        if logstdout:
            hdlr = logging.StreamHandler()
            hdlr.setFormatter(basic_format)
            logger.addHandler(hdlr)

        if self.config.log2file and logfile:
            hdlr = logging.FileHandler(logfile)
            hdlr.setFormatter(basic_format)
            logger.addHandler(hdlr)

        if level in [-1, False]:
            logger.setLevel(logging.WARN)
        elif level in [0, None]:
            logger.setLevel(logging.INFO)
        elif level in [True, 1, 2]:
            logger.setLevel(logging.DEBUG)
        self.logger = logger

    def _delete(self, *args, **kwargs):
        ' requests DELETE; using current session '
        return self._run(self.session.delete, *args, **kwargs)

    def _get(self, *args, **kwargs):
        ' requests GET; using current session '
        return self._run(self.session.get, *args, **kwargs)

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

    def get_cube(self, cube, init=True, **kwargs):
        ' wrapper for utils.get_cube(); try to load a cube, pyclient '
        config = copy(self.config)
        # don't apply the name to the current obj, but to the object
        # we get back from get_cube
        name = kwargs.get('name')
        if name:
            del kwargs['name']
        return get_cube(cube=cube, init=init, config=config,
                        name=name, **kwargs)

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
        _attempted = self._auto_login_attempted
        try:
            _response = runner(_url, auth=(username, password),
                               cookies=self.session.cookies,
                               verify=self.config.ssl_verify,
                               allow_redirects=allow_redirects,
                               stream=stream)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                'Failed to connect (%s). Try http://? or https://?' % _url)

        if _response.status_code in [401, 403] and _auto and not _attempted:
            self._auto_login_attempted = True
            # try to login and rerun the request
            self.logger.debug('HTTP 40*: going to try to auto re-log-in')
            self.user_login(username, password)
            _response = self._get_response(runner, _url, username, password,
                                           allow_redirects, stream)
        try:
            _response.raise_for_status()
            # reset autologin flag since we've logged in successfully
            self._auto_login_attempted = False
        except Exception as e:
            m = getattr(e, 'message')
            content = '%s\n%s' % (m, _response.content)
            self.logger.error(content)
            raise
        return _response

    def _kwargs_json(self, **kwargs):
        ' encode all arguments/parameters as JSON '
        return dict([(k,
                      json.dumps(v, default=json_encode, ensure_ascii=False))
                    for k, v in kwargs.items()])

    def load_config(self, config=None):
        ' try to load a config file and handle when its not available '
        if type(config) is type(Config):
            self._config_file = config.config_file
        else:
            config_file = config or self._config_file
            self.config = Config(config_file=config_file)
            self._config_file = config_file

    def _load_session(self):
        ' load a fresh new requests session; mainly, reset cookies '
        self.session = requests.Session()

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
                content = '%s\n%s' % (m, _response.content)
                self.logger.error(content)
                raise

    def save_uri(self, uri, saveas):
        return urllib.urlretrieve(uri, saveas)

    def _save(self, filename, *args, **kwargs):
        ' requests GET of a "file stream" using current session '
        return self._run(self.session.get, stream=True, filename=filename,
                         *args, **kwargs)

    def whoami(self, auth=False):
        ' quick way of checking the username the instance is working as '
        if auth:
            self.user_login()
        else:
            return self.config['username']


# import alias
# ATTENTION: this is the main interface for clients!
pyclient = HTTPClient
