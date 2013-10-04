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
    >>> q = c.query.fetch('git_commit', 'author, committer_ts')
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


import logging
logger = logging.getLogger(__name__)
from functools import partial
import os
import requests
import simplejson as json

from metrique import query_api, user_api, cube_api
from metrique.config import Config
from metrique.utils import json_encode, get_cube


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
    cube_register = cube_api.register
    cube_update_role = cube_api.update_role

    cube_activity_import = cube_api.activity_import
    cube_save = cube_api.save
    cube_remove = cube_api.remove
    cube_index_list = cube_api.list_index
    cube_index = cube_api.ensure_index
    cube_index_drop = cube_api.drop_index

    query_find = find = query_api.find
    query_deptree = deptree = query_api.deptree
    query_count = count = query_api.count
    query_fetch = fetch = query_api.fetch
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
            try:
                cube_cls = get_cube(kwargs['cube'])
            except ImportError:
                cube_cls = cls
        else:
            cube_cls = cls
        return object.__new__(cube_cls)

    def __init__(self, host=None, port=None, username=None,
                 password=None, async=True, debug=0, logfile=None,
                 config_file=None, cube=None, auto_login=None):
        self._config_file = config_file
        '''
        all defaults are loaded, unless specified in
        metrique_config.json
        '''
        self.load_config()
        if logfile:
            'override logfile, if path specified'
            self.config.logfile = logfile
        '''
        keep logging local to the cube so multiple
        cubes can independently log without interferring
        with each others logging.
        '''
        logging.basicConfig()
        self.logger = logging.getLogger('metrique.%s' % self.__module__)
        self.config.debug = self.logger, debug
        ' async == False disabled prepare().@gen.coroutine() tornado async '
        self.config.async = async

        if isinstance(cube, basestring):
            self.set_cube(cube)
        elif cube:
            raise TypeError(
                "expected cube as a string, got %s" % type(cube))

        if host:
            self.config.host = host
        if port:
            self.config.port = port
        if username:
            self.config.username = username
        if password:
            self.config.password = password

        ' we load a new requests session; mainly for the cookies. '
        self._load_session()

        if auto_login:
            self.config.auto_login = auto_login
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

    @property
    def current_user(self):
        ' alias for whoami(); returns back username in config.username '
        return self.whoami()

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
        owner = owner or self.config.username
        if not owner:
            raise ValueError('owner required!')
        cube = cube or self.name
        if not cube:
            raise ValueError('cube required!')
        if api_name:
            return os.path.join(owner, cube, api_name)
        else:
            return os.path.join(owner, cube)

    def get_cube(self, cube):
        ' wrapper for utils.get_cube(); try to load a cube, pyclient '
        return get_cube(cube)

    def get_last_field(self, field):
        '''
        shortcut for querying to get the last field value for
        a given owner, cube.
        '''
        # FIXME: make sure it hits the baseindex
        query = None
        last = self.find(query, fields=[field],
                         sort=[(field, -1)], one=True, raw=True)
        if last:
            last = last.get(field)
        self.logger.debug(
            "last %s.%s: %s" % (self.name, field, last))
        return last

    def get_last_oid(self):
        ' get the last known object id (_oid) in a given cube '
        return self.get_last_field('_oid')

    def get_last_start(self):
        ' get the last known object start (_start) in a given cube '
        # FIXME: these "get_*" methods are assuming owner/cube
        # are "None" defaults; ie, that the current instance
        # has self.name set... maybe we should be explicit?
        # pass owner, cube?
        return self.get_last_field('_start')

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
                      allow_redirects=True):
        ' wrapper for running a metrique api request; get/post/etc '
        try:
            return runner(_url,
                          auth=(username, password),
                          cookies=self.session.cookies,
                          verify=self.config.ssl_verify,
                          allow_redirects=allow_redirects)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                'Failed to connect (%s). Try http://? or https://?' % _url)

    def _kwargs_json(self, **kwargs):
        ' encode all arguments/parameters as JSON '
        return dict([(k, json.dumps(v, default=json_encode,
                                    ensure_ascii=False,
                                    encoding="ISO-8859-1"))
                    for k, v in kwargs.items()])

    def load_config(self, config_file=None):
        ' try to load a config file and handle when its not available '
        config_file = config_file or self._config_file
        try:
            self.config = Config(config_file=config_file)
        except Exception:
            logger.error("failed to load config: %s" % config_file)
            raise
        else:
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
        return self._get('ping', auth=auth)

    def _post(self, *args, **kwargs):
        ' requests POST; using current session '
        return self._run(self.session.post, *args, **kwargs)

    def _run(self, kind, cmd, api_url=True,
             allow_redirects=True, full_response=False,
             username=None, password=None,
             **kwargs):
        '''
        wrapper for handling all requests; authentication,
        preparing arguments, calling request, handling
        exceptions, returning results.
        '''
        if not username:
            username = self.config.username
        else:
            # we actually want to pass this to the server
            kwargs['username'] = username

        if not password:
            password = self.config.password
        else:
            kwargs['password'] = password

        runner = self._build_runner(kind, kwargs)
        _url = self._build_url(cmd, api_url)

        _response = self._get_response(runner, _url,
                                       username, password,
                                       allow_redirects)

        _auto = self.config.auto_login
        _attempted = self._auto_login_attempted

        if _response.status_code in [401, 403] and _auto and not _attempted:
            self._auto_login_attempted = True
            # try to login and rerun the request
            self.logger.debug('HTTP 40*: going to try to auto re-log-in')
            self.user_login(username, password)
            _response = self._get_response(runner, _url,
                                           username, password,
                                           allow_redirects)

        try:
            _response.raise_for_status()
        except Exception as e:
            m = getattr(e, 'message')
            content = '%s\n%s' % (m, _response.content)
            logger.error(content)
            raise
        else:
            # reset autologin flag since we've logged in successfully
            self._auto_login_attempted = False
            if full_response:
                return _response
            else:
                return json.loads(_response.content)

    def set_cube(self, cube):
        '''
        give this instance a "cube name"; the cube name is expected
        to exist already in the metrique host being interacted with,
        or the cube needs to be registered.
        '''
        self.name = cube

    def whoami(self, auth=False):
        ' quick way of checking the username the instance is working as '
        if auth:
            self.user_login()
        else:
            return self.config['username']


# import alias
# ATTENTION: this is the main interface for clients!
pyclient = HTTPClient
