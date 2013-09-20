#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique
~~~~~~~~
**Python/MongoDB Data Warehouse and Information Platform**

Metrique can be used to bring data into an intuitive,
indexable data object collection that supports
transparent historical version snapshotting,
advanced ad-hoc server-side querying, including (mongodb)
aggregations and (mongodb) mapreduce, along with python,
ipython, pandas, numpy, matplotlib, and so on, is well
integrated with the scientific python computing stack.

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

from metrique.config import Config
from metrique.config import CONFIG_FILE
from metrique import query_api, user_api, cube_api
from metrique.utils import json_encode, get_cube


class HTTPClient(object):
    '''
    Base class that other metrique api wrapper sub-classes
    use to call special, shared call of _get (http request)
    '''
    name = None
    defaults = {}
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
        Return the specific cube class, if specified
        '''
        if 'cube' in kwargs and kwargs['cube']:
            try:
                cube_cls = get_cube(kwargs['cube'])
            except ImportError:
                cube_cls = cls
        else:
            cube_cls = cls
        return object.__new__(cube_cls)

    def __init__(self, host=None, username=None,
                 password=None, async=True,
                 force=True, debug=0, config_file=None,
                 cube=None, auto_login=None,
                 **kwargs):
        self._config_file = config_file or CONFIG_FILE
        self.load_config(force=force)
        logging.basicConfig()
        self.logger = logging.getLogger('metrique.%s' % self.__module__)
        self.config.debug = self.logger, debug
        self.config.async = async

        if isinstance(cube, basestring):
            self.set_cube(cube)
        elif cube:
            raise TypeError(
                "expected cube as a string, got %s" % type(cube))

        if host:
            self.config.host = host
        if username:
            self.config.username = username
        if password:
            self.config.password = password

        self._load_session()

        if auto_login:
            self.config.auto_login = auto_login
        self._auto_login_attempted = False

    def load_config(self, config_file=None, force=False):
        config_file = config_file or self._config_file
        try:
            self.config = Config(config_file=config_file, force=force)
        except Exception:
            logger.error("failed to load config: %s" % config_file)
            raise
        else:
            self._config_file = config_file

    def set_cube(self, cube):
        self.name = cube

    def get_cube(self, cube):
        return get_cube(cube)

    def get_last_oid(self):
        '''
        Query metrique for the last known object id (_oid)
        in a given cube.

        If a field is specified, find the mtime for
        the given cube.field if there are actually
        documents in the cube with the given field.
        '''
        # FIXME: use ifind
        self.logger.debug(
            "Get last ID: cube(%s)" % self.name)
        query = None
        last_oid = self.find(query, fields=['_oid'],
                             sort=[('_oid', -1)], one=True, raw=True)
        if last_oid:
            last_oid = last_oid.get('_oid')
        self.logger.info(" ... Last ID: %s" % last_oid)
        return last_oid

    def get_property(self, property, field=None, default=None):
        '''
        First try to use the field's property, if defined
        Then try to use the default property, if defined
        Then use the default for when neither is found
        Or None, if no default is defined
        '''
        try:
            return self.fields[field][property]
        except KeyError:
            try:
                return self.defaults[property]
            except (TypeError, KeyError):
                return default

    def _kwargs_json(self, **kwargs):
        return dict([(k, json.dumps(v, default=json_encode,
                                    ensure_ascii=False,
                                    encoding="ISO-8859-1"))
                    for k, v in kwargs.items()])

    def _load_session(self):
        # load a fresh new session
        self.session = requests.Session()

    def _get_response(self, runner, _url, username, password,
                      allow_redirects=True):
        try:
            return runner(_url,
                          auth=(username, password),
                          cookies=self.session.cookies,
                          verify=self.config.ssl_verify,
                          allow_redirects=allow_redirects)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                'Failed to connect (%s). Try http://? or https://?' % _url)

    def _build_runner(self, kind, kwargs):
        kwargs_json = self._kwargs_json(**kwargs)
        if kind == self.session.post:
            # use data instead of params
            runner = partial(kind, data=kwargs_json)
        else:
            runner = partial(kind, params=kwargs_json)
        return runner

    def _build_url(self, cmd, api_url):
        cmd = cmd or ''
        if api_url:
            _url = os.path.join(self.config.api_url, cmd)
        else:
            _url = os.path.join(self.config.host_port, cmd)
        return _url

    def _run(self, kind, cmd, api_url=True,
             allow_redirects=True, full_response=False,
             username=None, password=None,
             **kwargs):
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

    def _get(self, *args, **kwargs):
        return self._run(self.session.get, *args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._run(self.session.post, *args, **kwargs)

    def _delete(self, *args, **kwargs):
        return self._run(self.session.delete, *args, **kwargs)

    def ping(self, auth=False):
        return self._get('ping', auth=auth)

    @property
    def current_user(self):
        # alias for whoami(); returns back username in config.username
        return self.whoami()

    def whoami(self, auth=False):
        if auth:
            self.user_login()
        else:
            return self.config['username']

    def get_cmd(self, owner, cube, api_name):
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
