#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
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
from metrique.config import DEFAULT_CONFIG_FILE
from metrique import query_api, user_api, cube_api
from metrique import etl_activity
from metrique.utils import csv2list, json_encode, get_cube


class HTTPClient(object):
    '''
    Base class that other metrique api wrapper sub-classes
    use to call special, shared call of _get (http request)
    '''
    name = None

    user_login = user_api.login
    user_logout = user_api.logout
    user_register = user_api.register
    user_update_passwd = user_api.update_passwd
    user_update_profile = user_api.update_profile
    user_update_properties = user_api.update_properties

    cube_list_all = cube_api.list_all
    cube_stats = cube_api.stats
    cube_list_fields = cube_api.list_cube_fields
    cube_drop = cube_api.drop
    cube_register = cube_api.register
    cube_update_role = cube_api.update_role

    find = query_api.find
    deptree = query_api.deptree
    count = query_api.count
    fetch = query_api.fetch
    distinct = query_api.distinct
    sample = query_api.sample
    aggregate = query_api.aggregate

    activity_import = etl_activity.activity_import
    save_objects = cube_api.save_objects
    remove_objects = cube_api.remove_objects
    index_list = cube_api.list_index
    index = cube_api.ensure_index
    index_drop = cube_api.drop_index

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

    def __init__(self, api_host=None, api_username=None,
                 api_password=None, async=True,
                 force=True, debug=-1, config_file=None,
                 config_dir=None, cube=None,
                 api_auto_login=None,
                 **kwargs):
        self.load_config(config_file, force)
        logging.basicConfig()
        self.logger = logging.getLogger('metrique.%s' % self.__module__)
        self.config.debug = self.logger, debug
        self.config.async = async

        if cube and isinstance(cube, basestring):
            self.set_cube(cube)

        if api_host:
            self.config.api_host = api_host
        if api_username:
            self.config.api_username = api_username
        if api_password:
            self.config.api_password = api_password

        self._load_session()

        if api_auto_login:
            self.config.api_auto_login = api_auto_login
        self._api_auto_login_attempted = False

    def load_config(self, config_file, force=False):
        if not config_file:
            config_file = DEFAULT_CONFIG_FILE
        self.config = Config(config_file=config_file, force=force)

    def set_cube(self, cube):
        # FIXME: what about if we want to load an
        # existing cube module like csvobject?
        # so we have access to .extract() methods, etc
        self.name = cube

    def get_cube(self, cube):
        return get_cube(cube)

    def _kwargs_json(self, **kwargs):
        #return json.dumps(kwargs, default=json_encode,
        #                  ensure_ascii=False,
        #                  encoding="ISO-8859-1")
        try:
            return dict([(k, json.dumps(v, default=json_encode,
                                        ensure_ascii=False))
                        for k, v in kwargs.items()])
        except UnicodeDecodeError:
            pass

        return dict([(k, json.dumps(v, default=json_encode,
                                    ensure_ascii=False,
                                    encoding="ISO-8859-1"))
                    for k, v in kwargs.items()])

    def _load_session(self):
        # load a fresh new session
        self.session = requests.Session()

    def _get_response(self, runner, _url, api_username, api_password,
                      allow_redirects=True):
        try:
            return runner(_url,
                          auth=(api_username, api_password),
                          cookies=self.session.cookies,
                          verify=False,
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
        if not cmd:
            cmd = ''
        if api_url:
            _url = os.path.join(self.config.api_url, cmd)
        else:
            _url = os.path.join(self.config.host_port, cmd)
        return _url

    def _run(self, kind, cmd, api_url=True,
             allow_redirects=True, full_response=False,
             api_username=None, api_password=None,
             **kwargs):
        if not api_username:
            api_username = self.config.api_username
        else:
            # we actually want to pass this to the server
            kwargs['api_username'] = api_username

        if not api_password:
            api_password = self.config.api_password
        else:
            kwargs['api_password'] = api_password

        runner = self._build_runner(kind, kwargs)
        _url = self._build_url(cmd, api_url)

        _response = self._get_response(runner, _url,
                                       api_username, api_password,
                                       allow_redirects)

        _auto = self.config.api_auto_login
        _attempted = self._api_auto_login_attempted

        if _response.status_code in [401, 403] and _auto and not _attempted:
            self._api_auto_login_attempted = True
            # try to login and rerun the request
            self.logger.debug('HTTP 40*: going to try to auto re-log-in')
            self.user_login(api_username,
                            api_password)
            _response = self._get_response(runner, _url,
                                           api_username, api_password,
                                           allow_redirects)

        try:
            _response.raise_for_status()
        except Exception as e:
            m = getattr(e, 'message')
            content = '%s\n%s' % (m, _response.content)
            logger.error(content)
            raise
        else:
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

    def parse_fields(self, fields):
        if not fields:
            return []
        elif fields == '__all__':
            return self.fields
        else:
            fields = set(csv2list(fields))
            cube_fields = set(self.fields.keys())
            err_fields = [f for f in fields if f not in cube_fields]
            if err_fields:
                self.logger.warn(
                    "Skipping invalid fields in set: %s" % (
                        err_fields))
                self.logger.warn('%s\n%s' % (cube_fields, fields))
            return sorted(fields)
