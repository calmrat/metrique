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
import pickle

from metrique.config import Config
from metrique.config import DEFAULT_CONFIG_DIR, DEFAULT_CONFIG_FILE
from metrique import query_api, etl_api, user_api, cube_api
from metrique import etl_activity, get_cube

from metrique.utils import csv2list, json_encode


class HTTPClient(object):
    '''
    Base class that other metrique api wrapper sub-classes
    use to call special, shared call of _get (http request)
    '''
    name = None

    find = query_api.find
    deptree = query_api.deptree
    count = query_api.count
    fetch = query_api.fetch
    distinct = query_api.distinct
    sample = query_api.sample
    aggregate = query_api.aggregate
    activity_import = etl_activity.activity_import
    save_objects = etl_api.save_objects
    remove_objects = etl_api.remove_objects
    list_index = cube_api.list_index
    ensure_index = cube_api.ensure_index
    drop_index = cube_api.drop_index
    cube_drop = cube_api.drop
    cube_register = cube_api.register
    user_login = user_api.login
    user_logout = user_api.logout
    user_add = user_api.add
    user_register = user_api.register
    user_passwd = user_api.passwd

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
        self.load_config(config_file, config_dir, force)
        logging.basicConfig()
        self.logger = logging.getLogger('metrique.%s' % self.__module__)
        self.config.debug = self.logger, debug
        self.config.async = async

        if cube and not self.name and isinstance(cube, basestring):
            self.name = cube

        if api_host:
            self.config.api_host = api_host
        if api_username:
            self.config.api_username = api_username
        if api_password:
            self.config.api_password = api_password

        self._load_session()

        if api_auto_login:
            self.config.api_auto_login = api_auto_login

    def load_config(self, config_file, config_dir, force=False):
        if not config_file:
            config_file = DEFAULT_CONFIG_FILE
        if not config_dir:
            config_dir = DEFAULT_CONFIG_DIR
        self.config = Config(config_file, config_dir, force)

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

    #def _save_session(self):
    #    if not self.session:
    #        return None

    #    session_path = os.path.join(self.config.config_dir,
    #                                self.config.session_file)

    #    # FIXME: WARNING THIS WILL OVERWRITE EXISTING FILE!
    #    with open(session_path, 'w') as f:
    #        pickle.dump(self.session, f)

    def _load_session(self):
        # try to load a session from disk
        session_path = os.path.join(self.config.config_dir,
                                    self.config.session_file)

        if os.path.exists(session_path):
            with open(session_path) as f:
                self.session = pickle.load(f)
                return

        # finally, fall back to loading a fresh new session
        self.session = requests.Session()

    def _get_response(self, runner, _url, api_username, api_password):
        try:
            return runner(_url,
                          auth=(api_username, api_password),
                          cookies=self.session.cookies,
                          verify=False)
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
        if api_url:
            _url = os.path.join(self.config.api_url, cmd)
        else:
            _url = os.path.join(self.config.host_port, cmd)
        return _url

    def _run(self, kind, cmd, api_url=True,
             api_username=None, api_password=None,
             **kwargs):
        if not api_username:
            api_username = self.config.api_username
        if not api_password:
            api_password = self.config.api_password

        runner = self._build_runner(kind, kwargs)
        _url = self._build_url(cmd, api_url)

        _response = self._get_response(runner, _url,
                                       api_username, api_password)

        if _response.status_code in [401, 403] and self.config.api_auto_login:
            # try to login and rerun the request
            self.logger.debug('HTTP 401: going to try to auto re-log-in')
            #self._load_session()
            self.user_login(api_username,
                            api_password)
            _response = self._get_response(runner, _url,
                                           api_username, api_password)

        #self._save_session()

        try:
            _response.raise_for_status()
        except Exception as e:
            m = getattr(e, 'message')
            content = '%s\n%s' % (m, _response.content)
            logger.error(content)
            raise
        else:
            return json.loads(_response.content)

    def _get(self, *args, **kwargs):
        return self._run(self.session.get, *args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._run(self.session.post, *args, **kwargs)

    def _delete(self, *args, **kwargs):
        return self._run(self.session.delete, *args, **kwargs)

    def ping(self):
        return self._get('ping')

    def list_cubes(self):
        ''' List all valid cubes for a given metrique instance '''
        return self._get('cube')

    def list_cube_fields(self, cube=None,
                         exclude_fields=None, _mtime=False):
        '''
        List all valid fields for a given cube

        :param string cube:
            Name of the cube you want to query
        :param list exclude_fields:
            List (or csv) of fields to exclude from the results
        :param bool mtime:
            Include mtime details
        '''
        if not cube:
            cube = self.name
        return self._get('cube', cube=cube,
                         exclude_fields=exclude_fields,
                         _mtime=_mtime)

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
