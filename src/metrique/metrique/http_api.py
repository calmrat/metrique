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
import os
import requests as rq
import simplejson as json

from metrique.config import Config
from metrique.config import DEFAULT_CONFIG_DIR, DEFAULT_CONFIG_FILE
from metrique import query_api, etl_api, users_api
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
    list_index = etl_api.list_index
    ensure_index = etl_api.ensure_index
    drop_index = etl_api.drop_index
    activity_import = etl_activity.activity_import
    save_objects = etl_api.save_objects
    remove_objects = etl_api.remove_objects
    cube_drop = etl_api.cube_drop
    user_add = users_api.add

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

    def load_config(self, config_file, config_dir, force=False):
        if not config_file:
            config_file = DEFAULT_CONFIG_FILE
        if not config_dir:
            config_dir = DEFAULT_CONFIG_DIR
        self.config = Config(config_file, config_dir, force)

    def _kwargs_json(self, **kwargs):
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

    def _args_url(self, *args):
        _url = os.path.join(self.config.api_url, *args)
        self.logger.debug("URL: %s" % _url)
        return _url

    def _get(self, *args, **kwargs):
        kwargs_json = self._kwargs_json(**kwargs)
        _url = self._args_url(*args)

        username = self.config.api_username
        password = self.config.api_password

        try:
            _response = rq.get(_url, params=kwargs_json,
                               auth=(username, password), verify=False)
        except rq.exceptions.ConnectionError:
            raise rq.exceptions.ConnectionError(
                'Failed to connect (%s). Try https://?' % _url)
        _response.raise_for_status()
        return json.loads(_response.text)

    def _post(self, *args, **kwargs):
        '''
        Arguments are expected to be json encoded!
        verify = False in requests.get() skips SSL CA validation
        '''
        kwargs_json = self._kwargs_json(**kwargs)
        _url = self._args_url(*args)

        username = self.config.api_username
        password = self.config.api_password

        try:
            _response = rq.post(_url, data=kwargs_json,
                                auth=(username, password), verify=False)
        except rq.exceptions.ConnectionError:
            raise rq.exceptions.ConnectionError(
                'Failed to connect (%s). Try https://?' % _url)
        _response.raise_for_status()
        # responses are always expected to be json encoded
        return json.loads(_response.text)

    def _delete(self, *args, **kwargs):
        kwargs_json = self._kwargs_json(**kwargs)
        _url = self._args_url(*args)

        username = self.config.api_username
        password = self.config.api_password

        try:
            _response = rq.delete(_url, params=kwargs_json,
                                  auth=(username, password),
                                  verify=False)
        except rq.exceptions.ConnectionError:
            raise rq.exceptions.ConnectionError(
                'Failed to connect (%s). Try https://?' % _url)
        _response.raise_for_status()
        # responses are always expected to be json encoded
        return json.loads(_response.text)

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
