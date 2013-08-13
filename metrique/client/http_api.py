#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import os
import requests as rq
import simplejson as json

from metrique.client.config import Config
from metrique.client import query_api, etl_api, users_api
from metrique.client import etl_activity

from metrique.tools import csv2list
from metrique.tools.defaults import DEFAULT_CONFIG_FILE
from metrique.tools.json import json_encode, decoder

# FIXME: IDEAS
# commands should return back an object immediately which
# runs the command and sets obj.result when complete
# fetch results could be an iterator? fetching only X items at a time


class HTTPClient(object):
    '''
    Base class that other metrique api wrapper sub-classes
    use to call special, shared call of _get (http request)
    '''
    find = query_api.find
    count = query_api.count
    fetch = query_api.fetch
    distinct = query_api.distinct
    aggregate = query_api.aggregate
    list_index = etl_api.list_index
    ensure_index = etl_api.ensure_index
    drop_index = etl_api.drop_index
    snapshot = etl_api.snapshot
    activity_import = etl_activity.activity_import
    save_objects = etl_api.save_objects
    cube_drop = etl_api.cube_drop
    user_add = users_api.add

    def __init__(self, host=None, username=None, password=None,
                 async=True, force=False, debug=1,
                 config_file=None, config_dir=None,
                 **kwargs):

        self.load_config(config_file, config_dir, force)
        self.config.debug = debug
        self.config.async = async

        if host:
            self.config.api_host = host
        if username:
            self.config.api_username = username
        if password:
            self.config.api_password = password

    def load_config(self, config_file, config_dir=None, force=False):
        if not config_file:
            config_file = DEFAULT_CONFIG_FILE
        self.config = Config(config_file, config_dir, force)

    def _kwargs_json(self, **kwargs):
        try:
            return dict([(k, json.dumps(v,
                                        default=json_encode,
                                        ensure_ascii=False))
                        for k, v in kwargs.items()])
        except UnicodeDecodeError:
            pass

        return dict([(k, json.dumps(v, default=json_encode, ensure_ascii=False,
                                    encoding="ISO-8859-1"))
                    for k, v in kwargs.items()])

    def _args_url(self, *args):
        _url = os.path.join(self.config.api_url, *args)
        logger.debug("URL: %s" % _url)
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
        logger.warn('response arrived')
        # responses are always expected to be json encoded
        return json.loads(_response.text, object_hook=decoder)

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
                                  auth=(username, password), verify=False)
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
        ''' List all valid fields for a given cube

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        exclude_fields : str or list
            List (or csv) of fields to exclude from the results
        mtime : bool
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
                logger.warn(
                    "Skipping invalid fields in set: %s" % (
                        err_fields))
                logger.warn('%s\n%s' % (cube_fields, fields))
            return sorted(fields)
