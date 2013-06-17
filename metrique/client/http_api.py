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

from metrique.tools import csv2list
from metrique.tools.json import Encoder

CONFIG_FILE = 'http_api'

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
    aggregate = query_api.aggregate
    index_warehouse = etl_api.index_warehouse
    snapshot = etl_api.snapshot
    activity_import = etl_api.activity_import
    save_objects = etl_api.save_objects
    add_user = users_api.add

    def __init__(self, host=None, username=None, password=None,
                 async=True, force=False, debug=False,
                 config_file=None, config_dir=None, **kwargs):
        if not config_file:
            base_config_file = CONFIG_FILE
        base_config_dir = config_dir
        self.baseconfig = Config(base_config_file, base_config_dir, force=force)

        self.baseconfig.debug = debug
        self.baseconfig.async = async

        if host:
            self.baseconfig.api_host = host

        if username:
            self.baseconfig.api_username = username

        if password:
            self.baseconfig.api_password = password

    def _kwargs_json(self, **kwargs):
        return dict([(k, json.dumps(v, cls=Encoder, ensure_ascii=False))
                    for k, v in kwargs.items()])

    def _args_url(self, *args):
        _url = os.path.join(self.baseconfig.api_url, *args)
        logger.debug("URL: %s" % _url)
        return _url

    def _get(self, *args, **kwargs):
        kwargs_json = self._kwargs_json(**kwargs)
        _url = self._args_url(*args)

        username = self.baseconfig.api_username
        password = self.baseconfig.api_password

        try:
            _response = rq.get(_url, params=kwargs_json, auth=(username, password), verify=False)
        except rq.exceptions.ConnectionError:
            raise rq.exceptions.ConnectionError(
                'Failed to connect (%s). Try https://?' % _url)
        _response.raise_for_status()
        # responses are always expected to be json encoded
        return json.loads(_response.text)

    def _post(self, *args, **kwargs):
        '''
            Arguments are expected to be json encoded!
            verify = False in requests.get() skips SSL CA validation
        '''
        kwargs_json = self._kwargs_json(**kwargs)
        _url = self._args_url(*args)

        username = self.baseconfig.api_username
        password = self.baseconfig.api_password

        try:
            _response = rq.post(_url, data=kwargs_json, auth=(username, password), verify=False)
        except rq.exceptions.ConnectionError:
            raise rq.exceptions.ConnectionError(
                'Failed to connect (%s). Try https://?' % _url)
        _response.raise_for_status()
        # responses are always expected to be json encoded
        return json.loads(_response.text)

    def ping(self):
        return self._get('ping')

    @property
    def get_cubes(self):
        ''' List all valid cubes for a given metrique instance '''
        return self._get('cubes')

    def get_fields(self, cube=None, details=False):
        ''' List all valid fields for a given cube

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        details : bool
            return back dict of additional cube.field metadata
        '''
        if not cube:
            cube = self.cube
        return self._get('cubes', cube=cube, details=details)

    def parse_fields(self, fields):
        if not fields:
            return []
        elif fields == '__all__':
            return self.fields

        fields = set(csv2list(fields))
        if fields <= set(self.fields):
            return fields
        else:
            raise ValueError(
                "Invalid field in set: %s" % (set(self.fields) - fields))
