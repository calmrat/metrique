#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from collections import defaultdict

from metrique.client.http_api import HTTPClient
from metrique.client.decorators import memo


class BaseCube(HTTPClient):
    name = '__UNKNOWN__'
    defaults = {}
    fields = {}
    result = None
    _result_class = None

    def __init__(self, cube=None, **kwargs):
        if cube:
            self.name = cube
        super(BaseCube, self).__init__(**kwargs)

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

    @property
    @memo
    def fieldmap(self):
        '''
        Dictionary of field_id: field_name
        '''
        fieldmap = defaultdict(str)
        for field in self.fields:
            field_id = self.get_property('what', field)
            if field_id is not None:
                fieldmap[field_id] = field
        return fieldmap

    def setdefault(self, value, default, config_key=None):
        ''' config helper. Set a cube property value
            based on config, a given default or the
            value provided itself.
        '''
        if value is None:
            try:
                return self.config[config_key]
            except (KeyError, AttributeError, TypeError):
                return default
        else:
            return value

    def get_last_oid(self, field=None):
        '''
        Query metrique for the last known object id (_oid)
        in a given cube.

        If a field is specified, find the mtime for
        the given cube.field if there are actually
        documents in the cube with the given field.
        '''
        self.logger.debug(
            "Get last ID: cube(%s) field(%s)" % (self.name, field))
        if field:
            q = "%s == exists(True)" % field
        else:
            q = "_start == exists(True)"
        last_oid = self.find(q, fields=[], sort=[('_oid', -1)],
                             one=True, raw=True)
        if last_oid:
            last_oid = last_oid.get('_oid')
        self.logger.info(" ... Last ID: %s" % last_oid)
        return last_oid

    def activity_get(self, ids=None, mtime=None):
        '''
        Generator that yields by ids ascending.
        Each yield has format: (id, [(when, field, removed, added)])
        For fields that are containers, added and removed must be lists
            (no None allowed)
        '''
        raise NotImplementedError(
            'The activity_get method is not implemented in this cube.')
