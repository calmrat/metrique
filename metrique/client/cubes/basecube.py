#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)


from metrique.client.http_api import HTTPClient


class BaseCube(HTTPClient):
    name = '__UNKNOWN__'
    defaults = {}
    fields = {}
    result = None

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

    def last_mtime(self, cube=None, field=None):
        '''get the last known warehouse object mtime'''
        start = None
        q = '%s == exists(True)' % field
        doc = self.find(q, fields='_mtime', one=True, raw=True)
        if doc:
            start = doc['_mtime']
        logger.debug('... Last object mtime: %s' % start)
        return start

    def get_last_id(self, field=None):
        '''
        Query metrique for the last known object id (_id, _oid)
        in a given cube.

        If a field is specified, find the mtime for
        the given cube.field if there are actually
        documents in the cube with the given field.
        '''
        logger.debug(
            "Get last ID: cube(%s) field(%s)" % (self.name, field))
        if field:
            last_id = self.find("%s == exists(True)" % field,
                                fields=[], sort=[('_id', -1)],
                                one=True, raw=True)
            if last_id:
                last_id = last_id.get('_id')
        else:
            last_id = self.fetch(sort=[('_id', -1)], fields=[],
                                 limit=1, raw=True)
            if last_id:
                last_id = last_id[0]['_id']
        logger.debug(" ... Last ID: %s" % last_id)
        return last_id

    def activity_get(self, ids=None, mtime=None):
        '''
        Generator that yields by ids ascending.
        Each yield has format: (id, [(when, field, removed, added)])
        For fields that are containers, added and removed must be lists
            (no None allowed)
        '''
        raise NotImplementedError(
            'The activity_get method is not implemented in this cube.')
