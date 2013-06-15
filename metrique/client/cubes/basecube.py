#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from collections import defaultdict

from metrique.client.http_api import HTTPClient
from metrique.tools.decorators import memo


class BaseCube(HTTPClient):
    name = '__UNKNOWN__'
    defaults = {}
    fields = {}

    def __init__(self, cube=None, **kwargs):
        if cube:
            self.name = cube
        super(BaseCube, self).__init__(**kwargs)

    @memo
    def get_property(self, property, field=None, default=None):
        '''
        First try to get the field's fielddef property, if defined
        Then try to get the default property, if defined
        Then return the default for when neither is found
        Or return None, if no default is defined
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

    def setdefault(self, value, default):
        if value is None:
            return default
        else:
            return value

    def last_known_warehouse_mtime(self, field=None, value=None):
        '''get the last known warehouse object mtime'''
        start = None
        if field:
            # we need to check the etl_activity collection
            if value:
                spec = {'cube': self.name, field: value}
                raise NotImplementedError()
                # FIXME: THIS IS BORKED
                doc = self.c_etl_activity.find_one(spec, ['%s.mtime' % field])
            else:
                spec = {'cube': self.name, field: {'$exists': True}}
                doc = self.find_one(spec, ['%s._mtime' % field])
            if doc:
                start = doc[field]['mtime']
        else:
            # get the most recent _mtime of all objects in the cube
            mtime = '_mtime'
            spec = {}
            doc = self.find_one(spec, [mtime], sort=[(mtime, -1)])
            if doc:
                start = doc[mtime]

        logger.debug('... Last field mtime: %s' % start)
        return start

    def get_last_id(self, field=None):
        '''
        '''
        logger.debug("Get last ID (%s): %s" % (self.name, field))
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
                last_id = last_id[0]
        logger.debug(" ... Last ID: %s" % last_id)
        return last_id
