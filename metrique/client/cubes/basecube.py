#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from collections import defaultdict

from metrique.client.http_api import HTTPClient
from metrique.tools.decorators import memo


class BaseCube(HTTPClient):
    defaults = {}
    fields = {}

    def __init__(self):
        super(BaseCube, self).__init__()

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
