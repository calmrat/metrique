#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from metrique.server.config import metrique, mongodb
from metrique.server.defaults import METRIQUE_CONF, MONGODB_CONF
from metrique.server.drivers.drivermap import get_cube, drivermap
from metrique.server.etl import save_doc

from metrique.tools.type_cast import type_cast
from metrique.tools.decorators import memo

MAX_WORKERS = 1


class BaseDriver(object):
    '''
    '''
    metrique_config = metrique(METRIQUE_CONF)
    mongodb_config = mongodb(MONGODB_CONF)

    db_timeline_data = mongodb_config.db_timeline_data
    db_timeline_admin = mongodb_config.db_timeline_admin
    db_warehouse_data = mongodb_config.db_warehouse_data
    db_warehouse_admin = mongodb_config.db_warehouse_admin
    c_etl_activity = mongodb_config.c_etl_activity

    def __init__(self, name):
        '''
        '''
        self.name = name
        self.mask = 0
        self.cube = {'fielddefs': {}}
        self.enabled = True

    def __str__(self):
        return self.name

    @property
    @memo
    def fields(self):
        '''
        '''
        fields = {}
        for field in self.cube['fielddefs']:
            if self.get_field_property('enabled', field, True):
                fields[field] = {
                    'help': self.get_field_property('help', field, ''),
                    'type': self.get_field_property('type', field, unicode),
                    'container': self.get_field_property('type', field, False)}
        return fields

    @property
    @memo
    def fieldmap(self):
        '''
        Dictionary of field_id: field_name
        '''
        fieldmap = defaultdict(str)
        for field in self.fields:
            field_id = self.get_field_property('what', field)
            if field_id is not None:
                fieldmap[field_id] = field
        return fieldmap

    # FIXME: split out into get_timeline... and drop the timeline arg...
    def get_collection(self, cube=None, timeline=False, admin=False,
                       name=None):
        if name is None:
            name = self.name
        if cube:
            _d = drivermap[cube]
            collection = _d.get_collection(timeline=timeline, admin=admin)
        else:
            # use the cached timeline/warehouse collections
            # A) if they already exist; if not, create/cache them
            # B) if we're getting data only
            # otherwise, we need to return authorized db collections
            if admin and timeline:
                db = self.db_timeline_admin
            elif admin:
                db = self.db_warehouse_admin
            elif timeline:
                db = self.db_timeline_data
            else:
                db = self.db_warehouse_data
            collection = db[name]

        return collection

    def get_field_property(self, property, field=None, default=None):
        '''
        First try to get the field's fielddef property, if defined
        Then try to get the default property, if defined
        Then return the default for when neither is found
        Or return None, if no default is defined
        '''
        try:
            return self.cube['fielddefs'][field][property]
        except KeyError:
            try:
                return self.cube['defaults'][property]
            except KeyError:
                return default

    def extract_func(self, **kwargs):
        with ProcessPoolExecutor(MAX_WORKERS) as executor:
            future = executor.submit(_extract_func, self.name, **kwargs)
        return future.result()


def _extract_func(cube, field, **kwargs):
    c = get_cube(cube)
    # id_x if None will become ObjectID()
    id_x = c.get_field_property('id_x', field)
    # raw_x if None will become field
    raw_x = c.get_field_property('raw_x', field, field)
    # convert if None will skip convert step
    convert = c.get_field_property('convert', field)
    # _type will be default if not set
    _type = c.get_field_property('type', field)

    saved = 0
    failed = []
    for item in c._reader:
        if not item:
            continue

        try:
            id_ = id_x(item)
        except TypeError:
            id_ = item[id_x]

        try:
            raw = raw_x(item)
        except TypeError:
            raw = item[raw_x]

        tokens = type_cast(raw, _type)
        if convert:
            tokens = convert(tokens)

        saved += save_doc(c.name, field, tokens, id_)
        if not saved:
            failed.append(id_)

    result = {'saved': saved}
    if failed:
        result.update({'failed_ids': failed})
    return result
