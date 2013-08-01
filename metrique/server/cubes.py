#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from metrique.server.config import mongodb
from metrique.server.defaults import MONGODB_CONF
from metrique.tools import csv2list

mongodb_config = mongodb(MONGODB_CONF)
ETL_ACTIVITY = mongodb_config.c_etl_activity


def get_cube(cube, admin=True, timeline=False):
    ''' return back a mongodb connection to give cube collection '''
    if timeline:
        if admin:
            return mongodb_config.db_timeline_admin.db[cube]
        else:
            return mongodb_config.db_timeline_data.db[cube]
    else:
        if admin:
            return mongodb_config.db_warehouse_admin.db[cube]
        else:
            return mongodb_config.db_warehouse_data.db[cube]


def get_etl_activity():
    return ETL_ACTIVITY


def strip_split(item):
    if isinstance(item, basestring):
        return [s.strip() for s in item.split(',')]
    else:
        return item


def get_fields(cube, fields=None, check=False):
    ''' return back a list of known fields in documents of a given cube '''
    _fields = {}
    if fields:
        cube_fields = list_cube_fields(cube)
        if fields == '__all__':
            _fields = cube_fields
        else:
            fields = strip_split(fields)
            if not check:
                _fields = fields
            else:
                _fields = []
                for field in fields:
                    if field not in cube_fields:
                        raise ValueError('Invalid field: %s' % field)
                    else:
                        _fields.append(field)
    return _fields


def list_cube_fields(cube, exclude_fields=[]):
    cube_fields = ETL_ACTIVITY.find_one({'_id': cube}, {'_id': False})
    if not cube_fields:
        cube_fields = {}
    exclude_fields = csv2list(exclude_fields)
    for f in exclude_fields:
        try:
            del cube_fields[f]
        except KeyError:
            # just ignore any invalid fields
            pass
    return cube_fields


def list_cubes():
    '''
        Get a list of cubes server exports
        (optionally) filter out cubes user can't 'r' access
    '''
    names = mongodb_config.db_warehouse_data.db.collection_names()
    return [n for n in names if not n.startswith('system')]
