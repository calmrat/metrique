#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from metrique.server.config import mongodb
from metrique.tools import csv2list
from metrique.tools.decorators import memo

mongodb_config = mongodb()


@memo
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
    return mongodb_config.c_etl_activity


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
            _fields = cube_fields.keys()
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


def list_cube_fields(cube, exclude_fields=[], _mtime=False):
    spec = {'_id': cube}
    _filter = {'_id': 0}
    if not _mtime:
        _filter.update({'_mtime': 0})
    cube_fields = get_etl_activity().find_one(spec, _filter)
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
