#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import bson
import pymongo

from metrique.server.config import mongodb
from metrique.tools.decorators import memo

from metrique.server.defaults import MONGODB_CONF

mongodb_config = mongodb(MONGODB_CONF)


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


@memo
def get_cube_fields(cube):
    db = get_cube(cube)
    m = bson.Code(
        """ function() {
                for (var key in this) { emit(key, null); }
            } """)
    r = """ function(key, stuff) { return null; } """
    try:
        result = db.inline_map_reduce(m, r)
    except pymongo.errors.OperationFailure:
        # Cube doesn't have any documents; ie, no fields
        return []
    else:
        return sorted([d['_id'] for d in result])


@memo
def get_fields(cube, fields=None):
    ''' return back a list of fields found in documents of a given cube '''
    if not fields:
        return []
    cube_fields = set(get_cube_fields(cube))
    if fields == '__all__':
        return cube_fields
    elif fields and isinstance(fields, basestring):
        fields = [s.strip() for s in fields.split(',')]
        if set(fields) <= cube_fields:
            return fields
        else:
            raise ValueError("Invalid field in set: %s" % fields)


@memo
def get_cubes(username=None):
    '''
        Get a list of cubes server exports
        (optionally) filter out cubes user can't 'r' access
    '''
    names = mongodb_config.db_warehouse_data.db.collection_names()
    return [n for n in names if not n.startswith('system')]
