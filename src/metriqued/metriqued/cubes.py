#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from tornado.web import HTTPError

from metriqued.config import mongodb

mongodb_config = mongodb()


def get_collection(owner, cube, admin=False, create=False):
    ''' return back a mongodb connection to give cube collection '''
    collection = '%s__%s' % (owner, cube)
    if collection in mongodb_config.db_timeline_data.db.collection_names():
        if create:
            raise HTTPError(
                409, "collection already exists: %s" % collection)
    else:
        if not create:
            raise HTTPError(
                412, "collection does not exist: %s" % collection)
    if admin:
        return mongodb_config.db_timeline_admin.db[collection]
    else:
        return mongodb_config.db_timeline_data.db[collection]

# FIXME: remove this when all get_cube calls refactored
get_cube = get_collection  # backwards compatability


def get_auth_keys():
    return mongodb_config.c_auth_keys


def get_etl_activity():
    return mongodb_config.c_etl_activity
ETL_ACTIVITY = get_etl_activity()


def strip_split(item):
    if isinstance(item, basestring):
        return [s.strip() for s in item.split(',')]
    else:
        return item


def get_fields(owner, cube, fields=None):
    '''
    Return back a dict of (field, 0/1) pairs, where the matching fields have 1.
    '''
    logger.debug('... fields: %s' % fields)
    _fields = []
    if fields:
        cube_fields = list_cube_fields(owner, cube)
        if fields == '__all__':
            _fields = cube_fields.keys()
        else:
            _fields = [f for f in strip_split(fields) if f in cube_fields]
    _fields += ['_oid', '_start', '_end']
    _fields = dict([(f, 1) for f in set(_fields)])

    # If `_id` should not be in returned it must have 0 otherwise mongo will
    # return it.
    if '_id' not in _fields:
        _fields['_id'] = 0
    logger.debug('... matched fields (%s)' % _fields)
    return _fields


# FIXME: this will need to become more like field_struct
# since we expect nested docs
def list_cube_fields(owner, cube, exclude_fields=None, _mtime=False):
    collection = '%s__%s' % (owner, cube)
    spec = {'_id': collection}
    _filter = {'_id': 0}
    if not _mtime:
        _filter.update({'_mtime': 0})

    cube_fields = ETL_ACTIVITY.find_one(spec, _filter) or {}

    # exclude fields from `cube_fields` that are in `exclude_fields`
    for f in set(strip_split(exclude_fields) or []) & set(cube_fields):
        del cube_fields[f]

    # these are included, constantly
    cube_fields.update({'_id': 1, '_oid': 1, '_start': 1, '_end': 1})
    return cube_fields


def list_cubes(owner=None):
    '''
        Get a list of cubes server exports
    '''
    names = mongodb_config.db_timeline_data.db.collection_names()
    names = [n for n in names if not n.startswith('system')]
    if owner:
        return [n for n in names if n.startswith(owner)]
    else:
        return names
