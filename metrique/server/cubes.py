#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from metrique.server.config import mongodb

mongodb_config = mongodb()


def get_cube(cube, admin=True, timeline=False):
    ''' return back a mongodb connection to give cube collection '''
    if admin:
        return mongodb_config.db_timeline_admin.db[cube]
    else:
        return mongodb_config.db_timeline_data.db[cube]


def get_auth_keys():
    return mongodb_config.c_auth_keys


def get_etl_activity():
    return mongodb_config.c_etl_activity


def strip_split(item):
    if isinstance(item, basestring):
        return [s.strip() for s in item.split(',')]
    else:
        return item


def get_fields(cube, fields=None, check=False):
    ''' return back a list of known fields in documents of a given cube '''
    logger.debug('... fields: %s' % fields)
    _fields = []
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
    _fields += ['_oid', '_start', '_end']
    _fields = dict([(f, 1) for f in set(_fields)])
    if '_id' not in _fields:
        _fields['_id'] = 0
    logger.debug('... matched fields (%s)' % _fields)
    return _fields


def list_cube_fields(cube, exclude_fields=[], _mtime=False):
    spec = {'_id': cube}
    _filter = {'_id': 0}
    if not _mtime:
        _filter.update({'_mtime': 0})
    cube_fields = get_etl_activity().find_one(spec, _filter)
    if not cube_fields:
        cube_fields = {}
    if not isinstance(exclude_fields, (list, tuple, set)):
        raise ValueError(
            'expected list, got %s' % type(exclude_fields))
    else:
        exclude_fields = list(exclude_fields)

    for f in exclude_fields:
        try:
            del cube_fields[f]
        except KeyError:
            # just ignore any invalid fields
            pass

    cube_fields.update({'_id': 1, '_oid': 1, '_start': 1, '_end': 1})
    return cube_fields


def list_cubes():
    '''
        Get a list of cubes server exports
        (optionally) filter out cubes user can't 'r' access
    '''
    names = mongodb_config.db_timeline_data.db.collection_names()
    return [n for n in names if not n.startswith('system')]
