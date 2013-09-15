#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from bson import SON
import logging
logger = logging.getLogger(__name__)
import os
import pql

from metriqueu.utils import batch_gen

OBJECTS_MAX_BYTES = 16777216
EXISTS_SPEC = {'$exists': 1}
BASE_INDEX = [('_start', -1), ('_end', -1), ('_oid', -1), ('_hash', 1)]


def exec_update_role(_cube, username, role, action):
    spec = {'_id': role}
    update = {'$%s' % action: {'value': username}}
    _cube.update(spec, update, safe=True, multi=False)
    return True


def get_cube_quota_count(doc):
    if doc:
        cube_quota = doc.get('cube_quota', None)
        cube_count = doc.get('cube_count', None)
    else:
        cube_quota = None
        cube_count = None
    if cube_quota is None:
        cube_quota = 0  # FIXME: SET AS CONFIGURABLE DEFAULT
    if cube_count is None:
        cube_count = 0  # FIXME: SET AS CONFIGURABLE DEFAULT
    cube_quota = int(cube_quota)
    cube_count = int(cube_count)
    return cube_quota, cube_count


def get_pid_from_file(pid_file):
    pid_file = os.path.expanduser(pid_file)
    if os.path.exists(pid_file):
        pid = int(open(pid_file).readlines()[0])
    else:
        pid = 0
    return pid


def ifind(_cube, _start=None, _end=None, _oid=None, _hash=None,
          fields=None, spec=None, sort=None, **kwargs):
    # note, to force limit; use __getitem__ like...
    # docs_limited_50 = ifind(...)[50]
    # SEE:
    # http://api.mongodb.org/python/current/api/pymongo/cursor.html
    # section #pymongo.cursor.Cursor.__getitem__
    # trying to use limit=... fails to work given our index
    index_spec = make_index_spec(_start, _end, _oid, _hash)
    # FIXME: index spec is bson... can we .update() bson???
    # like here, below?
    if spec:
        index_spec.update(spec)
    result = _cube.find(index_spec, fields,
                        sort=sort, **kwargs).hint(BASE_INDEX)
    return result


def insert_bulk(_cube, docs, size=-1):
    # little reason to batch insert...
    # http://stackoverflow.com/questions/16753366
    # and after testing, it seems splitting things
    # up more slows things down.
    if size <= 0:
        _cube.insert(docs, manipulate=False)
    else:
        for batch in batch_gen(docs, size):
            _cube.insert(batch, manipulate=False)


def log_head(owner, cube, cmd, *args):
    logger.debug('%s (%s.%s): %s' % (cmd, owner, cube, args))


def make_update_spec(_start):
    return {'$set': {'_end': _start}}


def make_index_spec(_start=None, _end=None, _oid=None, _hash=None):
    _start = EXISTS_SPEC if not _start else _start
    _end = EXISTS_SPEC if not _end else _end
    _oid = EXISTS_SPEC if not _oid else _oid
    _hash = EXISTS_SPEC if not _hash else _hash
    spec = SON([('_start', _start),
                ('_end', _end),
                ('_oid', _oid),
                ('_hash', _hash)])
    return spec


def parse_pql_query(query):
    if not query:
        return {}
    if not isinstance(query, basestring):
        raise TypeError("query expected as a string")
    pql_parser = pql.SchemaFreeParser()
    try:
        spec = pql_parser.parse(query)
    except Exception as e:
        raise ValueError("Invalid Query (%s):\n%s" % (query, str(e)))
    logger.debug("PQL Query: %s" % query)
    logger.debug('Query: %s' % spec)
    return spec


def parse_oids(oids, delimeter=','):
    if isinstance(oids, basestring):
        oids = [s.strip() for s in oids.split(delimeter)]
    if type(oids) is not list:
        raise TypeError("ids expected to be a list")
    return oids


def remove_pid_file(pid_file, quiet=True):
    if not pid_file:
        logger.warn('no pid_file arg provided...')
        return
    try:
        os.remove(pid_file)
    except OSError:
        if quiet:
            pass
        else:
            raise


def set_property(dct, key, value, _types):
    # expecting iterable
    assert isinstance(_types, (list, tuple))
    if value is None:
        return dct
    elif not isinstance(value, tuple(_types)):
        # isinstance expects arg 2 as tuple
        raise TypeError(
            "Invalid type for %s; "
            "got (%s), expected %s" % (key, type(value), _types))
    else:
        dct[key] = value
    return dct


def strip_split(item):
    if isinstance(item, basestring):
        return [s.strip() for s in item.split(',')]
    elif item is None:
        return []
    elif not isinstance(item, (list, tuple)):
        raise ValueError('Expected a list/tuple')
    else:
        # nothing to do here...
        return item
