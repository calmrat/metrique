#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import base64
from collections import OrderedDict
import logging
logger = logging.getLogger(__name__)
import os
import pql
import uuid

from metriqueu.utils import batch_gen

OBJECTS_MAX_BYTES = 16777216
EXISTS_SPEC = {'$exists': 1}
BASE_INDEX = OrderedDict(
    [('_id', 1), ('_start', -1), ('_end', -1), ('_oid', -1), ('_hash', 1)])
SYSTEM_INDEXES = [BASE_INDEX]


def get_pid_from_file(pid_file):
    pid_file = os.path.expanduser(pid_file)
    if os.path.exists(pid_file):
        pid = int(open(pid_file).readlines()[0])
    else:
        pid = 0
    return pid


def ifind(_cube, _id=None, _start=None, _end=None, _oid=None, _hash=None,
          fields=None, spec=None, sort=None, hint=False, one=False,
          **kwargs):
    # note, to force limit; use __getitem__ like...
    # docs_limited_50 = ifind(...)[50]
    # SEE:
    # http://api.mongodb.org/python/current/api/pymongo/cursor.html
    # section #pymongo.cursor.Cursor.__getitem__
    # trying to use limit=... fails to work given our index
    index_spec = make_index_spec(_id, _start, _end, _oid, _hash)
    # FIXME: index spec is bson... can we .update() bson???
    # like here, below?
    if spec:
        index_spec.update(spec)

    if one:
        return _cube.find_one(index_spec, fields, sort=sort, **kwargs)
    else:
        cursor = _cube.find(index_spec, fields, sort=sort, **kwargs)
        if hint:
            return cursor.hint(BASE_INDEX)
        else:
            return cursor


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


def make_index_spec(_id=None, _start=None, _end=None, _oid=None, _hash=None):
    _id = _id or EXISTS_SPEC
    _start = _start or EXISTS_SPEC
    _end = _end or EXISTS_SPEC
    _oid = _oid or EXISTS_SPEC
    _hash = _hash or EXISTS_SPEC
    spec = OrderedDict([('_id', _id),
                        ('_start', _start),
                        ('_end', _end),
                        ('_oid', _oid),
                        ('_hash', _hash)])
    return spec


def new_cookie_secret():
    cs = base64.b64encode(uuid.uuid4().bytes + uuid.uuid4().bytes)
    logger.warn('new cookie secret: %s' % cs)
    return cs


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
        logger.info('no pid_file arg provided...')
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
