#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import base64
from bson import SON
import logging
logger = logging.getLogger(__name__)
import os
import pql
import uuid
import re

from metriqueu.utils import batch_gen, dt2ts

OBJECTS_MAX_BYTES = 16777216
EXISTS_SPEC = {'$exists': 1}
BASE_INDEX = [('_start', -1), ('_end', -1),
              ('_oid', -1), ('_hash', 1)]
SYSTEM_INDEXES = [BASE_INDEX]


def get_date_pql_string(date, prefix=' and ', query=None):
    if date is None:
        if query:
            return prefix + '_end == None'
        else:
            return '_end == None'

    if date == '~':
        return ''

    before = lambda d: '_start <= %f' % dt2ts(d)
    after = lambda d: '(_end >= %f or _end == None)' % dt2ts(d)
    split = date.split('~')
    # replace all occurances of 'T' with ' '
    # this is used for when datetime is passed in
    # like YYYY-MM-DDTHH:MM:SS instead of
    #      YYYY-MM-DD HH:MM:SS as expected
    # and drop all occurances of 'timezone' like substring
    split = [re.sub('\+\d\d:\d\d', '', d.replace('T', ' ')) for d in split]
    if len(split) == 1:
        # 'dt'
        ret = '%s and %s' % (before(split[0]), after(split[0]))
    elif split[0] == '':
        # '~dt'
        ret = '%s' % before(split[1])
    elif split[1] == '':
        # 'dt~'
        ret = '%s' % after(split[0])
    else:
        # 'dt~dt'
        ret = '%s and %s' % (before(split[1]), after(split[0]))
    if query:
        return query + prefix + ret
    else:
        return prefix + ret


def get_pid_from_file(pid_file):
    pid_file = os.path.expanduser(pid_file)
    if os.path.exists(pid_file):
        pid = int(open(pid_file).readlines()[0])
    else:
        pid = 0
    return pid


def ifind(_cube, _start=EXISTS_SPEC, _end=EXISTS_SPEC, _oid=EXISTS_SPEC,
          _hash=EXISTS_SPEC,
          fields=None, spec=None, sort=None, hint=True, one=False,
          **kwargs):
    # note, to force limit; use __getitem__ like...
    # docs_limited_50 = ifind(...)[50]
    # SEE:
    # http://api.mongodb.org/python/current/api/pymongo/cursor.html
    # section #pymongo.cursor.Cursor.__getitem__
    # trying to use limit=... fails to work given our index
    index_spec = make_index_spec(_start, _end, _oid, _hash)
    #logger.debug('ifind... INDEX SPEC: %s' % index_spec)
    # FIXME: index spec is bson... can we .update() bson???
    # like here, below?
    if spec:
        index_spec.update(spec)
    #logger.debug('ifind... UPDATED SPEC: %s' % index_spec)

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


def make_index_spec(_start=EXISTS_SPEC, _end=EXISTS_SPEC,
                    _oid=EXISTS_SPEC, _hash=EXISTS_SPEC):
    spec = SON([('_start', _start),
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
