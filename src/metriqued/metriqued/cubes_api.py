#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from datetime import datetime
from tornado.web import HTTPError

from metriqued.utils import dt2ts
from metriqued.cubes import get_collection, get_auth_keys

auth_keys = get_auth_keys()


def _get_cube_quota_count(doc):
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


def _insert_meta_docs(_cube, owner):
    meta_docs = [
        {'_id': '__created__', 'value': dt2ts(datetime.utcnow())},
        {'_id': '__owner__', 'value': owner},
        {'_id': '__read__', 'value': [owner]},
        {'_id': '__write__', 'value': [owner]},
        {'_id': '__admin__', 'value': [owner]},
    ]
    _cube.insert(meta_docs, safe=True)


def register(owner, cube):
    '''
    Client registration method

    Update the user__cube __meta__ doc with defaults

    Bump the user's total cube count, by 1
    '''
    auth_keys_spec = {'_oid': owner}
    fields = {'cube_count': 1, 'cube_quota': 1}
    doc = auth_keys.find_one(auth_keys_spec, fields)
    cube_quota, cube_count = _get_cube_quota_count(doc)

    if cube_quota == 0:
        raise HTTPError(400, "sorry, but you can't create new cubes")
    elif cube_quota <= -1:
        pass
    elif (cube_quota - cube_count) > 0:
        pass
    else:
        raise HTTPError(
            400,
            "sorry, but you reached your cube quota limit (%s)" % cube_quota)

    _cube = get_collection(owner, cube, admin=True, create=True)
    # FIXME: RETURN ANYTHING HERE?
    _insert_meta_docs(_cube, owner)

    _cc = _cube.database.collection_names()
    real_cc = sum([1 for c in _cc if c.startswith(owner)])
    update = {'$set': {'cube_count': real_cc}}
    auth_keys.update(auth_keys_spec, update, safe=True)

    # do something with the fact that we know if it was
    # successful or not?
    return True
