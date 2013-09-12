#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from datetime import datetime
from tornado.web import HTTPError

from metriqued.cubes import get_collection, get_auth_keys
from metriqued.config import role_is_valid, action_is_valid
from metriqued.users_api import user_is_valid

from metriqueu.utils import dt2ts

AUTH_KEYS = get_auth_keys()


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
    now_utc = dt2ts(datetime.utcnow())
    meta_docs = [
        {'_id': '__created__', 'value': now_utc},
        {'_id': '__mtime__', 'value': now_utc},
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
    doc = AUTH_KEYS.find_one(auth_keys_spec, fields)
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
    _insert_meta_docs(_cube, owner)

    _cc = _cube.database.collection_names()
    real_cc = sum([1 for c in _cc if c.startswith(owner)])
    update = {'$set': {'cube_count': real_cc}}
    AUTH_KEYS.update(auth_keys_spec, update, safe=True)
    # do something with the fact that we know if it was
    # successful or not?
    return True


def _update_role(_cube, username, role, action):
    spec = {'_id': role}
    update = {'$%s' % action: {'value': username}}
    _cube.update(spec, update, safe=True)
    return True


def update_role(owner, cube, username, action, role):
    if owner == username and username != 'admin':
        # allow admin to do whatever (eg, disable access)
        # but don't allow the owner to shoot themselves
        # in the foot
        raise HTTPError(400, "Cannot modify cube.owner's role")
    role_is_valid(role)
    action_is_valid(action)
    user_is_valid(username)
    _cube = get_collection(owner, cube)
    result = _update_role(_cube, username, role, action)
    if result:
        return True
    else:
        return False
