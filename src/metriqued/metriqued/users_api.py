#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from passlib.hash import sha256_crypt
from tornado.web import HTTPError

from metriqued.config import DEFAULT_CUBE_QUOTA
from metriqued.cubes import get_auth_keys

# FIXME: rather than dumping this meta data into auth_keys...
# drop it into a _metrique_cube using the api() calls
# to take advantage of transparent snapshotting, etc.

auth_keys = get_auth_keys()


def register(username, password=None, quota=None):
    # FIXME: TRY to kerberos auth; otherwise
    # fail if no password is provided
    spec = {'_oid': username}
    if auth_keys.find(spec).count():
        raise HTTPError(409, "[%s] user exists" % username)
    if quota is None:
        quota = DEFAULT_CUBE_QUOTA
    passhash = sha256_crypt.encrypt(password) if password else None
    if not passhash:
        logger.debug("[%s] no password provided" % username)
    doc = spec
    doc['groups'] = []
    doc['cube_quota'] = quota
    doc['cube_count'] = 0
    doc['passhash'] = passhash
    auth_keys.save(doc, safe=True)
    logger.debug("new user added (%s)" % (username))
    return True


def update_passwd(username, new_password, old_password=None):
    ''' Change a logged in user's password '''
    if not new_password:
        raise HTTPError(400, 'new password can not be null')
    if not old_password:
        old_password = ''

    spec = {'_oid': username}

    doc = auth_keys.find_one(spec)
    old_passhash = None
    if doc:
        old_passhash = doc['passhash']
    else:
        raise HTTPError(400, "user doesn't exist")

    if old_passhash and sha256_crypt.verify(old_password, old_passhash):
        new_passhash = sha256_crypt.encrypt(new_password)
    elif not old_password:
        new_passhash = sha256_crypt.encrypt(new_password)
    else:
        raise HTTPError(400, "old password does not match")

    update = {'$set': {'passhash': new_passhash}}
    auth_keys.update(spec, update, upsert=True, safe=True)
    logger.debug("passwd updated (%s)" % username)
    return True


def _set_property(dct, key, value, _types):
    assert isinstance(_types, (list, tuple))
    if value is None:
        return dct
    elif not isinstance(value, _types):
        raise ValueError(
            "Invalid type for %s; "
            "got (%s), expected %s" % (key, type(value), _types))
    else:
        dct[key] = value
    return dct


def update_profile(username, backup=False, email=None):
    '''
    update user profile
    '''
    spec = {'_oid': username}
    if backup:
        backup = auth_keys.find_one(spec)

    spec = {}
    _set_property(spec, email, [basestring])

    update = {'$set': spec}
    auth_keys.update(spec, update, safe=True)
    if backup:
        return backup
    else:
        return True


def update_properties(username, backup=False, quota=None):
    '''
    update global user properties
    '''
    spec = {'_oid': username}
    if backup:
        backup = auth_keys.find_one(spec)

    spec = {}
    _set_property(spec, quota, [int, float])

    update = {'$set': spec}
    result = auth_keys.update(spec, update, safe=True)
    logger.debug("user properties updated (%s): %s" % (username, result))
    if backup:
        return backup
    else:
        return True
