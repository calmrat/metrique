#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from passlib.hash import sha256_crypt
from tornado.web import HTTPError

from metriqued.config import VALID_ROLES, DEFAULT_CUBE_QUOTA
from metriqued.cubes import get_auth_keys

# FIXME: rather than dumping this meta data into auth_keys...
# drop it into a _metrique_cube using the api() calls
# to take advantage of transparent snapshotting, etc.

auth_keys = get_auth_keys()


def add(username, cube=None, role='r'):
    if cube is None:
        # if cube is not specified, the user is getting
        # role for all available cubes...
        cube = '__all__'

    cube = '%s.%s' % (username, cube)

    if role not in VALID_ROLES:
        raise ValueError(
            "Expected acl == %s. Got %s" % (
                (VALID_ROLES, role)))

    spec = {'_oid': username}
    logger.debug("ADD USER (%s:%s:%s)" % (username, cube, role))
    update = {'$set': {cube: {'roles': [{username: role}]}}}
    return auth_keys.update(spec, update, upsert=True)


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
        logger.debug("[%s] NO PASSWORD PROVIDED" % username)
    doc = spec
    doc['groups'] = []
    doc['cube_quota'] = quota
    doc['cube_count'] = 0
    doc['passhash'] = passhash
    result = auth_keys.save(doc, safe=True)
    logger.debug("NEW USER ADDED (%s)" % (username))
    # FIXME: ADD HOOKS HERE TO DISPACT ADDITIONAL
    # LOOKUP ALGORITHMS BASED ON THE USERNAME;
    # LDAP, ... twitter... to auto-fill in more profile details
    return str(result)


def passwd(username, new_password, old_password=None):
    ''' Change a logged in user's password '''
    if not new_password:
        raise HTTPError(400, 'new password can not be null')

    spec = {'_oid': username}

    doc = auth_keys.find_one(spec)
    old_passhash = doc['passhash']
    if old_passhash and sha256_crypt.verify(old_password,
                                            old_passhash):
        new_passhash = sha256_crypt.encrypt(new_password)
    else:
        raise RuntimeError("old password does not match")

    update = {'passhash': new_passhash}
    result = auth_keys.update(spec, update, upsert=True, safe=True)
    logger.debug("passwd updated (%s): %s" % (username, result))
    return result


def update(username, backup=False, **kwargs):
    '''
    update user profile
    '''
    # FIXME: maybe use _ to indicate 'immutable' properties?
    # raise 400 anytime and _immutable is included in kwargs
    if 'passhash' in kwargs:
        raise HTTPError(400, "use passwd to update password")
    # FIXME: have each possible kwarg
    # have a 'validate' funtion to type check?
    # should we avoid overwrites?
    spec = {'_oid': username}
    if backup:
        backup = auth_keys.find_one(spec)
    update = {'$set': kwargs}
    result = auth_keys.update(spec, update, safe=True)
    logger.debug("user properties updated (%s): %s" % (username, result))
    if backup:
        return {'result': result,
                'backup': backup}
    else:
        return result
