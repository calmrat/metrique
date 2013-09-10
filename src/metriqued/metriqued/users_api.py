#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from passlib.hash import sha256_crypt

from metriqued.config import VALID_ROLES
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

    spec = {'_id': username}
    logger.debug("ADD USER (%s:%s:%s)" % (username, cube, role))
    update = {'$set': {cube: {'roles': [{username: role}]}}}
    return auth_keys.update(spec, update, upsert=True)


def register(username, password=None):
    # FIXME: TRY to kerberos auth; otherwise
    # fail if no password is provided
    spec = {'_id': username}
    # FIXME: CORRECT HTTP ERROR CODES
    if not password:
        raise RuntimeError("Password required")
    elif auth_keys.find_one(spec):
        raise RuntimeError("User exists (%s)" % username)
    passhash = sha256_crypt.encrypt(password) if password else None
    logger.debug("NEW USER (%s)" % (username))
    if not passhash:
        logger.debug(" [%s] NO PASSWORD PROVIDED" % username)
    update = {'$set': {'passhash': passhash}}
    return auth_keys.update(spec, update, upsert=True)


def passwd(username, old_password, new_password):
    ''' Change a logged in user's password '''
    if not new_password:
        # FIXME: CORRECT HTTPError's
        raise RuntimeError('new password can not be null')

    spec = {'_id': username}

    doc = auth_keys.find_one(spec)
    old_passhash = doc['passhash']
    if old_passhash and sha256_crypt.verify(old_password,
                                            old_passhash):
        new_passhash = sha256_crypt.encrypt(new_password)
    else:
        raise RuntimeError("old password does not match")

    update = {'passhash': new_passhash}
    result = auth_keys.update(spec, update, upsert=True, safe=True)
    logger.debug("PASSWD updated (%s): %s" % (username, result))
