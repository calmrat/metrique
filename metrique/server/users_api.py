#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from metrique.server.job import job_save
from metrique.server.defaults import VALID_PERMISSIONS
from metrique.server.cubes import get_auth_keys
from metrique.tools import hash_password


@job_save('users_add')
def add(cube, username, password=None, permissions='r'):
    if permissions not in VALID_PERMISSIONS:
        raise ValueError(
            "Expected acl == %s. Got %s" % (
                (VALID_PERMISSIONS, permissions)))
    if password:
        salt, password = hash_password(password)
    else:
        salt, password = None, None
    spec = {'_id': cube}
    logger.debug("NEW USER (%s:%s)" % (username,
                                       permissions))
    update = {'$set': {
              username: {'salt': salt,
                         'password': password,
                         'permissions': permissions}}}

    return get_auth_keys().update(spec, update, upsert=True)
