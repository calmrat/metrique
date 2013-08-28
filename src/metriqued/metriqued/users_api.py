#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from metriqued.job import job_save
from metriqued.defaults import VALID_PERMISSIONS
from metriqued.cubes import get_auth_keys
from passlib.hash import sha256_crypt


@job_save('users_add')
def add(username, password=None, permissions='r', resource=None):
    if resource is None:
        # if resource is not specified, the user is getting
        # permissions for all available cubes...
        resource = '__all__'
    if permissions not in VALID_PERMISSIONS:
        raise ValueError(
            "Expected acl == %s. Got %s" % (
                (VALID_PERMISSIONS, permissions)))

    password = sha256_crypt.encrypt(password) if password else None
    spec = {'_id': resource}
    logger.debug("NEW USER (%s:%s:%s)" % (resource,
                                          username,
                                          permissions))
    update = {'$set': {
              username: {'password': password,
                         'permissions': permissions}}}
    return get_auth_keys().update(spec, update, upsert=True)
