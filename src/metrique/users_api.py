#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique User Management" related funtions '''

import logging
logger = logging.getLogger(__name__)

CMD = 'admin/users'


def add(self, user, password=None, permissions='r', cube=None):
    '''
    Add user permissions (or update if exists)
    Assigne that user a password (salt+hash)

    permissions are, as of v0.1::
    * r, rw, admin
    * inherent right (r <- rw <- admin)

    :param String user: Name of the user you're managing
    :param String password:
        Password (plain text), if any of user
    :param String permission:
        Permission set, as of v0.1 (None, r, rw, admin)
        Permissions decorate tornado object methods (result?)
        and add 'auth'
    :param string: cube name to use
    '''
    if not cube:
        cube = self.name
    return self._get(CMD, 'add', cube=cube, user=user,
                     password=password, permissions=permissions)
