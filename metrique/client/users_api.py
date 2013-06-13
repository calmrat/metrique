#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique User Management" related funtions '''

import logging
logger = logging.getLogger(__name__)

CMD = 'admin/users'


def add(self, user, password, permissions):
    '''
    Add user permissions (or update if exists)
    Assigne that user a password (salt+hash)

    permissions are, as of v0.1::
    * r, rw, admin
    * inherent right (r <- rw <- admin)

    Paremeters
    ----------
    cube : str
        Name of the cube you want to query
    user : str
        Name of the user you're managing
    password : str
        Password (plain text), if any of user
    permission : str
        Permission set, as of v0.1 (r, rw, admin)
        Permissions decorate tornado object methods (result?)
        and add 'auth'
    '''
    return self._get(CMD, 'add', cube=self.name, user=user,
                     password=password, permissions=permissions)
