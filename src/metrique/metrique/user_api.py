#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique User Management" related funtions '''

import logging
logger = logging.getLogger(__name__)
import os

from metrique.utils import set_default


def login(self, api_username=None, api_password=None):
    '''
    Login a user

    :param String user: Name of the user you're managing
    :param String password:
        Password (plain text), if any of user
    '''
    if not api_username:
        api_username = self.config.api_username
    if not api_password:
        api_password = self.config.api_password
    return self._post('login', api_username=api_username,
                      api_password=api_password, api_url=False)


def logout(self):
    '''
    Log a user out by nulling their secrete cookie key
    '''
    self._load_session()  # new session
    return self._post('logout', api_url=False)


def register(self, username=None, password=None):
    '''
    Register new user

    :param String user: Name of the user you're managing
    :param String password:
        Password (plain text), if any of user
    '''
    username = set_default(username, self.config.api_username, required=True)
    password = set_default(password, self.config.api_password, required=True)
    return self._post('register',
                      username=username, password=password,
                      role=None, api_url=False)


def passwd(self, new_password, old_password=None, username=None, save=False):
    '''
    Update existing user profile properties

    :param String user: Name of the user you're managing
    :param String password:
        Password (plain text), if any of user
    '''
    username = set_default(username, self.config.api_username, required=True)
    cmd = os.path.join(username, 'passwd')
    result = self._post(cmd,
                        username=username,
                        old_password=old_password,
                        new_password=new_password,
                        api_url=False)
    if result and username == self.config.api_username:
        self._load_session()
        self.config.api_password = new_password
        if save:
            self.config.save()
    return result


def update(self, username=None, backup=False, **kwargs):
    if not kwargs:
        self.logger.debug("kwargs is empty... not updating")
        return False
    username = set_default(username, self.config.api_username, required=True)
    cmd = os.path.join(username, 'update')
    result = self._post(cmd,
                        backup=backup,
                        api_url=False,
                        **kwargs)
    return result


# cube should be user.cube
# so username shouldn't be necessary...
def add(self, username, cube=None, role='r'):
    '''
    Add user permissions (or update if exists)
    Assigne that user a password (salt+hash)

    permissions are, as of v0.1::
    * r, rw, admin
    * inherent right (r <- rw <- admin)

    :param String user: Name of the user you're managing
    :param String role:
        Permission set, as of v0.1 (None, r, rw, admin)
        Permissions decorate tornado object methods (result?)
        and add 'auth'
    :param string: cube name to use
    '''
    cube = set_default(cube, self.name, required=True)
    return self._get('add',
                     cube=cube, username=username, role=role,
                     api_url=False)
