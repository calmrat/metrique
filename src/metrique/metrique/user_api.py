#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique User Management" related funtions '''

import logging
logger = logging.getLogger(__name__)


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
    if not username:
        username = self.config.api_username
    if not password:
        password = self.config.api_password
    return self._post('register',
                      username=username, password=password,
                      api_url=False)


def passwd(self, old_password, new_password, save=False):
    '''
    Update existing user profile properties

    :param String user: Name of the user you're managing
    :param String password:
        Password (plain text), if any of user
    '''
    result = self._post('passwd',
                        old_password=old_password,
                        new_password=new_password,
                        api_url=False)
    if result:
        self._load_session()
        self.config.api_password = new_password
        if save:
            self.config.save()
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
    if not cube:
        cube = self.name
    return self._get('add',
                     cube=cube, username=username, role=role,
                     api_url=False)
