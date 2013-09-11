#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique User Management" related funtions '''

import logging
logger = logging.getLogger(__name__)
import os

from metriqueu.utils import set_default
from metriqueu.defaults import DEFAULT_METRIQUE_LOGIN_URL


def login(self, username=None, password=None):
    '''
    Login a user

    :param String user: Name of the user you're managing
    :param String password:
        Password (plain text), if any of user
    '''
    if not username:
        username = self.config.api_username
    if not password:
        password = self.config.api_password
    return self._post('login', username=username,
                      password=password, api_url=False)


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
    username = set_default(username,
                           self.config.api_username, required=True)
    password = set_default(password,
                           self.config.api_password, required=True)
    return self._post('register',
                      username=username, password=password,
                      role=None, api_url=False)


def update_passwd(self, new_password, old_password=None,
                  username=None, save=False):
    '''
    Update existing user profile properties

    :param String user: Name of the user you're managing
    :param String password:
        Password (plain text), if any of user
    '''
    username = set_default(username,
                           self.config.api_username, required=True)
    cmd = os.path.join(username, 'passwd')
    response = self._post(cmd,
                          username=username,
                          old_password=old_password,
                          new_password=new_password,
                          api_url=False,
                          allow_redirects=False,
                          full_response=True)

    if response.status_code == 302 and username == self.config.api_username:
        self._load_session()
        self.config.api_password = new_password
        if save:
            self.config.save()

    if response.headers.get('location') == DEFAULT_METRIQUE_LOGIN_URL:
        login(self, self.config.api_username, self.config.api_password)

    return True


# FIXME: prefix api_url with _ to indicate it's a special kwarg
def update_profile(self, username=None, backup=False, email=None):
    username = set_default(username,
                           self.config.api_username, required=True)
    cmd = os.path.join(username, 'update_profile')
    result = self._post(cmd,
                        backup=backup,
                        email=email,
                        api_url=False)
    return result


def update_properties(self, username=None, backup=False, quota=None):
    username = set_default(username,
                           self.config.api_username, required=True)
    cmd = os.path.join(username, 'update')
    result = self._post(cmd,
                        backup=backup,
                        quota=quota,
                        api_url=False)
    return result
