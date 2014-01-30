#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriqued.user_api
~~~~~~~~~~~~~~~~~~

This module contains all the user related api functionality.
'''

from passlib.hash import sha256_crypt
import re
import time
from tornado.web import authenticated

from metriqued.core_api import MongoDBBackendHdlr

from metriqueu.utils import utcnow

INVALID_USERNAME_RE = re.compile('[^a-z]', re.I)


class AboutMeHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for retreiving a user profile
    '''
    @authenticated
    def get(self, username):
        # FIXME: add admin check
        # if admin, look up anyone is possible
        # otherwise, must be owner or read
        result = self.aboutme(username=username)
        self.write(result)

    def aboutme(self, username):
        '''
        Get a user profile.

        Requires the requesting user be authenticated as the same
        user who's details are being requested or a superuser.

        :param username: username whose profile is being requested
        '''
        mask = ['passhash']
        if not self.is_self():
            self._raise(401, "not authorized")
        self.user_exists(username, raise_if_not=True)
        return self.get_user_profile(username, mask=mask)


class LoginHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for authenticating a user into metrique
    '''
    def post(self):
        '''
        Authenticate the user.

        Check for existing secure cookie if available.

        Otherwise, fallback to checking auth headers; basic, kerberos,
        etc. depending on what's enabled.

        Redirect user according to the 'next' argument's value, if
        the user was directed to login from some other resource
        they were trying to access which requires authentication.

        Raise 401 if there are any issues
        '''
        if self.current_user:
            ok, username = True, self.current_user
        else:
            ok, username = self._parse_auth_headers()
            if ok:
                _next = self.get_argument('next', with_json=False)
                if _next:
                    # go ahead and redirect if we expected to be somewhere else
                    self.redirect(_next)
                else:
                    ok = True
            else:
                time.sleep(1)  # sleep for a second
                self._raise(401, "authentication failed")
        if ok:
            self.clear_cookie("user")
            self.set_secure_cookie("user", username)
        result = 'OK' if ok else 'FAILED'
        self.logger.debug("Auth %s ... [%s]" % (result, username))
        self.write(ok)


class LogoutHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for logging a user out of metrique
    '''
    @authenticated
    def post(self):
        '''
        Clear any existing secure cookies.
        '''
        self.clear_cookie("user")
        self.write(True)


class RegisterHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self):
        username = self.get_argument('username')
        password = self.get_argument('password')
        result = self.register(username=username, password=password)
        self.set_status(201, 'Registration successful: %s' % username)
        self.write(result)

    def register(self, username, password=None, null_password_ok=False):
        '''
        Register a given username, if available.

        Username's must be ascii alpha characters only (a-z).

        Usernames are automatically normalized in the following ways:
            * lowercased

        :param username: username to register
        :param password: password to register for username
        :param null_password_ok: flag for whether empty password is ok (krb5)
        '''
        # FIXME: add a 'cube registration' lock
        if INVALID_USERNAME_RE.search(username):
            self._raise(
                400, "Invalid username; ascii alpha [a-z] characters only!")
        username = username.lower()
        if self.user_exists(username, raise_if_not=False):
            self._raise(409, "[%s] user exists" % username)
        passhash = sha256_crypt.encrypt(password) if password else None
        if not (passhash or null_password_ok):
            self._raise(400, "[%s] no password provided" % username)
        cube_quota = self.metrique_config.user_cube_quota
        doc = {'_id': username,
               '_ctime': utcnow(),
               'own': [],
               'read': [],
               'write': [],
               'admin': [],
               'cube_quota': cube_quota,
               '_passhash': passhash,
               }
        self.user_profile(admin=True).save(doc, upsert=True, safe=True)
        self.logger.info("new user added (%s)" % (username))
        return True


class RemoveHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for removing existing users.
    '''
    def delete(self, username):
        result = self.remove(username=username)
        self.write(result)

    def remove(self, username):
        '''
        Remove an existing user.

        Requires requesting user be a superuser.

        Raises an exception if the user doesn't exist.

        :param username: username whose profile will be removed
        '''
        if not self.is_superuser():
            self._raise(401, "not authorized")
        username = username.lower()
        self.user_exists(username, raise_if_not=True)
        # delete the user's profile
        spec = {'_id': username}
        self.user_profile(admin=True).remove(spec)

        # remove the user's cubes
        spec = {'owner': username}
        cubes = [x for x in self.cube_profile().find(spec, {'_id': 1})]
        for cube in cubes:
            cube = cube.get('_id')
            self.mongodb_config.db_timeline_admin[cube].drop()
        # FIXME: # remove the user from cube acls?
        self.logger.info("user removed (%s)" % username)
        return True


class UpdatePasswordHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for updating existing users password
    '''
    @authenticated
    def post(self, username):
        old_password = self.get_argument('old_password')
        new_password = self.get_argument('new_password')
        result = self.update_passwd(username=username,
                                    old_password=old_password,
                                    new_password=new_password)
        if result:
            self.clear_cookie("user")
        if self.metrique_config.login_url:
            self.redirect(self.metrique_config.login_url)
        else:
            self.write(result)

    def update_passwd(self, username, new_password, old_password=None):
        '''
        Change a logged in user's password.

        :param username: username who's password will be updated
        :param new_password: new password to apply to user's profile
        :param old_password: old (current) password for validation
        '''
        # FIXME: take out a lock... for updating any properties
        if not self.is_self():
            self._raise(401, "not authorized")
        self.user_exists(username, raise_if_not=True)
        if not new_password:
            self._raise(400, 'new password can not be null')
        if not old_password:
            if not self.is_superuser():
                self._raise(400, 'old password can not be null')
        old_passhash = None
        if old_password:
            old_passhash = self.get_user_profile(username, ['_passhash'])
            if old_passhash and sha256_crypt.verify(old_password,
                                                    old_passhash):
                new_passhash = sha256_crypt.encrypt(new_password)
            else:
                self._raise(400, "old password does not match")
        else:
            new_passhash = sha256_crypt.encrypt(new_password)
        self.update_user_profile(username, 'set', '_passhash', new_passhash)
        self.logger.debug(
            "password updated (%s) by %s" % (username, self.current_user))
        return True


class UpdateProfileHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for updating existing user profiles non-system properties.
    '''
    @authenticated
    def post(self, username=None):
        gnupg = self.get_argument('gnupg')
        result = self.update_profile(username=username,
                                     gnupg=gnupg)
        self.write(result)

    def update_profile(self, username, gnupg=None):
        '''
        Update user profile non-system properties.

        A backup of the current profile state will be returned
        after succesful modification of user profile.

        :param username: username whose profile will be manipulated
        :param gnupg: gnupg public key
        '''
        if not self.is_self():
            self._raise(401, "not authorized")
        self.user_exists(username, raise_if_not=True)
        reqkeys = ('fingerprint', 'pubkey')
        if not isinstance(gnupg, dict) and sorted(gnupg.keys()) != reqkeys:
            self._raise(400,
                        "gnupg must be a dict with keys pubkey/fingerprint")
        else:
            backup = self.get_user_profile(username)
            self.logger.debug('GPG PubKey Import: %s' % gnupg)
            self.gnupg_pubkey_import(gnupg['pubkey'])
            self.update_user_profile(username, 'set', 'gnupg', gnupg)
        current = self.get_user_profile(username)
        return {'now': current, 'previous': backup}

    def gnupg_pubkey_import(self, gnupg_key):
        return self.metrique_config.gnupg.import_keys(gnupg_key)


class UpdatePropertiesHdlr(MongoDBBackendHdlr):
    '''
    RequestHandler for updating existing user profiles system properties.
    '''
    @authenticated
    def post(self, username=None):
        backup = self.get_argument('backup')
        cube_quota = self.get_argument('cube_quota')
        result = self.update_properties(username=username,
                                        backup=backup,
                                        cube_quota=cube_quota)
        self.write(bool(result))

    def update_properties(self, username, cube_quota=None):
        '''
        Update user profile system properties.

        A backup of the current profile state will be returned
        after succesful modification of user profile.

        Requesting user must be a superuser to update system level
        user profile properties.

        :param username: username whose profile will be manipulated
        :param gnupg: maximum number of cubes the user can create
        '''
        if not self.is_superuser():
            self._raise(401, "not authorized")
        self.user_exists(username, raise_if_not=True)
        backup = self.get_user_profile(username)
        # FIXME: make update_user_profile (or new method) to accept
        # a dict to apply not just a single key/value
        self.update_user_profile(username, 'set', 'cube_quota', cube_quota)
        current = self.get_user_profile(username)
        self.logger.debug(
            "user properties updated (%s): %s" % (username, current))
        return {'now': current, 'previous': backup}
