#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import simplejson as json
from tornado.web import authenticated

from metriqued.tornadod.handlers.core_api import MetriqueHdlr
from metriqued import user_api


class RegisterHdlr(MetriqueHdlr):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self):
        # FIXME: add a 'cube registration' lock
        username = self.get_argument('username')
        password = self.get_argument('password')
        if not (username and password):
            self._raise(400, "username and password REQUIRED")
        result = user_api.register(username=username,
                                   password=password)
        # FIXME: DO THIS FOR ALL HANDLERS! REST REST REST
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
        self.set_status(201, 'Registration successful: %s' % username)
        self.write(result)


class LoginHdlr(MetriqueHdlr):
    '''
    RequestHandler for logging a user into metrique
    '''
    def post(self):
        # FIXME: IF THE USER IS USING KERB
        # OR WE OTHERWISE ALREADY KNOW WHO
        # THEY ARE, CHECK THAT THEY HAVE
        # A USER ACCOUNT NOW; IF NOT, CREATE
        # ONE FOR THEM

        current_user = self.get_current_user()
        if current_user:
            logger.debug('USER SESSION OK: %s' % current_user)
            self.write(True)
            # FIXME: update cookie expiration date
            return

        ok, username = self._parse_auth_headers()
        logger.debug("AUTH HEADERS ... [%s] %s" % (username, ok))

        if ok:
            self.set_secure_cookie("user", username)

            _next = self.get_argument('next', with_json=False)
            if _next:
                # go ahead and redirect if we expected to be somewhere else
                self.redirect(_next)
            else:
                self.write(True)
                # necessary only in cases of running with @async
                #self.finish()
        else:
            self._raise(401, "this requires admin privleges")

    def get(self):
        ''' alias get/post for login '''
        self.post()


class LogoutHdlr(MetriqueHdlr):
    '''
    RequestHandler for logging a user out of metrique
    '''
    @authenticated
    def post(self):
        self.clear_cookie("user")
        self.write(True)


class UpdatePasswordHdlr(MetriqueHdlr):
    '''
    RequestHandler for updating existing users password
    '''
    @authenticated
    def post(self, username):
        old_password = self.get_argument('old_password')
        new_password = self.get_argument('new_password')
        if not new_password:
            self._raise(400, "new password REQUIRED")

        self._requires_self_admin(username)

        result = user_api.update_passwd(username=username,
                                        old_password=old_password,
                                        new_password=new_password)

        current_user = self.get_current_user()
        if current_user == 'admin':
            self.write(result)

        if result:
            self.clear_cookie("user")
        if self.metrique_config.login_url:
            self.redirect(self.metrique_config.login_url)
        else:
            self.write(result)


class UpdateGroupHdlr(MetriqueHdlr):
    '''
    RequestHandler for managing user group properties

    action can be push, pop
    role can be admin
    '''
    @authenticated
    def post(self, username):
        self._requires_self_admin(username)
        action = self.get_argument('action', 'push')
        group = self.get_argument('group')
        result = user_api.update_passwd(username=username,
                                        group=group, action=action)
        self.write(result)


class UpdateProfileHdlr(MetriqueHdlr):
    '''
    '''
    @authenticated
    def post(self, username=None):
        self._requires_self_admin(username)
        backup = self.get_argument('backup')
        email = self.get_argument('email')
        result = user_api.update_profile(username=username,
                                         backup=backup,
                                         email=email)
        if result:
            self.write(True)
        else:
            self.write(False)


class UpdatePropertiesHdlr(MetriqueHdlr):
    '''
    '''
    @authenticated
    def post(self, username=None):
        current_user = self.get_current_user()
        self._is_admin(current_user)
        backup = self.get_argument('backup')
        quota = self.get_argument('quota')
        result = user_api.update_properties(username=username,
                                            backup=backup,
                                            quota=quota)
        if result:
            self.write(True)
        else:
            self.write(False)
