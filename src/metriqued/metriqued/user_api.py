#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from passlib.hash import sha256_crypt
from tornado.web import authenticated

# move DEFAULT... to config and load from self.
from metriqued.config import DEFAULT_CUBE_QUOTA
from metriqued.core_api import MetriqueHdlr
from metriqued.utils import set_property

from metriqueu.utils import utcnow


class AboutMeHdlr(MetriqueHdlr):
    '''
    RequestHandler for seeing your user profile

    action can be push, pop
    role can be read, write, admin
    '''
    @authenticated
    def get(self, owner):
        # FIXME: add admin check
        # if admin, look up anyone is possible
        # otherwise, must be owner or read
        if not self._requires_self_read(owner):
            self._raise(401, "authorization required")
        result = self.aboutme(owner=owner)
        self.write(result)

    def aboutme(self, owner):
        user_profile = self.get_user_profile(owner, one=True)
        mask_filter = set(['passhash', ''])
        user_profile = dict([(k, v) for k, v in user_profile.items()
                             if k not in mask_filter])
        return user_profile


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
                self._raise(401, "this requires admin privleges")
        if ok:
            # bump expiration...
            self.set_secure_cookie("user", username)
        logger.debug("AUTH HEADERS ... [%s] %s" % (username, ok))
        self.write(ok)

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


class RegisterHdlr(MetriqueHdlr):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self):
        # FIXME: add a 'cube registration' lock
        username = self.get_argument('username')
        password = self.get_argument('password')
        # FIXME: assumes we're not using sso kerberos, etc
        if not (username and password):
            self._raise(400, "username and password REQUIRED")
        result = self.register(username=username,
                               password=password)
        # FIXME: DO THIS FOR ALL HANDLERS! REST REST REST
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
        self.set_status(201, 'Registration successful: %s' % username)
        self.write(result)

    def register(self, username, password=None, null_password_ok=False):
        if self.user_exists(username):
            raise ValueError("user exists")
        passhash = sha256_crypt.encrypt(password) if password else None
        if not (passhash or null_password_ok):
            raise ValueError("[%s] no password provided" % username)
        doc = {'_id': username,
               '_ctime': utcnow(),
               '_groups': [],  # _these should be private (owner/admin only)
               'own': [],
               '_read': [],
               '_write': [],
               '_admin': [],
               'cube_quota': DEFAULT_CUBE_QUOTA,
               'passhash': passhash,
               #'cube_count': 0,  # can be calculated counting 'own'
               }
        doc['passhash'] = passhash
        self.user_profile(admin=True).save(doc, upset=True, safe=True)
        logger.debug("new user added (%s)" % (username))
        return True


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

        result = self.update_passwd(username=username,
                                    old_password=old_password,
                                    new_password=new_password)

        if self.current_user == 'admin':
            self.write(result)

        if result:
            self.clear_cookie("user")
        if self.metrique_config.login_url:
            self.redirect(self.metrique_config.login_url)
        else:
            self.write(result)

    def update_passwd(self, username, new_password, old_password=None):
        ''' Change a logged in user's password '''
        # FIXME: take out a lock... for updating any properties
        # like this....
        if not new_password:
            raise ValueError('new password can not be null')
        if not old_password:
            old_password = ''

        old_passhash = self.get_user_profile(username, ['passhash'])
        if not old_passhash:
            raise ValueError("user doesn't exist")

        if old_passhash and sha256_crypt.verify(old_password, old_passhash):
            new_passhash = sha256_crypt.encrypt(new_password)
        elif not old_password:
            new_passhash = sha256_crypt.encrypt(new_password)
        else:
            raise ValueError("old password does not match")

        update = {'$set': {'passhash': new_passhash}}
        spec = {'_id': username}
        self.user_profile(admin=True).update(spec, update,
                                             upsert=True, safe=True)
        logger.debug("passwd updated (%s)" % username)
        return True


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
        result = self.update_passwd(username=username,
                                    group=group, action=action)
        self.write(result)

    def _update_group(self, username, group, action):
        spec = {'_id': username}
        update = {'$%s' % action: {'groups': group}}
        self.user_profile(admin=True).update(spec, update, safe=True)
        return True

    def update_group(self, username, group, action='push'):
        ''' Change a logged in user's password '''
        self.valid_user(username)
        self.valid_group(group)
        self.valid_action(action)
        self._update_group(username, group, action)
        logger.debug("group updated (%s)" % username)
        return True


class UpdateProfileHdlr(MetriqueHdlr):
    '''
    '''
    @authenticated
    def post(self, username=None):
        self._requires_self_admin(username)
        backup = self.get_argument('backup')
        email = self.get_argument('email')
        result = self.update_profile(username=username,
                                     backup=backup,
                                     email=email)
        if result:
            self.write(True)
        else:
            self.write(False)

    def update_profile(self, username, backup=False, email=None):
        '''
        update user profile
        '''
        if backup:
            backup = self.get_user_profile(username)

        spec = {'_id': self.current_user}
        email = set_property({}, 'email', email, [basestring])

        update = {'$set': email}
        self.user_profile(admin=True).update(spec, update, safe=True)
        if backup:
            return backup
        else:
            return True


class UpdatePropertiesHdlr(MetriqueHdlr):
    '''
    '''
    @authenticated
    def post(self, username=None):
        self._is_admin(username)
        backup = self.get_argument('backup')
        cube_quota = self.get_argument('cube_quota')
        result = self.update_properties(username=username,
                                        backup=backup,
                                        cube_quota=cube_quota)
        if result:
            self.write(True)
        else:
            self.write(False)

    def update_properties(self, username, backup=True, cube_quota=None):
        '''
        update global user properties
        '''
        if backup:
            backup = self.get_user_profile(username)

        spec = {'_id': username}
        cuba_quota = set_property({}, 'cube_quota', cube_quota,
                                  [int, float])

        update = {'$set': cuba_quota}
        result = self.user_profile(admin=True).update(spec, update,
                                                      safe=True)
        logger.debug("user properties updated (%s): %s" % (username, result))
        if backup:
            return backup
        else:
            return True
