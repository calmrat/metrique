#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import base64
from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import simplejson as json
from socket import getfqdn
from tornado.web import RequestHandler, authenticated, HTTPError
from tornado import gen

from metriqued.config import role_is_valid
from metriqued.utils import get_auth_keys, get_collection
from metriqued.tornadod.auth import is_admin, basic, krb_basic

FQDN = getfqdn()
AUTH_KEYS = get_auth_keys()


class MetriqueHdlr(RequestHandler):
    '''
    Template RequestHandler that accepts init parameters
    and unifies json get_argument handling
    '''
    def ping(self):
        logger.debug('got ping @ %s' % datetime.utcnow())
        response = {
            'action': 'ping',
            'response': 'pong',
            'from_host': FQDN,
            'current_user': self.get_current_user(),
        }
        return response

    def initialize(self, metrique_config, mongodb_config):
        '''
        :param HTTPServer proxy:
            A pointer to the running metrique server instance
        '''
        self.metrique_config = metrique_config
        self.mongodb_config = mongodb_config

    def get_argument(self, key, default=None, with_json=True):
        '''
        Assume incoming arguments are json encoded,
        get_arguments should always deserialize on the way in
        '''
        # arguments are expected to be json encoded!
        _arg = super(MetriqueHdlr, self).get_argument(key, default)

        if _arg and with_json:
            try:
                arg = json.loads(_arg)
            except Exception as e:
                a_type = type(_arg)
                self._raise(400,
                            "Invalid JSON content (%s): %s\n%s" % (a_type,
                                                                   e, _arg))
        else:
            arg = _arg

        return arg

    def write(self, value):
        result = json.dumps(value, ensure_ascii=False)
        super(MetriqueHdlr, self).write(result)

    def get_current_user(self):
        current_user = self.get_secure_cookie("user")
        if current_user:
            logger.debug('CURRENT USER: %s' % current_user)
            return current_user
        else:
            ok, current_user = self._parse_auth_headers()
            if ok:
                self.set_secure_cookie("user", current_user)
            logger.debug('CURRENT USER: %s' % current_user)
            return current_user

    @gen.coroutine
    def _prepare_async(self):
        return super(MetriqueHdlr, self).prepare()

    def prepare(self):
        if self.metrique_config.async:
            return self._prepare_async()
        else:
            return super(MetriqueHdlr, self).prepare()

    def _raise(self, code, msg):
            if code == 401:
                self.set_header('WWW-Authenticate', 'Basic realm="metrique"')
            raise HTTPError(code, msg)

    def _has_cube_role(self, current_user, owner, cube, role):
        role_is_valid(role)
        try:
            _cube = get_collection(owner, cube)
        except HTTPError:
            return 0
        else:
            spec = {'_id': role,
                    'value': {'$in': [current_user, '__all__']}}
            return _cube.find(spec).count()

    def _is_owner(self, current_user, owner, cube):
        # WARNING: create=True creates a new mongodb, lazilly
        #_cube = get_collection(owner, cube, create=True)
        _cube = get_collection(owner, cube)
        spec = {'__owner__': current_user}
        return _cube.find(spec).count()

    def _is_self(self, current_user, user):
        return current_user == user

    def _in_group(self, current_user, group):
        spec = {'_id': current_user, 'groups': group}
        return AUTH_KEYS.find(spec).count()

    def _is_admin(self, current_user, owner, cube=None):
        _is_admin = self._is_self(current_user, 'admin')
        _is_group_admin = self._in_group(current_user, 'admin')
        _cube_role = self._has_cube_role(current_user, owner,
                                         cube, '__admin__')
        return any((_is_admin, _is_group_admin, _cube_role))

    def _is_write(self, current_user, owner, cube=None):
        _is_admin = self._is_admin(current_user, owner, cube)
        _cube_role = self._has_cube_role(current_user, owner,
                                         cube, '__write__')
        return any((_is_admin, _cube_role))

    def _is_read(self, current_user, owner, cube=None):
        _is_admin = self._is_admin(current_user, owner, cube)
        _cube_role = self._has_cube_role(current_user, owner,
                                         cube, '__read__')
        return any((_is_admin, _cube_role))

    def _requires(self, owner, role_func, cube=None):
        assert role_func in (self._is_read, self._is_write, self._is_admin)
        current_user = self.get_current_user()
        _exists = self._cube_exists(owner, cube)
        if cube and _exists:
            _is_owner = self._is_owner(current_user, owner, cube)
            _is_role = role_func(current_user, owner, cube)
            ok = any((_is_owner, _is_role))
        elif not cube:
            ok = role_func(current_user, owner, cube)
        else:
            # there's no current owner... cube doesn't exist
            ok = True
        if not ok:
            self._raise(401, "insufficient privleges")
        return ok

    def _requires_self_admin(self, owner):
        current_user = self.get_current_user()
        _is_self = self._is_self(current_user, owner)
        _is_admin = self._is_admin(current_user, owner)
        return any((_is_self, _is_admin))

    def _requires_owner_admin(self, owner, cube=None):
        self._requires(owner=owner, cube=cube, role_func=self._is_admin)

    def _requires_owner_write(self, owner, cube=None):
        self._requires(owner=owner, cube=cube, role_func=self._is_write)

    def _requires_owner_read(self, owner, cube=None):
        self._requires(owner=owner, cube=cube, role_func=self._is_read)

    def _cube_exists(self, owner, cube):
        try:
            _cube = get_collection(owner, cube)
        except HTTPError:
            return 0
        else:
            return _cube.find({'_id': '__created__'}).count()

    def _scrape_username_password(self):
        username = ''
        password = ''
        auth_header = self.request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Basic '):
            auth = base64.decodestring(auth_header[6:])
            username, password = auth.split(':', 2)
        return username, password

    def _parse_admin_auth(self, username, password):
        admin_user = self.metrique_config.admin_user
        admin_password = self.metrique_config.admin_password
        return is_admin(admin_user, admin_password, username, password)

    def _parse_basic_auth(self, username, password):
        return basic(username, password)

    def _parse_krb_basic_auth(self, username, password):
        if not (self.metrique_config.krb_auth and
                self.metrique_config.krb_realm):
            return False, username
        else:
            return krb_basic(username, password,
                             self.metrique_config.krb_realm)

    def _parse_auth_headers(self):
        username, password = self._scrape_username_password()
        _admin, username = self._parse_admin_auth(username, password)
        _basic, username = self._parse_basic_auth(username, password)
        _krb_basic, username = self._parse_krb_basic_auth(username, password)

        if any((_admin, _basic, _krb_basic)):
            return True, username
        else:
            return False, username


class PingHdlr(MetriqueHdlr):
    ''' RequestHandler for pings '''
    def get(self):
        auth = self.get_argument('auth')
        if auth and not self.get_current_user():
            self._raise(403, "authentication failed")
        elif auth:
            pong = authenticated(self.ping())
        else:
            pong = self.ping()
        self.write(pong)


class ObsoleteAPIHdlr(MetriqueHdlr):
    ''' RequestHandler for handling obsolete API calls '''
    def get(self):
        self._raise(410, "This API version is no long supported")
