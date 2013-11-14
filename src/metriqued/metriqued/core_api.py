#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Server package covers server side of metrique,
including http api (via tornado) server side
configuration, ETL, warehouse, query,
usermanagement, logging, etc.
'''

import base64
from bson import SON
try:
    import kerberos
except ImportError:
    kerberos = None
from passlib.hash import sha256_crypt
import cPickle
import random
import socket
import simplejson as json
from tornado.web import RequestHandler, HTTPError

from metriqued.utils import parse_pql_query

from metriqueu.utils import set_default, utcnow, strip_split

HOSTNAME = socket.gethostname()
SAMPLE_SIZE = 1
# 'own' is the one who created the cube; is cube superuser
# 'admin' is cube superuser; 'read' can only read; 'write' can only write
VALID_CUBE_ROLES = set(('own', 'admin', 'read', 'write'))
VALID_ACTIONS = set(('pull', 'addToSet', 'set'))


class MetriqueHdlr(RequestHandler):
    '''
    Template RequestHandler that accepts init parameters
    and unifies json get_argument handling
    '''
##################### mongo db #################################
    @staticmethod
    def check_sort(sort, son=False):
        '''
        ordered dict (or son) is required for pymongo's $sort operators
        '''
        if not sort:
            # FIXME
            return None

        try:
            assert len(sort[0]) == 2
        except (AssertionError, IndexError, TypeError):
            raise ValueError(
                "Invalid sort value (%s); try [('_oid': -1)]" % sort)
        if son:
            return SON(sort)
        else:
            return sort

    @staticmethod
    def cjoin(owner, cube):
        ''' shorthand for joining owner and cube together with dunder'''
        return '__'.join((owner, cube))

    def user_exists(self, username, raise_if_not=False):
        ''' user exists if there is a valid user profile '''
        return self.get_user_profile(username=username,
                                     raise_if_not=raise_if_not,
                                     exists_only=True)

    def cube_exists(self, owner, cube, raise_if_not=True):
        ''' cube exists if there is a valid cube profile '''
        return self.get_cube_profile(owner=owner, cube=cube,
                                     raise_if_not=raise_if_not,
                                     exists_only=True)

    def cube_profile(self, admin=False):
        ''' return back a mongodb connection to give cube collection '''
        if admin:
            return self.mongodb_config.c_cube_profile_admin
        else:
            return self.mongodb_config.c_cube_profile_data

    @staticmethod
    def estimate_obj_size(obj):
        return len(cPickle.dumps(obj))

    def get_fields(self, owner, cube, fields=None):
        '''
        Return back a dict of (field, 0/1) pairs, where
        the matching fields have 1.
        '''
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        self.logger.debug('... fields: %s' % fields)
        if fields in ['__all__', '~']:
            # None will make pymongo return back entire objects
            _fields = None
        else:
            # to return `_id`, it must be included in fields
            _fields = {'_id': 0, '_oid': 1, '_start': 1, '_end': 1}
            _split_fields = [f for f in strip_split(fields)]
            _fields.update(dict([(f, 1) for f in set(_split_fields)]))
        return _fields

    def get_cube_last_start(self, owner, cube):
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        _cube = self.timeline(owner, cube)
        doc = _cube.find_one({'_start': {'$exists': 1}}, sort=[('_start', -1)])
        if doc:
            return doc.get('_start')
        else:
            return None

    def get_user_profile(self, username, keys=None, raise_if_not=False,
                         exists_only=False, mask=None, null_value=None):
        if not username:
            self._raise(400, "username required")
        return self.get_profile(self.mongodb_config.c_user_profile_data,
                                _id=username, keys=keys,
                                raise_if_not=raise_if_not,
                                exists_only=exists_only,
                                mask=mask, null_value=null_value)

    def get_cube_profile(self, owner, cube, keys=None, raise_if_not=False,
                         exists_only=False, mask=None):
        if not owner and cube:
            self._raise(400, "owner and cube required")
        collection = self.cjoin(owner, cube)
        return self.get_profile(self.mongodb_config.c_cube_profile_data,
                                _id=collection, keys=keys,
                                raise_if_not=raise_if_not,
                                exists_only=exists_only,
                                mask=mask)

    def get_profile(self, _cube, _id, keys=None, raise_if_not=False,
                    exists_only=False, mask=None, null_value=None):
        '''
        find and return the user's profile data
        exists will just check if the user exists or not, then return
        '''
        if not _id:
            self._raise(400, "_id required")
        keys = set_default(keys, list, null_ok=True,
                           err_msg="keys must be a list")
        mask = set_default(mask, list, null_ok=True,
                           err_msg="keys must be a list")
        spec = {'_id': _id}
        cursor = _cube.find(spec)
        count = cursor.count()
        if not count:
            if raise_if_not:
                self._raise(400, 'resource does not exist: %s' % _id)
            elif exists_only:
                return False
            else:
                return {}
        elif exists_only:
            # return back only the count
            return True if count else False
        else:
            # return back the profile doc
            # which is the first and only item in the cursor
            profile = cursor.next()

        if keys:
            if profile:
                # extract out the nested items; we have
                # lists of singleton lists
                result = [profile.get(k, null_value) for k in keys
                          if not k in mask]
            else:
                # keep the same list length, for tuple unpacking assignments
                # like a, b = ...get_profile(..., keys=['a', 'b'])
                result = [null_value for k in keys]
            if len(keys) == 1:
                # return it un-nested
                result = result[0]
        else:
            result = profile
        return result

    def sample_timeline(self, owner, cube, sample_size=None, query=None):
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        if sample_size is None:
            sample_size = SAMPLE_SIZE
        query = set_default(query, '', null_ok=True)
        spec = parse_pql_query(query)
        _cube = self.timeline(owner, cube)
        docs = _cube.find(spec)
        n = docs.count()
        if n <= sample_size:
            docs = tuple(docs)
        else:
            to_sample = sorted(set(random.sample(xrange(n), sample_size)))
            docs = [docs[i] for i in to_sample]
        return docs

    @property
    def _timeline_data(self):
        return self.mongodb_config.db_timeline_data.db

    @property
    def _timeline_admin(self):
        return self.mongodb_config.db_timeline_admin.db

    def timeline(self, owner, cube, admin=False):
        ''' return back a mongodb connection to give cube collection '''
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        collection = self.cjoin(owner, cube)
        if admin:
            return self._timeline_admin[collection]
        else:
            return self._timeline_data[collection]

    def user_profile(self, admin=False):
        ''' return back a mongodb connection to give cube collection '''
        if admin:
            return self.mongodb_config.c_user_profile_admin
        else:
            return self.mongodb_config.c_user_profile_data

    def update_cube_profile(self, owner, cube, action, key, value):
        self.cube_exists(owner, cube)
        self.valid_action(action)
        collection = self.cjoin(owner, cube)
        _cube = self.cube_profile(admin=True)
        return self.update_profile(_cube=_cube, _id=collection,
                                   action=action, key=key, value=value)

    def update_user_profile(self, username, action, key, value):
        self.user_exists(username, raise_if_not=True)
        _cube = self.user_profile(admin=True)
        return self.update_profile(_cube=_cube, _id=username,
                                   action=action, key=key, value=value)

    def update_profile(self, _cube, _id, action, key, value):
        # FIXME: add optional type check...
        self.valid_action(action)
        spec = {'_id': _id}
        update = {'$%s' % action: {key: value}}
        _cube.update(spec, update)
        return True

    def valid_in_set(self, x, valid_set, raise_if_not=True):
        if isinstance(x, basestring):
            x = [x]
        if not isinstance(x, (list, tuple, set)):
            raise TypeError("expected string or iterable; got %s" % type(x))
        ok = set(x) <= valid_set
        if not ok and raise_if_not:
            self._raise(400, "invalid item in set; "
                        "got (%s). expected: %s" % (x, valid_set))
        return ok

    def valid_cube_role(self, roles, raise_if_not=True):
        return self.valid_in_set(roles, VALID_CUBE_ROLES, raise_if_not)

    def valid_action(self, actions, raise_if_not=True):
        return self.valid_in_set(actions, VALID_ACTIONS, raise_if_not)

##################### http request #################################
    def get_argument(self, key, default=None, with_json=True):
        '''
        Assume incoming arguments are json encoded,
        get_arguments should always deserialize on the way in
        '''
        # FIXME: it seems this should be unnecessary to do
        # manually; what if we set content-type to JSON in header?
        # would json decoding happen automatically?

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

    def initialize(self, metrique_config, mongodb_config, logger):
        '''
        :param HTTPServer proxy:
            A pointer to the running metrique server instance
        '''
        self.metrique_config = metrique_config
        self.mongodb_config = mongodb_config
        self.logger = logger

    def write(self, value, binary=False):
        if binary:
            super(MetriqueHdlr, self).write(value)
        else:
            result = json.dumps(value, ensure_ascii=False)
            super(MetriqueHdlr, self).write(result)

##################### auth #################################
    def get_current_user(self):
        current_user = self.get_secure_cookie("user")
        if current_user:
            self.logger.debug('EXISTING AUTH OK: %s' % current_user)
            return current_user
        else:
            ok, current_user = self._parse_auth_headers()
            if ok:
                self.set_secure_cookie("user", current_user)
                self.logger.debug('NEW AUTH OK: %s' % current_user)
                return current_user
            else:
                self.logger.debug('NEW AUTH FAILED: %s' % current_user)
                self.clear_cookie("user")
                return None

    def _scrape_username_password(self):
        username, password = '', ''
        auth_header = self.request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Basic '):
            auth = base64.decodestring(auth_header[6:])
            username, password = auth.split(':', 2)
        password = None if password == 'None' else password
        return username, password

    def _parse_basic_auth(self, username, password):
        ok = (username and password)
        if ok:
            username, password = str(username), str(password)
            passhash = self.get_user_profile(username, keys=['_passhash'])
            ok = bool(passhash and sha256_crypt.verify(password, passhash))
        self.logger.error('AUTH BASIC [%s]: %s' % (username, ok))
        return ok

    def _parse_krb_basic_auth(self, username, password):
        krb_auth = self.metrique_config.krb_auth
        # kerberos module must be available, krb auth enabled, realm defined
        # and username/password provided
        realm = self.metrique_config.realm
        ok = (kerberos and krb_auth and realm and username and password)
        if ok:
            try:
                ok = kerberos.checkPassword(username, password, '', realm)
            except kerberos.BasicAuthError as e:
                self.logger.debug('KRB ERROR [%s]: %s' % (username, e))
                ok = False
        self.logger.debug('KRB AUTH [%s]: %s' % (username, ok))
        return ok

    def _parse_auth_headers(self):
        username, password = self._scrape_username_password()
        ok = bool(self._parse_basic_auth(username, password) or
                  self._parse_krb_basic_auth(username, password))
        return ok, username

    def has_cube_role(self, owner, cube, role):
        ''' valid roles: read, write, admin, own '''
        self.cube_exists(owner, cube)
        cr = self.get_cube_profile(owner, cube, keys=[role])  # cube_role
        cu = self.current_user
        u = [cu, '__all__', '~']
        ok = bool(cr and any(x in cr for x in u))
        return ok

    def is_self(self, owner):
        return self.current_user == owner

    def is_admin(self, owner, cube=None):
        ok = self.current_user in self.metrique_config.superusers
        if not ok and cube:
            ok = self.has_cube_role(owner, cube, 'admin')
        return ok

    def is_write(self, owner, cube=None):
        return bool(self.has_cube_role(owner, cube, 'write') or
                    self.is_admin(owner, cube))

    def is_read(self, owner, cube=None):
        return bool(self.has_cube_role(owner, cube, 'read') or
                    self.has_cube_role(owner, cube, 'write') or
                    self.is_admin(owner, cube))

    def _requires(self, ok, raise_if_not=True):
        if not ok and raise_if_not:
            self._raise(401, 'insufficient privileges')
        return ok

    def requires_owner_admin(self, owner, cube=None, raise_if_not=True):
        ok = bool(self.is_self(owner) or self.is_admin(owner, cube))
        return self._requires(ok, raise_if_not)

    def requires_owner_read(self, owner, cube=None, raise_if_not=True):
        ok = bool(self.is_self(owner) or self.is_read(owner, cube))
        return self._requires(ok, raise_if_not)

    def requires_owner_write(self, owner, cube=None, raise_if_not=True):
        ok = bool(self.is_self(owner) or self.is_write(owner, cube))
        return self._requires(ok, raise_if_not)

##################### utils #################################
    def _raise(self, code, msg):
        if code == 401:
            _realm = self.metrique_config.realm
            basic_realm = 'Basic realm="%s"' % _realm
            self.set_header('WWW-Authenticate', basic_realm)
        raise HTTPError(code, msg)


class PingHdlr(MetriqueHdlr):
    ''' RequestHandler for pings '''
    def get(self):
        auth = self.get_argument('auth')
        result = self.ping(auth)
        self.write(result)

    def ping(self, auth=None):
        user = self.current_user
        if auth and not user:
            self._raise(401, "authentication required")
        else:
            self.logger.debug(
                'got ping from %s @ %s' % (user, utcnow(as_datetime=True)))
            response = {
                'action': 'ping',
                'current_user': user,
                'metriqued': HOSTNAME,
            }
            return response


class ObsoleteAPIHdlr(MetriqueHdlr):
    ''' RequestHandler for handling obsolete API calls '''
    def delete(self):
        self._raise(410, "this API version is no long supported")

    def get(self):
        self._raise(410, "this API version is no long supported")

    def post(self):
        self._raise(410, "this API version is no long supported")

    def update(self):
        self._raise(410, "this API version is no long supported")
