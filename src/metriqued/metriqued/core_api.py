#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriqued.core_api
~~~~~~~~~~~~~~~~~~

This module contains all the core metriqued api functionality.
'''

import base64
from bson import SON
from concurrent.futures import ThreadPoolExecutor
try:
    import kerberos
except ImportError:
    kerberos = None
from passlib.hash import sha256_crypt
import cPickle
import random
import socket
import simplejson as json
from tornado import gen
from tornado.web import RequestHandler, HTTPError

from metriqued.utils import parse_pql_query, json_encode

from metriqueu.utils import set_default, utcnow, strip_split

HOSTNAME = socket.gethostname()
SAMPLE_SIZE = 1
# 'own' is the one who created the cube; is cube superuser
# 'admin' is cube superuser; 'read' can only read; 'write' can only write
VALID_CUBE_ROLES = set(('own', 'admin', 'read', 'write'))
VALID_ACTIONS = set(('pull', 'addToSet', 'set'))


class MetriqueHdlr(RequestHandler):
    '''
    Main tornado.RequestHandler for handling incoming 'metriqued api' requests.
    '''
############################### Cube Manipulation ##########
    @staticmethod
    def cjoin(owner, cube):
        '''Shorthand for joining owner and cube together with dunder

        :param cube: cube name
        :param owner: username of cube owner
        '''
        return '__'.join((owner, cube))

    def user_exists(self, username, raise_if_not=False):
        '''Check if user exists.

        True if there is a valid user profile
        False if there is not a valid user profile

        :param username: username to query
        :param raise_if_not: raise exception if user dosn't exist?
        '''
        return self.get_user_profile(username=username,
                                     raise_if_not=raise_if_not,
                                     exists_only=True)

    def cube_exists(self, owner, cube, raise_if_not=False):
        '''Check if cube exists.

        True if there is a valid cube profile
        False if there is not a valid cube profile

        :param cube: cube name
        :param owner: username of cube owner
        :param raise_if_not: raise exception if user dosn't exist?
        '''
        return self.get_cube_profile(owner=owner, cube=cube,
                                     raise_if_not=raise_if_not,
                                     exists_only=True)

    @staticmethod
    def estimate_obj_size(obj):
        '''
        Naively calculate an objects "size" by counting the
        total number number of characters of the pickle when
        dumped as a string.

        :param obj: object to estimate size of
        '''
        return len(cPickle.dumps(obj))

    def get_fields(self, owner, cube, fields=None):
        '''
        Return back a dict of (field, 0/1) pairs, where
        the matching fields have 1.

        :param cube: cube name
        :param owner: username of cube owner
        :param fields: list of fields to query
        '''
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        self.logger.debug('... fields: %s' % fields)
        if fields in ['__all__', '~']:
            # None indicates a request should return back whole objs
            _fields = None
        else:
            # to return `_id`, it must be included in fields
            _fields = {'_id': 0, '_oid': 1, '_start': 1, '_end': 1}
            _split_fields = [f for f in strip_split(fields)]
            _fields.update(dict([(f, 1) for f in set(_split_fields)]))
        return _fields

    def get_profile(self, _cube, _id, keys=None, raise_if_not=True,
                    exists_only=False, mask=None, null_value=None):
        '''
        Find and return a profile object from the designated cube.

        Expected to be implemented in Backend subclass
        '''
        raise NotImplemented

    def get_user_profile(self, username, keys=None, raise_if_not=True,
                         exists_only=False, mask=None, null_value=None):
        '''
        Query and return a given user profile.

        :param username: username to query
        :param keys: profile keys to return
        :param raise_if_not: raise exception if user dosn't exist?
        :param exists_only: only return bool whether profile exists
        :param mask: keys to exclude from results
        :param null_value: value to use to fill resulting list to keep
                           the same list length, when used in a tuple
                           unpacking assignments
        '''
        return self.get_profile(self.mongodb_config.c_user_profile_data,
                                _id=username, keys=keys,
                                raise_if_not=raise_if_not,
                                exists_only=exists_only,
                                mask=mask, null_value=null_value)

    def get_cube_profile(self, owner, cube, keys=None, raise_if_not=True,
                         exists_only=False, mask=None, null_value=None):
        '''
        Query and return a given cube profile.

        :param cube: cube name
        :param owner: username of cube owner
        :param keys: profile keys to return
        :param raise_if_not: raise exception if cube dosn't exist?
        :param exists_only: only return bool whether profile exists
        :param mask: keys to exclude from results
        :param null_value: value to use to fill resulting list to keep
                           the same list length, when used in a tuple
                           unpacking assignments
        '''
        if not owner and cube:
            self._raise(400, "owner and cube required")
        collection = self.cjoin(owner, cube)
        return self.get_profile(self.mongodb_config.c_cube_profile_data,
                                _id=collection, keys=keys,
                                raise_if_not=raise_if_not,
                                exists_only=exists_only,
                                mask=mask, null_value=null_value)

    def update_cube_profile(self, owner, cube, action, key, value):
        '''
        Update a given cube profile.

        :param cube: cube name
        :param owner: username of cube owner
        :param action: update action to take
        :param key: key to manipulate
        :param value: new value to assign to key

        Available actions:
            * pull - remove a value
            * addToSet - add a value
            * set - set or replace a value
        '''
        self.cube_exists(owner, cube)
        self.valid_action(action)
        collection = self.cjoin(owner, cube)
        _cube = self.cube_profile(admin=True)
        return self._update_profile(_cube=_cube, _id=collection,
                                    action=action, key=key, value=value)

    def update_user_profile(self, username, action, key, value):
        '''
        Update a given cube profile.

        :param cube: cube name
        :param owner: username of cube owner
        :param action: update action to take
        :param key: key to manipulate
        :param value: new value to assign to key

        Available actions:
            * pull - remove a value
            * addToSet - add a value
            * set - set or replace a value
        '''
        self.user_exists(username, raise_if_not=True)
        _cube = self.user_profile(admin=True)
        return self._update_profile(_cube=_cube, _id=username,
                                    action=action, key=key, value=value)

    def valid_in_set(self, x, valid_set, raise_if_not=True):
        '''
        Check if a value or list of values is a valid subset of
        another list (set) of predefined values.

        :param x: the value or list of values to be validated
        :param valid_set: the predefined super-set to be validated against
        :param raise_if_not: raise exception if not a subset
        '''
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
        '''
        Check if one or more given role strings are valid.

        :param roles: one or list of role strings to validate
        :param raise_if_not: raise exception if any are invalid
        '''
        return self.valid_in_set(roles, VALID_CUBE_ROLES, raise_if_not)

    def valid_action(self, actions, raise_if_not=True):
        '''
        Check if one or more given action strings are valid.

        :param roles: one or list of action strings to validate
        :param raise_if_not: raise exception if any are invalid
        '''
        return self.valid_in_set(actions, VALID_ACTIONS, raise_if_not)

##################### http request #########################
    def get_argument(self, key, default=None, with_json=True):
        '''
        We assume incoming arguments are json encoded.

        Therefore, we override get_arguments to always attempt to
        deserialize on the way in, unless explictly told the
        data is not json.

        :param key: argument key to manipulate
        :param default: default to apply to argument value if not present
        :param with_json: flag indicating our assumption that the data is JSON
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

    def _request_dict(self):
        r = self.request
        request = {
            'current_user': self.current_user,
            'start_time': r._start_time,
            'finish_time': r._finish_time,
            'arguments_len': len(r.arguments),
            'arguments_size': self.estimate_obj_size(r.arguments),
            'body_size': self.estimate_obj_size(r.body),
            'files': r.files,
            'full_url': r.full_url(),
            'headers': r.headers,
            'host': r.host,
            'method': r.method,
            'path': r.path,
            'protocol': r.protocol,
            'query': r.query,
            'remote_ip': r.remote_ip,
            'request_time': r.request_time(),
            'supports_http_1_1': r.supports_http_1_1(),
            'status': self.get_status(),
            'uri': r.uri,
            'version': r.version,
        }
        return request

    def _log_request(self):
        request_json = json.dumps(self._request_dict(), indent=1)
        with ThreadPoolExecutor(1) as ex:
            return ex.submit(self.logger.log_request, request_json)

    @gen.coroutine
    def on_finish(self):
        '''
        Routines to run after every tornado request completes.

        Currently implemented routines are as follows:
            * log the request (access) details
        '''
        yield self._log_request()  # log request details

    def write(self, value, binary=False):
        '''
        All http request writes are expected to be in JSON form,
        unless otherwise explicity requested to be in binary.

        :param value: value to be return to requesting http client
        :param binary: flag to indicate whether data is binary (write as-is)
        '''
        if binary:
            super(MetriqueHdlr, self).write(value)
        else:
            result = json.dumps(value, default=json_encode, ensure_ascii=False)
            super(MetriqueHdlr, self).write(result)

##################### auth #################################
    def current_user_acl(self, roles):
        '''
        Check if the current authenticated user has a given set
        of ACL roles assigned in their user profile.
        '''
        self.valid_cube_role(roles)
        roles = self.get_user_profile(self.current_user, keys=roles,
                                      null_value=[])
        return roles if roles else []

    def get_current_user(self):
        '''
        Authentication. Check for existing cookies or validate
        auth_headers (etc) to determine if a 'login' authentication
        request is in process.

        If user is logged in or successfully authenticates, return
        back the username.

        Otherwise, return None.
        '''
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

    def get_readable_collections(self):
        '''
        Return back a filtered list of collections the current authenticated
        user has read access to.
        '''
        names = [c for c in self._timeline_data.collection_names()
                 if not c.startswith('system')]
        if not self.is_superuser():
            names = [n for n in names if self.can_read(*n.split('__'))]
        return names

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
            passhash = self.get_user_profile(username, keys=['_passhash'],
                                             raise_if_not=False)
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

    def has_cube_role(self, owner, cube, role, raise_if_not=False):
        '''
        Access control check for cubes.

        Checks if an authenticated user has a given ACL 'role' in his profile
        which permits them to execute certain actions on a given cube.

        Valid Roles:
            * read - can read the cube (r)
            * write - can write to the cube (w)
            * admin - can admin the cube (r/w)
            * own - owns the cube (r/w)
        '''
        self.cube_exists(owner, cube, raise_if_not)
        cr = self.get_cube_profile(owner, cube, [role], raise_if_not)
        cu = self.current_user
        u = (cu, '__all__', '~')
        ok = bool(cr and any(x in cr for x in u))
        if raise_if_not:
            self._raise(400, "user does not in role %s for %s cube" % (
                role, self.cjoin(owner, cube)))
        return ok

    def is_self(self, user):
        '''
        Check if authenticated user is the user being queried or a superuser.

        :param owner: user current_user is expected to be (or superuser)
        '''
        ok = self.is_superuser()
        if not ok:
            ok = bool(self.current_user == user)
        return ok

    def is_superuser(self):
        '''
        Check if authenticated user has superuser privleges.

        The users in this group are defined in the global metriqued
        config file under the 'superusers' key.
        '''
        return bool(self.current_user in self.metrique_config.superusers)

    def can_admin(self, owner, cube):
        '''
        Check if authenticated user has admin privleges.

        The default is check if the authenticated user is listed
        as a global 'superuser'. If the user is not a superuser,
        we require that

        If not a superuser, we check if the authenticed user has
        the 'admin' role for the given cube.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        ok = self.is_superuser()
        if not ok:
            ok = self.has_cube_role(owner, cube, 'admin')
        return ok

    def can_write(self, owner, cube):
        '''
        Check if authenticated user has write cube privleges (or higher).

        :param owner: username of cube owner
        :param cube: cube name
        '''
        return bool(self.has_cube_role(owner, cube, 'write') or
                    self.can_admin(owner, cube))

    def can_read(self, owner, cube):
        '''
        Check if authenticated user has read cube privleges (or higher).

        :param owner: username of cube owner
        :param cube: cube name
        '''
        return bool(self.has_cube_role(owner, cube, 'read') or
                    self.has_cube_role(owner, cube, 'write') or
                    self.can_admin(owner, cube))

    def _requires(self, ok):
        if not ok:
            self._raise(401, 'insufficient privileges')
        return ok

    def requires_admin(self, owner, cube):
        '''
        Requested resources requires an authenticated user
        be either an owner, admin or superuser.

        :param owner: username of cube owner
        :param cube: cube name
        '''
        ok = self.can_admin(owner, cube)
        return self._requires(ok)

    def requires_read(self, owner, cube):
        ok = bool(self.can_admin(owner, cube) or self.can_read(owner, cube))
        return self._requires(ok)

    def requires_write(self, owner, cube):
        ok = bool(self.can_admin(owner, cube) or self.can_write(owner, cube))
        return self._requires(ok)

##################### utils ################################
    def _raise(self, code, msg):
        if code == 401:
            _realm = self.metrique_config.realm
            basic_realm = 'Basic realm="%s"' % _realm
            self.set_header('WWW-Authenticate', basic_realm)
        self.logger.info('[%s] %s: %s ...\n%s' % (self.current_user, code,
                                                  msg, self.request))
        raise HTTPError(code, msg)


class ObsoleteAPIHdlr(MetriqueHdlr):
    '''
    RequestHandler for handling obsolete API calls

    Raises HTTP 410 "API version is no longer supported"
    '''
    def delete(self):
        self._raise(410, "API version is no longer supported")

    def get(self):
        self._raise(410, "API version is no longer supported")

    def post(self):
        self._raise(410, "API version is no longer supported")

    def update(self):
        self._raise(410, "API version is no longer supported")


class MongoDBBackendHdlr(MetriqueHdlr):
    '''
    This class provides metrique requests methods for interaction with MongoDB

    It is currently the main and only backend supported by metriqued.
    '''
    @staticmethod
    def check_sort(sort, son=False):
        '''
        list of tuples (or son) is required for pymongo's $sort operators

        :param sort: sort object to validate
        :param son: flag whether to convert obj to and return SON instance
        '''
        if not sort:
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

    def cube_profile(self, admin=False):
        '''
        Shortcut for getting a mongodb proxy read/admin cube profile collection

        :param admin: flag for getting back a (read/write) authenticated proxy
        '''
        if admin:
            return self.mongodb_config.c_cube_profile_admin
        else:
            return self.mongodb_config.c_cube_profile_data

    def get_cube_last_start(self, owner, cube):
        '''
        Return back the most recent objects _start timestamp

        :param cube: cube name
        :param owner: username of cube owner
        '''
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        _cube = self.timeline(owner, cube)
        doc = _cube.find_one({'_start': {'$exists': 1}}, sort=[('_start', -1)])
        if doc:
            return doc.get('_start')
        else:
            return None

    def get_profile(self, _cube, _id, keys=None, raise_if_not=True,
                    exists_only=False, mask=None, null_value=None):
        '''
        Find and return a profile object from the designated cube.

        :param _cube: proxy to profile collection to query
        :param _id: object _id to query
        :param keys: profile keys to return
        :param raise_if_not: raise exception if any are invalid
        :param exists_only: only return bool whether profile exists
        :param mask: keys to exclude from results
        :param null_value: value to use to fill resulting list to keep
                           the same list length, when used in a tuple
                           unpacking assignments
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
        elif exists_only:  # return back only the count
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

    def initialize(self, metrique_config, mongodb_config, logger):
        '''
        Initializer method which is run upon creation of each tornado request

        :param metrique_config: metriqued configuration object
        :param mongodb_config: mongodb configuration object
        :param logger: logger object
        '''
        self.metrique_config = metrique_config
        self.mongodb_config = mongodb_config
        self.logger = logger

    def sample_cube(self, owner, cube, sample_size=None, query=None):
        '''
        Take a psuedo-random sampling of objects from a given cube.

        :param cube: cube name
        :param owner: username of cube owner
        :param sample_size: number of objects to sample
        :param query: high-level query used to create population to sample
        '''
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
        return self.mongodb_config.db_timeline_data

    @property
    def _timeline_admin(self):
        return self.mongodb_config.db_timeline_admin

    def timeline(self, owner, cube, admin=False):
        '''
        Return back a mongodb connection to give cube collection in
        the timeline database

        :param cube: cube name
        :param owner: username of cube owner
        :param admin: flag for getting back a (read/write) authenticated proxy
        '''
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        collection = self.cjoin(owner, cube)
        if admin:
            return self._timeline_admin[collection]
        else:
            return self._timeline_data[collection]

    def user_profile(self, admin=False):
        '''
        Shortcut for getting a mongodb proxy read/admin user profile collection

        :param admin: flag for getting back a (read/write) authenticated proxy
        '''
        if admin:
            return self.mongodb_config.c_user_profile_admin
        else:
            return self.mongodb_config.c_user_profile_data

    def _update_profile(self, _cube, _id, action, key, value):
        # FIXME: add optional type check...
        # and drop utils set_propery function
        self.valid_action(action)
        spec = {'_id': _id}
        update = {'$%s' % action: {key: value}}
        _cube.update(spec, update)
        return True


class PingHdlr(MongoDBBackendHdlr):
    ''' RequestHandler for pings '''
    def get(self):
        auth = self.get_argument('auth')
        result = self.ping(auth)
        self.write(result)

    def ping(self, auth=None):
        '''
        Simple ping/pong. Returns back some basic details
        of the host:app which caught the ping.

        :param auth: flag to force authentication
        '''
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
