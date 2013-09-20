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
import logging
logger = logging.getLogger(__name__)
try:
    import kerberos
except ImportError:
    kerberos = None
from passlib.hash import sha256_crypt
import cPickle
import random
import re
import simplejson as json
from tornado.web import RequestHandler, HTTPError
from tornado import gen

from metriqued.utils import parse_pql_query, ifind

from metriqueu.utils import set_default, dt2ts, utcnow, strip_split

SAMPLE_SIZE = 1
VALID_CUBE_ROLES = set(('admin', 'own', 'read', 'write'))
VALID_GROUPS = set(('admin', ))
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
            sort = [('_oid', -1)]
        try:
            assert len(sort[0]) == 2
        except (AssertionError, IndexError, TypeError):
            raise ValueError("Invalid sort value; try [('_oid': -1)]")
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
        logger.debug('... fields: %s' % fields)
        if fields == '__all__':
            # None will make pymongo return back entire objects
            _fields = None
        else:
            # to return `_id`, it must be included in fields
            _fields = {'_id': -1, '_start': 1, '_end': 1}
            _split_fields = [f for f in strip_split(fields)]
            _fields.update(dict([(f, 1) for f in set(_split_fields)]))
        return _fields

    def get_cube_mtime(self, owner, cube):
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        return self.get_cube_profile(owner, cube, keys=['mtime'])

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
        docs = ifind(_cube=_cube, spec=spec)
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
        _cube.update(spec, update, safe=True)
        return True

    def valid_in_set(self, x, valid_set, raise_if_not=True):
        if isinstance(x, basestring):
            x = [x]
        elif not isinstance(x, (list, tuple, set)):
            raise TypeError("expected string or iterable; got %s" % type(x))
        is_subset = set(x) <= valid_set
        if is_subset:
            return True
        elif raise_if_not:
            self._raise(400, "invalid item in set; "
                        "got (%s). expected: %s" % (x, valid_set))
        else:
            return False

    def valid_cube_role(self, roles):
        return self.valid_in_set(roles, VALID_CUBE_ROLES)

    def valid_group(self, groups):
        return self.valid_in_set(groups, VALID_GROUPS)

    def valid_action(self, actions):
        return self.valid_in_set(actions, VALID_ACTIONS)

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

    def initialize(self, metrique_config, mongodb_config):
        '''
        :param HTTPServer proxy:
            A pointer to the running metrique server instance
        '''
        self.metrique_config = metrique_config
        self.mongodb_config = mongodb_config

    @gen.coroutine
    def _prepare_async(self):
        return super(MetriqueHdlr, self).prepare()

    def prepare(self):
        # FIXME: check size of request content
        # if more than 16M... reject
        if self.metrique_config.async:
            return self._prepare_async()
        else:
            return super(MetriqueHdlr, self).prepare()

    def write(self, value):
        # content expected to always be JSON
        # but isn't this unecessary? can we set content type to
        # JSON in header and this be handled automatically?
        result = json.dumps(value, ensure_ascii=False)
        super(MetriqueHdlr, self).write(result)

##################### auth #################################
    def get_current_user(self):
        current_user = self.get_secure_cookie("user")
        if current_user:
            logger.debug('EXISTING AUTH OK: %s' % current_user)
            return current_user
        else:
            ok, current_user = self._parse_auth_headers()
            if ok:
                self.set_secure_cookie("user", current_user)
                logger.debug('NEW AUTH OK: %s' % current_user)
                return current_user
            else:
                logger.debug('NEW AUTH FAILED: %s' % current_user)
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
        if not (username and password):
            return False, username
        if not isinstance(password, basestring):
            self._raise(400, "password expected to be a string; "
                        "got %s" % type(password))
        passhash = self.get_user_profile(username, keys=['_passhash'])
        if passhash and sha256_crypt.verify(password, passhash):
            logger.error('AUTH BASIC OK [%s]' % username)
            return True, username
        else:
            logger.error('AUTH BASIC ERROR [%s]' % username)
            return False, username

    def _parse_krb_basic_auth(self, username, password):
        krb_auth = self.metrique_config.krb_auth
        if not all((kerberos, krb_auth, username, password)):
            return False, username
        else:
            realm = self.metrique_config.realm
            try:
                authed = kerberos.checkPassword(username, password,
                                                '', realm)
                logger.error('KRB AUTH [%s]: %s' % (username, authed))
                return authed, username
            except kerberos.BasicAuthError as e:
                logger.error('KRB ERROR [%s]: %s' % (username, e))
                return False, username

    def _parse_auth_headers(self):
        username, password = self._scrape_username_password()
        _basic, username = self._parse_basic_auth(username, password)
        _krb_basic, username = self._parse_krb_basic_auth(username, password)
        if _basic or _krb_basic:
            return True, username
        else:
            return False, username

    def has_cube_role(self, owner, cube, role):
        ''' valid roles: read, write, admin, own '''
        if not (owner and cube):
            self._raise(400, "owner and cube required")
        self.valid_cube_role(role)
        user_role = self.get_user_profile(self.current_user, keys=['role'])
        if user_role and self.cjoin(owner, cube) in user_role:
            return True
        else:
            return False

    def is_self(self, user):
        return self.current_user == user

    def self_in_group(self, group):
        self.valid_group(group)
        user_group = self.get_user_profile(self.current_user, keys=['group'])
        if user_group and group in user_group:
            return True
        else:
            return False

    def is_admin(self, owner, cube=None):
        _is_group_admin = lambda: self.self_in_group('admin')
        _is_super_user = lambda x: x in self.metrique_config.superusers
        _is_admin = _is_group_admin() or _is_super_user(self.current_user)
        if cube:
            _cube_role = lambda: self.has_cube_role(owner, cube, 'admin')
            return _is_admin or _cube_role()
        else:
            return _is_admin

    def is_write(self, owner, cube=None):
        _cube_role = lambda: self.has_cube_role(owner, cube, 'write')
        _is_admin = lambda: self.is_admin(owner, cube)
        return _cube_role() or _is_admin()

    def is_read(self, owner, cube=None):
        _is_admin = lambda: self.is_admin(owner, cube)
        _cube_role = lambda: self.has_cube_role(owner, cube, 'read')
        return _cube_role() or _is_admin()

    def _requires(self, admin_func, raise_if_not=True):
        if admin_func():
            return True
        elif raise_if_not:
            self._raise(401, 'insufficient privileges')
        else:
            return False

    def requires_owner_admin(self, owner, cube=None, raise_if_not=True):
        _is_self = lambda: self.is_self(owner)
        _is_admin = lambda: self.is_admin(owner, cube)
        admin_func = lambda: _is_admin() or _is_self()
        return self._requires(admin_func, raise_if_not)

    def requires_owner_read(self, owner, cube=None, raise_if_not=True):
        _is_self = lambda: self.is_self(owner)
        _is_read = lambda: self.is_read(owner, cube=cube)
        admin_func = lambda: _is_read() or _is_self()
        return self._requires(admin_func, raise_if_not)

    def requires_owner_write(self, owner, cube=None, raise_if_not=True):
        _is_self = lambda: self.is_self(owner)
        _is_write = lambda: self.is_write(owner, cube=cube)
        admin_func = lambda: _is_write() or _is_self()
        return self._requires(admin_func, raise_if_not)

##################### utils #################################
    def _raise(self, code, msg):
            if code == 401:
                _realm = self.metrique_config.realm
                basic_realm = 'Basic realm="%s"' % _realm
                self.set_header('WWW-Authenticate', basic_realm)
            raise HTTPError(code, msg)

    @staticmethod
    def get_date_pql_string(date, prefix=' and '):
        if date is None:
            return prefix + '_end == None'
        if date == '~':
            return ''

        # replace all occurances of 'T' with ' '
        # this is used for when datetime is passed in
        # like YYYY-MM-DDTHH:MM:SS instead of
        #      YYYY-MM-DD HH:MM:SS as expected
        dt_str = date.replace('T', ' ')
        # drop all occurances of 'timezone' like substring
        dt_str = re.sub('\+\d\d:\d\d', '', dt_str)

        before = lambda d: '_start <= %f' % dt2ts(d)
        after = lambda d: '(_end >= %f or _end == None)' % dt2ts(d)
        split = date.split('~')
        logger.warn(split)
        if len(split) == 1:
            ret = '%s and %s' % (before(dt_str), after(dt_str))
        elif split[0] == '':
            ret = '%s' % before(split[1])
        elif split[1] == '':
            ret = '%s' % after(split[0])
        else:
            ret = '%s and %s' % (before(split[1]), after(split[0]))
        return prefix + ret


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
            logger.debug(
                'got ping from %s @ %s' % (user, utcnow(as_datetime=True)))
            response = {
                'action': 'ping',
                'response': 'pong',
                'current_user': user,
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
