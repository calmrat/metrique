#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import base64
from bson import SON
from datetime import datetime
import logging
logger = logging.getLogger(__name__)
try:
    import kerberos
except ImportError:
    kerberos = None
from passlib.hash import sha256_crypt
import pickle
import random
import re
import simplejson as json
from tornado.web import RequestHandler, HTTPError
from tornado import gen

from metriqued.utils import parse_pql_query, ifind, strip_split

from metriqueu.utils import set_default, dt2ts

DEFAULT_SAMPLE_SIZE = 1
VALID_ROLES = set(('admin', 'own', 'read', 'write'))
VALID_GROUPS = set(('admin', ))
VALID_CUBE_ROLE_ACTIONS = set(('pull', 'push'))

# FIXME: all 'raise' should raise an HTTPError


class MetriqueHdlr(RequestHandler):
    '''
    Template RequestHandler that accepts init parameters
    and unifies json get_argument handling
    '''
##################### mongo db #################################
    @staticmethod
    def check_sort(sort, son=False):
        '''
        son True is required for pymongo's aggregation $sort operator
        '''
        if not sort:
            sort = [('_oid', 1)]
        try:
            assert len(sort[0]) == 2
        except (AssertionError, IndexError, TypeError):
            raise ValueError("Invalid sort value; try [('_id': -1)]")
        if son:
            return SON(sort)
        else:
            return sort

    @staticmethod
    def cjoin(owner, cube):
        return '__'.join((owner, cube))

    def user_exists(self, username, check_only=False):
        if not username:
            if check_only:
                return None
            else:
                raise ValueError("username required")
        spec = {'_id': username}
        return self.user_profile().find(spec).count()

    def cube_exists(self, owner, cube, raise_on_null=False):
        if not (owner and cube):
            raise ValueError("owner and cube required")
        _cube = self.timeline(owner, cube)
        _has_docs = _cube.count()  # exists if count >= 1
        spec = {'_id': self.cjoin(owner, cube)}
        _meta_ok = self.cube_profile(admin=False).find(spec).count()
        ok = any((_has_docs, _meta_ok))
        if not ok and raise_on_null:
            raise ValueError("%s.%s does not exist" % (owner, cube))
        else:
            return ok

    @staticmethod
    def estimate_obj_size(obj):
        return len(pickle.dumps(obj))

    def get_fields(self, owner, cube, fields=None):
        '''
        Return back a dict of (field, 0/1) pairs, where
        the matching fields have 1.
        '''
        logger.debug('... fields: %s' % fields)
        _fields = []
        if fields:
            cube_fields = self.sample_fields(owner, cube)
            if fields == '__all__':
                _fields = cube_fields.keys()
            else:
                _fields = [f for f in strip_split(fields) if f in cube_fields]
        _fields += ['_id', '_start', '_end']
        _fields = dict([(f, 1) for f in set(_fields)])

        # If `_id` should not be in returned it must have
        # 0 otherwise mongo will return it.
        if '_id' not in _fields:
            _fields['_id'] = 0
        logger.debug('... matched fields (%s)' % _fields)
        return _fields

    def get_mtime(self, owner, cube):
        collection = self.cjoin(owner, cube)
        mtime = self.get_cube_profile(collection, keys=['mtime'])
        return mtime

    def get_user_profile(self, username, keys=None, check_only=False,
                         one=False):
        result = self.get_profile(self.mongodb_config.c_user_profile_data,
                                  username, keys, check_only, one)
        return result

    def get_cube_profile(self, collection, keys=None, check_only=False,
                         one=False):
        return self.get_profile(self.mongodb_config.c_cube_profile_data,
                                collection, keys, check_only, one)

    def get_profile(self, _cube, name, keys=None, check_only=False,
                    one=False):
        '''
        find and return the user's profile data
        exists will just check if the user exists or not, then return
        '''
        keys = set_default(keys, list, null_ok=True,
                           err_msg="keys must be a list")
        spec = {'_id': name}
        cursor = _cube.find(spec).sort([('_id', -1)])
        if cursor and check_only:
            # return back only the count
            return cursor.count()
        # return back the user's actual profile doc
        profile = list(cursor)
        if profile:
            profile = profile[0]
        else:
            profile = {}

        if keys and profile:
            # extract out the nexted items; we have
            # lists of singleton lists
            result = []
            for k in keys:
                # we don't want nested lists in lists;
                # this (and other's like it) where i'm
                # looking for 'list' type rather that'
                # 'iterable' type need to be fixed
                v = profile.get(k)
                result.append(v)
        elif keys and not profile:
            result = [None for k in keys]
        else:
            result = [profile]

        if one:
            assert len(result) == 1
            return result[0]
        else:
            return result

    def cube_profile(self, admin=False):
        ''' return back a mongodb connection to give cube collection '''
        if admin:
            return self.mongodb_config.c_cube_profile_admin
        else:
            return self.mongodb_config.c_cube_profile_data

    def user_profile(self, admin=False):
        ''' return back a mongodb connection to give cube collection '''
        if admin:
            return self.mongodb_config.c_user_profile_admin
        else:
            return self.mongodb_config.c_user_profile_data

    def sample_timeline(self, owner, cube, sample_size=None, query=None):
        if sample_size is None:
            sample_size = DEFAULT_SAMPLE_SIZE
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
            raise ValueError("owner and cube required")
        collection = '%s__%s' % (owner, cube)
        if admin:
            return self._timeline_admin[collection]
        else:
            return self._timeline_data[collection]

    def valid_meta(self, x, valid_set):
        if isinstance(x, basestring):
            x = [x]
        if not set(x) <= valid_set:
            raise ValueError("invalid meta; "
                             "got (%s). expected: %s" % (x, valid_set))
        return x

    def valid_role(self, roles):
        return self.valid_meta(roles, VALID_ROLES)

    def valid_group(self, groups):
        return self.valid_meta(groups, VALID_GROUPS)

    def valid_action(self, actions):
        return self.valid_meta(actions, VALID_CUBE_ROLE_ACTIONS)

##################### http request #################################
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
        if self.metrique_config.async:
            return self._prepare_async()
        else:
            return super(MetriqueHdlr, self).prepare()

    def write(self, value):
        result = json.dumps(value, ensure_ascii=False)
        super(MetriqueHdlr, self).write(result)

##################### auth #################################
    def get_current_user(self):
        current_user = self.get_secure_cookie("user")
        if current_user:
            return current_user
        else:
            ok, current_user = self._parse_auth_headers()
            if ok:
                self.set_secure_cookie("user", current_user)
                logger.debug('NEW AUTH OK: %s' % current_user)
                return current_user
            else:
                return ok

    def is_admin_user(self, username, password):
        '''
        admin pass is stored in metrique server config
        admin user gets 'rw' to all cubes
        '''
        admin_user = self.metrique_config.admin_user
        admin_password = self.metrique_config.admin_password
        if username == admin_user and password == admin_password:
            logger.debug('AUTH ADMIN: True')
            return True, username
        else:
            return False, username

    def basic(self, username, password):
        if not (username and password):
            return False, username
        if not isinstance(password, basestring):
            raise TypeError(
                "password expected to be a string; got %s" % type(password))
        passhash = self.get_user_profile(username, keys=['passhash'],
                                         one=True)
        if passhash and sha256_crypt.verify(password, passhash):
            logger.debug('AUTH BASIC: True')
            return True, username
        else:
            return False, username

    @staticmethod
    def krb_basic(username, password, krb_realm):
            try:
                authed = kerberos.checkPassword(username, password,
                                                '', krb_realm)
                return authed, username
            except kerberos.BasicAuthError as e:
                logger.error('KRB ERROR: %s' % e)
                return False, username

    def _scrape_username_password(self):
        username = ''
        password = ''
        auth_header = self.request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Basic '):
            auth = base64.decodestring(auth_header[6:])
            username, password = auth.split(':', 2)
        return username, password

    def _parse_basic_auth(self, username, password):
        return self.basic(username, password)

    def _parse_krb_basic_auth(self, username, password):
        krb_auth = self.metrique_config.krb_auth
        krb_realm = self.metrique_config.krb_realm
        if not all((kerberos, krb_auth, krb_realm, username, password)):
            return False, username
        else:
            return self.krb_basic(username, password,
                                  self.metrique_config.krb_realm)

    def _parse_auth_headers(self):
        username, password = self._scrape_username_password()
        _admin, username = self.is_admin_user(username, password)
        _basic, username = self._parse_basic_auth(username, password)
        _krb_basic, username = self._parse_krb_basic_auth(username, password)

        if any((_admin, _basic, _krb_basic)):
            return True, username
        else:
            return False, username

    def _has_cube_role(self, owner, role, cube=None):
        if not cube:
            return False
        self.valid_role(role)
        user_role = self.get_user_profile(self.current_user, keys=['role'])
        if user_role and self.cjoin(owner, cube) in user_role:
            return True
        else:
            return False

    def _is_self(self, user):
        return self.current_user == user

    def _in_group(self, group):
        self.valid_group(group)
        user_group = self.get_user_profile(self.current_user, keys=['group'])
        if user_group and group in user_group:
            return True
        else:
            return False

    def _is_admin(self, owner, cube=None):
        _is_admin = self._is_self('admin')
        _is_group_admin = self._in_group('admin')
        _cube_role = self._has_cube_role(owner,
                                         'admin', cube)
        return any((_is_admin, _is_group_admin, _cube_role))

    def _is_write(self, owner, cube=None):
        _is_admin = self._is_admin(owner, cube)
        _cube_role = self._has_cube_role(owner,
                                         'write', cube)
        return any((_is_admin, _cube_role))

    def _is_read(self, owner, cube=None):
        _is_admin = self._is_admin(owner, cube)
        _cube_role = self._has_cube_role(owner,
                                         'read', cube)
        return any((_is_admin, _cube_role))

    def _requires(self, owner, role_func, cube=None, raise_fail=True):
        assert role_func in (self._is_read, self._is_write,
                             self._is_admin)
        _exists = None
        if owner and cube:
            _exists = self.cube_exists(owner, cube)

        if cube and _exists:
            _is_self = self._is_self(owner, cube)
            _is_role = role_func(owner, cube)
            ok = any((_is_self, _is_role))
        elif not cube:
            # check if they fit the role we're looking for
            ok = role_func(owner)
        else:
            # there's no current owner... cube doesn't exist
            ok = True
        if not ok and raise_fail:
            self._raise(401, "insufficient privleges")
        else:
            return ok

    def _requires_self_admin(self, owner, raise_fail=True):
        _is_admin = self._is_admin(owner)
        _is_self = self._is_self(owner)
        return any((_is_self, _is_admin))

    def _requires_self_read(self, owner, raise_fail=True):
        _is_admin = self._is_admin(owner)
        _is_self = self._is_self(owner)
        _is_read = self._is_read(owner)
        return any((_is_self, _is_read, _is_admin))

    def _requires_owner_admin(self, owner, cube=None, raise_fail=True):
        self._requires(owner=owner, cube=cube, role_func=self._is_admin,
                       raise_fail=raise_fail)

    def _requires_owner_write(self, owner, cube=None, raise_fail=True):
        self._requires(owner=owner, cube=cube, role_func=self._is_write,
                       raise_fail=raise_fail)

    def _requires_owner_read(self, owner, cube=None, raise_fail=True):
        self._requires(owner=owner, cube=cube, role_func=self._is_read,
                       raise_fail=raise_fail)

##################### utils #################################
    def _raise(self, code, msg):
            if code == 401:
                self.set_header('WWW-Authenticate', 'Basic realm="metrique"')
            raise HTTPError(code, msg)

    @staticmethod
    def get_date_pql_string(date, prefix=' and '):
        if date is None:
            return prefix + '_end == None'
        if date == '~':
            return ''

        dt_str = date.replace('T', ' ')
        dt_str = re.sub('(\+\d\d:\d\d)?$', '', dt_str)

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
        if auth and not self.current_user:
            self._raise(403, "authentication failed")
        else:
            pong = self.ping()
        self.write(pong)

    def ping(self):
        logger.debug('got ping @ %s' % datetime.utcnow())
        response = {
            'action': 'ping',
            'response': 'pong',
            #'from_host': FQDN,  # when network is down getting the
            # fqdn with socket module causes hangups
            'current_user': self.current_user,
        }
        return response


class ObsoleteAPIHdlr(MetriqueHdlr):
    ''' RequestHandler for handling obsolete API calls '''
    def get(self):
        self._raise(410, "This API version is no long supported")
