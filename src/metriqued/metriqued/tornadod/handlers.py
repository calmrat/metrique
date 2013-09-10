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

from metriqued import query_api, etl_api, users_api, cubes_api
from metriqued.cubes import list_cubes, list_cube_fields
from metriqued.cubes import get_auth_keys, get_collection
from metriqued.tornadod.auth import is_admin, basic

FQDN = getfqdn()
AUTH_KEYS = get_auth_keys()


class MetriqueInitialized(RequestHandler):
    '''
    Template RequestHandler that accepts init parameters
    and unifies json get_argument handling
    '''

    def ping(self, **kwargs):
        logger.debug('got ping @ %s' % datetime.utcnow())
        response = {
            'action': 'ping',
            'response': 'pong',
            'from_host': FQDN,
        }
        response.update(kwargs)
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
        _arg = super(MetriqueInitialized, self).get_argument(key, default)

        if _arg and with_json:
            try:
                arg = json.loads(_arg)
            except Exception as e:
                raise ValueError(
                    "Invalid JSON content (%s): %s\n%s" % (
                        type(_arg), e, _arg))
        else:
            arg = _arg

        return arg

    def write(self, value):
        result = json.dumps(value, ensure_ascii=False)
        super(MetriqueInitialized, self).write(result)

    def get_current_user(self):
        user_json = self.get_secure_cookie("user")
        if user_json:
            user = json.loads(user_json)
            logger.debug('CURRENT USER: %s' % user)
            return user
        else:
            return None

    def _raise(self, code, msg):
            if code == 401:
                self.set_header('WWW-Authenticate', 'Basic realm="metrique"')
            raise HTTPError(code, msg)

    def _has_cube_role(self, current_user, owner, cube, role):
        # FIXME: CHECK THAT IT"S ONE OF THE VALID LEVELS
        # admin, read, __write__
        # WARNING: create=True creates a new mongodb, lazilly
        #_cube = get_collection(owner, cube, create=True)
        try:
            _cube = get_collection(owner, cube)
        except HTTPError:
            return 0
        else:
            spec = {role: current_user}
            return _cube.find(spec).count()

    def _is_owner(self, current_user, owner, cube):
        # WARNING: create=True creates a new mongodb, lazilly
        #_cube = get_collection(owner, cube, create=True)
        _cube = get_collection(owner, cube)
        spec = {'__owner__': current_user}
        return _cube.find(spec).count()

    def _is_user(self, current_user, user):
        # FIXME: make this work for any user in 'admin' group OR admin
        return current_user == user

    def _in_group(self, current_user, group):
        spec = {'_id': current_user, 'groups': group}
        return AUTH_KEYS.find(spec).count()

    def _is_admin(self, current_user, owner, cube):
        _is_admin = self._is_user(current_user, 'admin')
        _is_group_admin = self._in_group(current_user, 'admin')
        _cube_role = self._has_cube_role(current_user, owner,
                                         cube, '__admin__')
        return any((_is_admin, _is_group_admin, _cube_role))

    def _requires_owner_admin(self, owner, cube):
        current_user = self.get_current_user()
        _exists = self._cube_exists(owner, cube)
        if _exists:
            _is_owner = self._is_owner(current_user, owner, cube)
            _is_admin = self._is_admin(current_user, owner, cube)
            ok = any((_is_owner, _is_admin))
        else:
            # there's no current owner...
            ok = True
        if not ok:
            self._raise(401, "this requires admin privleges")
        return ok

    def _requires_owner_read(self, owner, cube):
        current_user = self.get_current_user()
        _exists = self._cube_exists(owner, cube)
        if _exists:
            _is_owner = self._is_owner(current_user, owner, cube)
            _can_read = self._can_read(current_user, owner, cube)
            ok = any((_is_owner, _can_read))
        else:
            # there's no current owner...
            ok = True
        if not ok:
            self._raise(401, "this requires admin privleges")
        return ok

    def _is_owner_or_write(self, username=None, owner=None, cube=None):
        current_user = self.get_current_user()
        role = '__write__'
        _is_owner = self._is_owner(current_user, owner, cube)
        _can_do = self._has_cube_role(current_user, owner, cube, role)
        return any((_is_owner, _can_do))

    def _cube_exists(self, owner, cube):
        try:
            _cube = get_collection(owner, cube)
        except HTTPError:
            return 0
        else:
            return _cube.find({'$exists': {'__created__': 1}}).count()


class LoginHandler(MetriqueInitialized):
    '''
    RequestHandler for logging a user into metrique
    '''
    def _scrape_username_password(self):
        username = ''
        password = ''
        auth_header = self.request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Basic '):
            auth = base64.decodestring(auth_header[6:])
            username, password = auth.split(':', 2)
        return username, password

    def _parse_admin_auth(self):
        username, password = self._scrape_username_password()
        admin_user = self.metrique_config.admin_user
        admin_password = self.metrique_config.admin_password
        if is_admin(admin_user, admin_password, username, password):
            return True, username
        else:
            return False, username

    def _parse_basic_auth(self):
        username, password = self._scrape_username_password()
        if basic(username, password):
            return True, username
        else:
            return False, username

    def parse_auth_headers(self):
        admin, username = self._parse_admin_auth()
        basic, username = self._parse_basic_auth()

        if admin:
            return True, username
        elif basic:
            return True, username
        else:
            return False, username

    def post(self):
        result = {
            'action': 'login',
            'result': False,
        }

        # FIXME: IF THE USER IS USING KERB
        # OR WE OTHERWISE ALREADY KNOW WHO
        # THEY ARE, CHECK THAT THEY HAVE
        # A USER ACCOUNT NOW; IF NOT, CREATE
        # ONE FOR THEM

        current_user = self.get_current_user()
        if current_user:
            logger.debug('USER SESSION OK: %s' % current_user)
            result.update({'result': None})
            result.update({'username': current_user})
            self.write(result)
            # FIXME: update cookie expiration date
            return

        ok, username = self.parse_auth_headers()
        result.update({'username': username})
        logger.debug("AUTH HEADERS ... [%s] %s" % (username, ok))

        if ok:
            result.update({'result': True})
            username_json = json.dumps(username, ensure_ascii=False)
            self.set_secure_cookie("user", username_json)

            _next = self.get_argument('next', with_json=False)
            if _next:
                # go ahead and redirect if we expected to be somewhere else
                self.redirect(_next)
            else:
                # write out the result dict
                self.write(result)
                # necessary only in cases of running with @async
                #self.finish()
        else:
            self._raise(401, "This requires admin privleges")

    def get(self):
        ''' alias get/post for login '''
        self.post()


class LogoutHandler(MetriqueInitialized):
    '''
    RequestHandler for logging a user out of metrique
    '''
    @authenticated
    def post(self):
        self.clear_cookie("user")
        response = {
            'action': 'logout',
            'result': True,
        }
        self.write(response)


class PingHandler(MetriqueInitialized):
    ''' RequestHandler for pings '''
    def get(self):
        c_user = self.get_current_user()
        pong = self.ping(username=c_user)
        self.write(pong)


class RegisterHandler(MetriqueInitialized):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self):
        # FIXME: add a 'cube registration' lock
        username = self.get_argument('username')
        password = self.get_argument('password')
        if not (username and password):
            raise HTTPError(400, "username and password REQUIRED")

        result = users_api.register(username=username,
                                    password=password)
        # FIXME: DO THIS FOR ALL HANDLERS! REST REST REST
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
        self.set_status(201, 'Registration successful: %s' % username)
        self.write(result)


class PasswordChangeHandler(MetriqueInitialized):
    '''
    RequestHandler for updating existing users profile properties
    '''
    @authenticated
    def post(self, username):
        old_password = self.get_argument('old_password')
        new_password = self.get_argument('new_password')
        current_user = self.get_current_user()

        if not (username and old_password) or current_user != username:
            # if this user is logged in... let them change
            # the password without specifying the existing one
            self._raise(401, "This requires admin privleges")

        result = users_api.passwd(username=username,
                                  old_password=old_password,
                                  new_password=new_password)
        if result:
            self.clear_cookie("user")
        if self.metrique_config.login_url:
            self.redirect(self.metrique_config.login_url)
        else:
            return result


class UsersAddHandler(MetriqueInitialized):
    '''
    RequestHandler for managing user access control for a given cube
    '''
    @authenticated
    def post(self):
        username = self.get_argument('username')
        cube = self.get_argument('cube')
        role = self.get_argument('role', 'r')
        return users_api.add(username=username,
                             cube=cube, role=role)


class CubeRegisterHandler(MetriqueInitialized):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        if self._cube_exists(owner, cube):
            self._raise(409, "this cube already exists")
        self.write(cubes_api.register(owner=owner, cube=cube))


class CubeDropHandler(MetriqueInitialized):
    ''' RequestsHandler for droping given cube from timeline '''
    @authenticated
    def delete(self, owner, cube):
        self.write(etl_api.drop_cube(owner=owner, cube=cube))


class UserCubeHandler(MetriqueInitialized):
    '''
    RequestHandler for querying about available cubes and cube.fields
    '''
    @authenticated
    def get(self, owner, cube):
        _mtime = self.get_argument('_mtime')
        exclude_fields = self.get_argument('exclude_fields')
        if not owner:
            result = list_cubes()
        elif cube is None:
            # return a list of cubes
            result = list_cubes(owner=owner)
        else:
            # return a list of fields in a cube
            # arg = username... return only cubes with 'r' access
            result = list_cube_fields(owner, cube,
                                      exclude_fields, _mtime=_mtime)
        self.write(result)


class UserHandler(MetriqueInitialized):
    '''
    RequestHandler for querying about available cubes and cube.fields

    STATE: UNSTABLE
    '''
    @authenticated
    def get(self, owner=None):
        result = list_cubes(owner=owner)
        self.write(result)


class UserUpdateHandler(MetriqueInitialized):
    '''
    RequestHandler for querying about available cubes and cube.fields

    STATE: UNSTABLE
    '''
    @authenticated
    def get(self, username=None):
        backup = self.get_argument('backup')
        kwargs = self.request.arguments
        result = users_api.update(username=username, backup=backup,
                                  **kwargs)
        self.write(result)


class ETLSaveObjectsHandler(MetriqueInitialized):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def post(self, owner, cube):
        objects = self.get_argument('objects')
        mtime = self.get_argument('mtime')
        result = etl_api.save_objects(owner=owner, cube=cube,
                                      objects=objects, mtime=mtime)
        self.write(result)


class ETLRemoveObjectsHandler(MetriqueInitialized):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        ids = self.get_argument('ids')
        backup = self.get_argument('backup')
        result = etl_api.remove_objects(owner=owner, cube=cube,
                                        ids=ids, backup=backup)
        self.write(result)


class ETLIndexHandler(MetriqueInitialized):
    '''
    RequestHandler for ensuring mongodb indexes
    in timeline collection for a given cube
    '''
    @authenticated
    def get(self, owner, cube):
        self.write(etl_api.index(owner=owner, cube=cube))

    @authenticated
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        ensure = self.get_argument('ensure')
        self.write(etl_api.index(owner=owner, cube=cube, ensure=ensure))

    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        drop = self.get_argument('drop')
        self.write(etl_api.index(owner=owner, cube=cube, drop=drop))


class ETLActivityImportHandler(MetriqueInitialized):
    '''
    RequestHandler for building pre-calculated
    object timelines given a 'activity history'
    data source that can be used to recreate
    objects in time
    '''
    @authenticated
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        ids = self.get_argument('ids')
        result = etl_api.activity_import(owner=owner, cube=cube, ids=ids)
        self.write(result)


class QueryFindHandler(MetriqueInitialized):
    '''
    RequestHandler for returning back object
    matching the given query
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        query = self.get_argument('query')
        fields = self.get_argument('fields', '')
        date = self.get_argument('date')
        sort = self.get_argument('sort', None)
        one = self.get_argument('one', False)
        explain = self.get_argument('explain', False)
        merge_versions = self.get_argument('merge_versions', True)
        result = query_api.find(owner=owner,
                                cube=cube,
                                query=query,
                                fields=fields,
                                date=date,
                                sort=sort,
                                one=one,
                                explain=explain,
                                merge_versions=merge_versions)
        self.write(result)


class QueryAggregateHandler(MetriqueInitialized):
    '''
    RequestHandler for running mongodb aggregation
    framwork pipeines against a given cube
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        pipeline = self.get_argument('pipeline')
        if not pipeline:
            # alias for pipeline
            pipeline = self.get_argument('query', '[]')
        result = query_api.aggregate(owner=owner, cube=cube,
                                     pipeline=pipeline)
        self.write(result)


class QueryFetchHandler(MetriqueInitialized):
    ''' RequestHandler for fetching lumps of cube data '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        sort = self.get_argument('sort', None)
        skip = self.get_argument('skip', 0)
        limit = self.get_argument('limit', 0)
        oids = self.get_argument('oids', [])
        result = query_api.fetch(owner=owner, cube=cube,
                                 fields=fields, date=date,
                                 sort=sort, skip=skip,
                                 limit=limit, oids=oids)
        self.write(result)


class QueryCountHandler(MetriqueInitialized):
    '''
    RequestHandler for returning back simple integer
    counts of objects matching the given query
    '''
    @authenticated
    def get(self, owner, cube):
        query = self.get_argument('query')
        date = self.get_argument('date', None)
        result = query_api.count(owner=owner, cube=cube,
                                 query=query, date=date)
        self.write(result)


class QueryDeptreeHandler(MetriqueInitialized):
    '''
    RequestHandler for returning back the list of
    oids matching the given tree.
    '''
    @authenticated
    def get(self, owner, cube):
        field = self.get_argument('field')
        oids = self.get_argument('oids')
        date = self.get_argument('date')
        level = self.get_argument('level')
        result = query_api.deptree(owner=owner, cube=cube,
                                   field=field, oids=oids,
                                   date=date, level=level)
        self.write(result)


class QueryDistinctHandler(MetriqueInitialized):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self, owner, cube):
        field = self.get_argument('field')
        result = query_api.distinct(owner=owner, cube=cube,
                                    field=field)
        self.write(result)


class QuerySampleHandler(MetriqueInitialized):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self, owner, cube):
        size = self.get_argument('size')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        result = query_api.sample(owner=owner, cube=cube,
                                  size=size, fields=fields,
                                  date=date)
        self.write(result)
