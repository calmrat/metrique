#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import base64
import logging
logger = logging.getLogger(__name__)


import simplejson as json
from tornado.web import RequestHandler, authenticated, HTTPError

from metriqued import query_api, etl_api, users_api, cubes_api
from metriqued.cubes import list_cubes, list_cube_fields
from metriqued.tornadod.auth import is_admin, basic


class MetriqueInitialized(RequestHandler):
    '''
    Template RequestHandler that accepts init parameters
    and unifies json get_argument handling
    '''

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
            logger.debug('GOT CURRENT USER: %s' % user)
            return user
        else:
            return None


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
        username = None
        result = {
            'action': 'login',
            'result': False,
            'username': None,
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

        logger.debug("Parsing auth_headers")
        ok, username = self.parse_auth_headers()

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
            self.set_header('WWW-Authenticate', 'Basic realm="metrique"')
            raise HTTPError(401, "Authentication Required")

    def get(self):
        ''' alias get/post for login '''
        self.post()


class LogoutHandler(MetriqueInitialized):
    '''
    RequestHandler for logging a user out of metrique
    '''
    def post(self):
        self.clear_cookie("user")
        response = {
            'action': 'logout',
            'result': True,
        }
        self.write(response)


class PingHandler(MetriqueInitialized):
    ''' RequestHandler for pings '''
    @authenticated
    def get(self):
        c_user = self.get_current_user()
        pong = query_api.ping(username=c_user)
        self.write(pong)


class RegisterHandler(MetriqueInitialized):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self):
        username = self.get_argument('username')
        password = self.get_argument('password')
        if not (username and password):
            raise HTTPError(500, "username and password REQUIRED")
        return users_api.register(username=username, password=password)


class PasswordChangeHandler(MetriqueInitialized):
    '''
    RequestHandler for updating existing users profile properties
    '''
    @authenticated
    def post(self):
        old_password = self.get_argument('old_password')
        new_password = self.get_argument('new_password')
        username = self.get_current_user()
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
        username = self.get_current_user()
        if not username:
            raise HTTPError(401)
            ## FIXME: add _next redirect argument? to get loop back?
            #print dir(self)
            #self.redirect(self.metrique_config.login_url)
        if owner != username:
            raise HTTPError(403, 'access restricted')
        self.write(cubes_api.register(owner=owner, cube=cube))


class CubeDropHandler(MetriqueInitialized):
    ''' RequestsHandler for droping given cube from timeline '''
    @authenticated
    def delete(self, owner, cube):
        self.write(etl_api.drop_cube(user=owner, cube=cube))


class UserCubeHandler(MetriqueInitialized):
    '''
    RequestHandler for querying about available cubes and cube.fields
    '''
    @authenticated
    def get(self, owner=None, cube=None):
        _mtime = self.get_argument('_mtime')
        exclude_fields = self.get_argument('exclude_fields')
        if not owner:
            result = list_cubes()
        if cube is None:
            # return a list of cubes
            result = list_cubes(user=owner)
        else:
            # return a list of fields in a cube
            # arg = username... return only cubes with 'r' access
            result = list_cube_fields(cube, exclude_fields, _mtime=_mtime)
        self.write(result)


class ETLSaveObjectsHandler(MetriqueInitialized):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def post(self, owner, cube):
        objects = self.get_argument('objects')
        mtime = self.get_argument('mtime')
        result = etl_api.save_objects(user=owner, cube=cube,
                                      objects=objects, mtime=mtime)
        self.write(result)


class ETLRemoveObjectsHandler(MetriqueInitialized):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def delete(self, owner, cube):
        ids = self.get_argument('ids')
        backup = self.get_argument('backup')
        result = etl_api.remove_objects(user=owner, cube=cube,
                                        ids=ids, backup=backup)
        self.write(result)


class ETLIndexHandler(MetriqueInitialized):
    '''
    RequestHandler for ensuring mongodb indexes
    in timeline collection for a given cube
    '''
    @authenticated
    def post(self):
        cube = self.get_argument('cube')
        ensure = self.get_argument('ensure')
        drop = self.get_argument('drop')
        return etl_api.index(cube=cube, ensure=ensure, drop=drop)


class ETLActivityImportHandler(MetriqueInitialized):
    '''
    RequestHandler for building pre-calculated
    object timelines given a 'activity history'
    data source that can be used to recreate
    objects in time
    '''
    @authenticated
    def post(self):
        cube = self.get_argument('cube')
        ids = self.get_argument('ids')
        return etl_api.activity_import(cube=cube, ids=ids)


class QueryAggregateHandler(MetriqueInitialized):
    '''
    RequestHandler for running mongodb aggregation
    framwork pipeines against a given cube
    '''
    @authenticated
    def get(self):
        cube = self.get_argument('cube')
        pipeline = self.get_argument('pipeline', '[]')
        return query_api.aggregate(cube, pipeline)


class QueryFetchHandler(MetriqueInitialized):
    ''' RequestHandler for fetching lumps of cube data '''
    @authenticated
    def get(self):
        cube = self.get_argument('cube')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        sort = self.get_argument('sort', None)
        skip = self.get_argument('skip', 0)
        limit = self.get_argument('limit', 0)
        oids = self.get_argument('oids', [])
        return query_api.fetch(cube=cube, fields=fields, date=date,
                               sort=sort, skip=skip, limit=limit, oids=oids)


class QueryCountHandler(MetriqueInitialized):
    '''
    RequestHandler for returning back simple integer
    counts of objects matching the given query
    '''
    @authenticated
    def get(self):
        cube = self.get_argument('cube')
        query = self.get_argument('query')
        date = self.get_argument('date', None)
        return query_api.count(cube=cube, query=query, date=date)


class QueryFindHandler(MetriqueInitialized):
    '''
    RequestHandler for returning back object
    matching the given query
    '''
    @authenticated
    def get(self):
        cube = self.get_argument('cube')
        query = self.get_argument('query')
        fields = self.get_argument('fields', '')
        date = self.get_argument('date')
        sort = self.get_argument('sort', None)
        one = self.get_argument('one', False)
        explain = self.get_argument('explain', False)
        merge_versions = self.get_argument('merge_versions', True)
        return query_api.find(cube=cube,
                              query=query,
                              fields=fields,
                              date=date,
                              sort=sort,
                              one=one,
                              explain=explain,
                              merge_versions=merge_versions)


class QueryDeptreeHandler(MetriqueInitialized):
    '''
    RequestHandler for returning back the list of
    oids matching the given tree.
    '''
    @authenticated
    def get(self):
        cube = self.get_argument('cube')
        field = self.get_argument('field')
        oids = self.get_argument('oids')
        date = self.get_argument('date')
        level = self.get_argument('level')
        return query_api.deptree(cube=cube,
                                 field=field,
                                 oids=oids,
                                 date=date,
                                 level=level)


class QueryDistinctHandler(MetriqueInitialized):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self):
        cube = self.get_argument('cube')
        field = self.get_argument('field')
        return query_api.distinct(cube=cube, field=field)


class QuerySampleHandler(MetriqueInitialized):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @authenticated
    def get(self):
        cube = self.get_argument('cube')
        size = self.get_argument('size')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        return query_api.sample(cube=cube, size=size, fields=fields,
                                date=date)
