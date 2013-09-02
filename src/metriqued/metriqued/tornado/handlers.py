#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
import base64
logger = logging.getLogger(__name__)

from functools import wraps
import kerberos
from passlib.hash import sha256_crypt
import simplejson as json
from tornado.web import RequestHandler, asynchronous, HTTPError
from tornado.ioloop import IOLoop
import traceback

from metriqued.defaults import VALID_PERMISSIONS as VP
from metriqued import query_api, etl_api, users_api


def async(f):
    '''
    Decorator for enabling async Tornado.Handlers
    If not metrique.config.async: disable async

    Requires: futures

    Uses: async.threading
    '''
    @asynchronous
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.proxy.metrique_config.async:
            def future_end(future):
                try:
                    _result = future.result()
                    logger.debug('JSON dump: START... ')
                    result = json.dumps(_result, ensure_ascii=False)
                    logger.debug('JSON dump: DONE')
                except Exception:
                    result = traceback.format_exc()
                    logger.error(result)
                    raise HTTPError(500, result)
                finally:
                    self.write(result)
                self.finish()

            future = self.proxy.executor.submit(f, self, *args, **kwargs)
            IOLoop.instance().add_future(future, future_end)
        else:
            _result = f(self, *args, **kwargs)
            logger.debug('JSON dump: START... ')
            result = json.dumps(_result, ensure_ascii=False)
            logger.debug('JSON dump: DONE... ')
            self.write(result)
            self.finish()
    return wrapper


def request_authentication(handler):
    ''' Helper-Function for settig 401 - Request for authentication '''
    handler.set_status(401)
    handler.set_header('WWW-Authenticate', 'Basic realm="Metrique"')


def _auth_admin(handler, username, password):
    '''
    admin pass is stored in metrique server config
    admin user gets 'rw' to all cubes
    '''
    admin_user = handler.proxy.metrique_config.admin_user
    admin_pass = handler.proxy.metrique_config.admin_password
    if username == admin_user:
        if password == admin_pass:
            return True
        else:
            return -1
    else:
        return 0


def _auth_kerb(handler, username, password):
    if not password:
        return -1
    krb_realm = handler.proxy.metrique_config.krb_realm
    if not krb_realm:
        return 0

    try:
        ret = kerberos.checkPassword(username,
                                     password, '',
                                     krb_realm)
        return ret
    except kerberos.BasicAuthError as e:
        logger.debug('KRB ERROR: %s' % e)
        return -1


def _auth_basic(handler, password, user_dict):
    if not (password and user_dict and user_dict.get('password')):
        # only authenticate if we actually have a password
        # and user_dict.password to compare
        return -1
    else:
        p_hash = user_dict.get('password')
        return sha256_crypt.verify(password, p_hash)


def _get_resource_acl(handler, resource, lookup):
    ''' Check if user is listed to access to a given resource '''
    resource = [resource, '__all__']
    _lookup = [{lookup:  {'$exists': True}},
               {'__all__': {'$exists': True}}]
    spec = {'_id': {'$in': resource},
            '$or': _lookup}
    logger.debug("Cube Check: spec (%s)" % spec)
    return handler.proxy.mongodb_config.c_auth_keys.find_one(spec)


def _check_acl(permissions, user):
    try:
        return VP.index(user['permissions']) >= VP.index(permissions)
    except (TypeError, KeyError):
        # permissions is not defined; assume they're unpermitted
        return -1


def _check_perms(doc, username, cube, permissions):
    if doc and '__all__' in doc:
        user = doc['__all__']
        is_ok = _check_acl(permissions, user)
        no_auth = True
    elif doc:
        user = doc[username]
        is_ok = _check_acl(permissions, user)
        no_auth = False
    elif cube:
        user = {}
        is_ok = -1
        no_auth = False
    return user, is_ok, no_auth


def authenticate(handler, username, password, permissions):
    ''' Helper-Function for determining whether a given
        user:password:permissions combination provides
        client with enough privleges to execute
        the requested command against the given cube
    '''
    cube = handler.get_argument('cube')

    is_admin = _auth_admin(handler, username, password)
    if is_admin in [True, -1]:
        # ... or if user is admin with correct admin pass
        logger.debug('AUTH OK: admin')
        return is_admin

    doc = _get_resource_acl(handler, cube, username)
    user, is_ok, no_auth = _check_perms(doc, username, cube, permissions)

    if not is_ok:
        return is_ok
    elif no_auth:
        return True
    elif _auth_kerb(handler, username, password) is True:
        # or if user is kerberous auth'd
        logger.debug(
            'AUTH OK: krb (%s:%s)' % (username, cube))
        return True
    elif _auth_basic(handler, password, user) is True:
        # or if the user is authed by metrique (built-in; auth_keys)
        logger.debug(
            'AUTH OK: basic (%s:%s)' % (username, cube))
        return True
    else:
        return -1


def auth(permissions='r'):
    ''' Decorator for auth dependent Tornado.Handlers '''
    # permissions must be a single string
    assert isinstance(permissions, basestring)
    assert permissions in VP

    def decorator(f):
        @wraps(f)
        def wrapper(handler, *args, **kwargs):
            if not handler.proxy.metrique_config.auth:
                # if auth isn't on, let anyone do anything!
                return f(handler, *args, **kwargs)

            auth_header = handler.request.headers.get('Authorization')
            if auth_header is None or not auth_header.startswith('Basic '):
                # No HTTP Basic Authentication header
                return request_authentication(handler)

            auth = base64.decodestring(auth_header[6:])
            username, password = auth.split(':', 2)

            privleged = authenticate(handler, username, password, permissions)
            logger.debug("User (%s): Privleged (%s)" % (username, privleged))
            if privleged in [True, 1]:
                return f(handler, *args, **kwargs)
            else:
                raise HTTPError(401)
        return wrapper
    return decorator


class MetriqueInitialized(RequestHandler):
    '''
    Template RequestHandler that accepts init parameters
    and unifies json get_argument handling
    '''

    def initialize(self, proxy):
        '''
        :param HTTPServer proxy:
            A pointer to the running metrique server instance
        '''
        self.proxy = proxy

    def get_argument(self, key, default=None):
        '''
        Assume incoming arguments are json encoded,
        get_arguments should always deserialize on the way in
        '''
        # arguments are expected to be json encoded!
        _arg = super(MetriqueInitialized, self).get_argument(key, default)

        if _arg is None:
            return _arg

        try:
            arg = json.loads(_arg)
        except Exception as e:
            raise ValueError("Invalid JSON content (%s): %s" % (type(_arg), e))
        return arg


class PingHandler(MetriqueInitialized):
    ''' RequestHandler for pings '''
    @async
    def get(self):
        return self.proxy.ping()


class QueryAggregateHandler(MetriqueInitialized):
    '''
    RequestHandler for running mongodb aggregation
    framwork pipeines against a given cube
    '''
    @auth('r')
    @async
    def get(self):
        cube = self.get_argument('cube')
        pipeline = self.get_argument('pipeline', '[]')
        return query_api.aggregate(cube, pipeline)


class QueryFetchHandler(MetriqueInitialized):
    ''' RequestHandler for fetching lumps of cube data '''
    @auth('r')
    @async
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
    @auth('r')
    @async
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
    @auth('r')
    @async
    def get(self):
        cube = self.get_argument('cube')
        query = self.get_argument('query')
        fields = self.get_argument('fields', '')
        date = self.get_argument('date')
        sort = self.get_argument('sort', None)
        one = self.get_argument('one', False)
        explain = self.get_argument('explain', False)
        return query_api.find(cube=cube,
                              query=query,
                              fields=fields,
                              date=date,
                              sort=sort,
                              one=one,
                              explain=explain)


class QueryDeptreeHandler(MetriqueInitialized):
    '''
    RequestHandler for returning back the list of
    oids matching the given tree.
    '''
    @auth('r')
    @async
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
    @auth('r')
    @async
    def get(self):
        cube = self.get_argument('cube')
        field = self.get_argument('field')
        return query_api.distinct(cube=cube, field=field)


class QuerySampleHandler(MetriqueInitialized):
    '''
    RequestHandler for fetching distinct token values for a
    given cube.field
    '''
    @auth('r')
    @async
    def get(self):
        cube = self.get_argument('cube')
        size = self.get_argument('size')
        fields = self.get_argument('fields')
        date = self.get_argument('date')
        return query_api.sample(cube=cube, size=size, fields=fields,
                                date=date)


class UsersAddHandler(MetriqueInitialized):
    '''
    RequestHandler for managing user access control for a given cube
    '''
    @auth('admin')
    @async
    def get(self):
        cube = self.get_argument('cube')
        user = self.get_argument('user')
        password = self.get_argument('password')
        permissions = self.get_argument('permissions', 'r')
        return users_api.add(user, password, permissions, cube)


class ETLIndexHandler(MetriqueInitialized):
    '''
    RequestHandler for ensuring mongodb indexes
    in timeline collection for a given cube
    '''
    @auth('rw')
    @async
    def get(self):
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
    @auth('rw')
    @async
    def get(self):
        cube = self.get_argument('cube')
        ids = self.get_argument('ids')
        return etl_api.activity_import(cube=cube, ids=ids)


class ETLSaveObjects(MetriqueInitialized):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @auth('rw')
    @async
    def post(self):
        cube = self.get_argument('cube')
        objects = self.get_argument('objects')
        update = self.get_argument('update')
        mtime = self.get_argument('mtime')
        return etl_api.save_objects(cube=cube, objects=objects, update=update,
                                    mtime=mtime)


class ETLRemoveObjects(MetriqueInitialized):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @auth('rw')
    @async
    def delete(self):
        cube = self.get_argument('cube')
        ids = self.get_argument('ids')
        backup = self.get_argument('backup')
        return etl_api.remove_objects(cube=cube, ids=ids,
                                      backup=backup)


class ETLCubeDrop(MetriqueInitialized):
    ''' RequestsHandler for droping given cube from timeline '''
    @auth('rw')
    @async
    def delete(self):
        cube = self.get_argument('cube')
        return etl_api.drop(cube=cube)


class CubeHandler(MetriqueInitialized):
    '''
    RequestHandler for querying about available cubes and cube.fields
    '''
    @auth('r')
    @async
    def get(self):
        cube = self.get_argument('cube')
        _mtime = self.get_argument('_mtime')
        exclude_fields = self.get_argument('exclude_fields')
        if cube is None:
            # return a list of cubes
            return self.proxy.list_cubes()
        else:
            # return a list of fields in a cube
            return self.proxy.list_cube_fields(cube,
                                               exclude_fields,
                                               _mtime)
