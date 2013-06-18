#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
import base64
logger = logging.getLogger(__name__)

from functools import wraps
import simplejson as json
import tornado
import traceback

from metrique.server.defaults import VALID_PERMISSIONS
from metrique.server import query_api, etl_api, users_api

from metrique.tools import hash_password
from metrique.tools.json import Encoder, decoder


# FIXME: create jobsave meta data here! rather rapping handler gets
def async(f):
    '''
    Decorator for enabling async Tornado.Handlers
    But if not metrique.config.async: disable async

    Requires: futures
    '''
    @tornado.web.asynchronous
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.proxy.metrique_config.async:
            def future_end(future):
                try:
                    _result = future.result()
                    # Result is always expected to be json encoded!
                    result = json.dumps(_result, cls=Encoder,
                                        ensure_ascii=False)
                except Exception:
                    result = traceback.format_exc()
                    logger.error(result)
                    raise tornado.web.HTTPError(500, result)
                finally:
                    self.write(result)
                self.finish()

            future = self.proxy.executor.submit(f, self, *args, **kwargs)
            tornado.ioloop.IOLoop.instance().add_future(future, future_end)
        else:
            _result = f(self, *args, **kwargs)
            # Result is always expected to be json encoded!
            result = json.dumps(_result, cls=Encoder, ensure_ascii=False)
            self.write(result)
            self.finish()
    return wrapper


def request_authentication(handler):
    ''' Helper-Function for settig 401 - Request for authentication '''
    handler.set_status(401)
    handler.set_header('WWW-Authenticate', 'Basic realm="Metrique"')
    return False


def authenticate(handler, username, password, permissions):
    ''' Helper-Function for determining whether a given
        user:password:permissions combination provides
        client with enough privleges to execute
        the requested command against the given cube '''
    # if auth isn't on, let anyone do anything!
    if not handler.proxy.metrique_config.auth:
        return True

    # GLOBAL DEFAULT
    cube = handler.get_argument('cube')
    if username == handler.proxy.metrique_config.admin_user:
        admin_password = handler.proxy.metrique_config.admin_password
        if password == admin_password:
            # admin pass is stored in plain text
            # we're admin user and so we get 'rw' to all cubes
            return True

    # user is not admin... lookup username in auth_keys
    def cube_check(cube, lookup):
        spec = {'_id': cube,
                lookup: {'$exists': True}}
        logger.debug("Cube Check: spec (%s)" % spec)
        return handler.proxy.mongodb_config.c_auth_keys.find_one(spec)
    udoc = cube_check(cube, username)
    adoc = cube_check('__all__', '__all__')

    if udoc:
        user = udoc[username]
    elif adoc:
        user = adoc['__all__']
    else:
        return False

    # permissions is a single string
    assert isinstance(permissions, basestring)
    VP = VALID_PERMISSIONS
    has_perms = VP.index(user['permissions']) >= VP.index(permissions)

    if not user['password'] and has_perms:
        return True

    _, p_hash = hash_password(password, user['salt'])
    p_match = user['password'] == p_hash
    if p_match and has_perms:
        # password is defined, make sure user's pass matches it
        # and that the user has the right permissions defined
        return True
    return False


def auth(permissions='r'):
    ''' Decorator for auth dependent Tornado.Handlers '''
    def decorator(f):
        @wraps(f)
        def wrapper(handler, *args, **kwargs):
            auth_header = handler.request.headers.get('Authorization')
            if auth_header is None or not auth_header.startswith('Basic '):
                #No HTTP Basic Authentication header
                return request_authentication(handler)

            auth = base64.decodestring(auth_header[6:])
            username, password = auth.split(':', 2)
            privleged = authenticate(handler, username, password, permissions)
            logger.debug("User (%s): Privleged (%s)" % (username, privleged))
            if privleged:
                return f(handler, *args, **kwargs)
            else:
                return request_authentication(handler)
        return wrapper
    return decorator


class MetriqueInitialized(tornado.web.RequestHandler):
    '''
        Template RequestHandler that accepts init parameters
        and unifies json get_argument handling
    '''

    def initialize(self, proxy):
        '''
        Paremeters
        ----------
        proxy : HTTPServer (MetriqueServer) Obj
            A pointer to the running metrique server namespace
        '''
        self.proxy = proxy

    def get_argument(self, key, default=None):
        '''
            Assume incoming arguments are json encoded,
            get_arguments should always deserialize
            on the way in
        '''
        # arguments are expected to be json encoded!
        _arg = super(MetriqueInitialized, self).get_argument(key, default)

        if _arg is None:
            return _arg

        try:
            arg = json.loads(_arg, object_hook=decoder)
        except Exception as e:
            raise ValueError("Invalid JSON content (%s): %s" % (type(_arg), e))
        return arg


class PingHandler(MetriqueInitialized):
    ''' RequestHandler for pings'''
    @async
    def get(self):
        return self.proxy.ping()


class JobStatusHandler(MetriqueInitialized):
    ''' RequestHandler wrapper handling client job status requests '''
    @auth('rw')
    @async
    def get(self, job_key):
        raise NotImplementedError
        # FIXME
        # the api call below '.job.status' is not available yet
        job_json = self.proxy.job.status(job_key)
        job = json.loads(job_json)
        return job['result']


class QueryAggregateHandler(MetriqueInitialized):
    '''
        RequestHandler for running mongodb aggregation
        framwork pipeines against a given cube
    '''
    @auth()
    @async
    def get(self):
        cube = self.get_argument('cube')
        pipeline = self.get_argument('pipeline', '[]')
        return query_api.aggregate(cube, pipeline)


class QueryFetchHandler(MetriqueInitialized):
    ''' RequestHandler for fetching lumps of cube data '''
    @auth()
    @async
    def get(self):
        cube = self.get_argument('cube')
        fields = self.get_argument('fields')
        sort = self.get_argument('sort', None)
        skip = self.get_argument('skip', 0)
        limit = self.get_argument('limit', 0)
        ids = self.get_argument('ids', [])
        return query_api.fetch(cube=cube, fields=fields, sort=sort,
                               skip=skip, limit=limit, ids=ids)


class QueryCountHandler(MetriqueInitialized):
    '''
        RequestHandler for returning back simple integer
        counts of objects matching the given query
    '''
    @auth()
    @async
    def get(self):
        cube = self.get_argument('cube')
        query = self.get_argument('query')
        return query_api.count(cube, query)


class QueryFindHandler(MetriqueInitialized):
    '''
        RequestHandler for returning back object
        matching the given query
    '''
    @auth()
    @async
    def get(self):
        cube = self.get_argument('cube')
        query = self.get_argument('query')
        fields = self.get_argument('fields', '')
        date = self.get_argument('date')
        most_recent = self.get_argument('most_recent', True)
        sort = self.get_argument('sort', None)
        one = self.get_argument('one', False)
        return query_api.find(cube=cube,
                              query=query,
                              fields=fields,
                              date=date,
                              most_recent=most_recent,
                              sort=sort,
                              one=one)


class UsersAddHandler(MetriqueInitialized):
    '''
        RequestHandler for managing user access control
        lists for a given cube
    '''
    @auth('admin')
    @async
    def get(self):
        cube = self.get_argument('cube')
        user = self.get_argument('user')
        password = self.get_argument('password')
        permissions = self.get_argument('permissions', 'r')
        return users_api.add(cube, user,
                             password, permissions)


class ETLIndexWarehouseHandler(MetriqueInitialized):
    '''
        RequestHandler for ensuring mongodb indexes
        in warehouse for a given cube
    '''
    @auth('rw')
    @async
    def get(self):
        cube = self.get_argument('cube')
        fields = self.get_argument('fields', '')
        return etl_api.index_warehouse(cube, fields)


class ETLSnapshotHandler(MetriqueInitialized):
    '''
        RequestHandler for taking a snapshot
        of warehouse data and copying objects
        to the timeline
    '''
    @auth('rw')
    @async
    def get(self):
        cube = self.get_argument('cube')
        ids = self.get_argument('ids')
        return etl_api.snapshot(cube=cube, ids=ids)


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
        RequestHandler for saving a given
        object to a metrique server cube
    '''
    @auth('rw')
    @async
    def post(self):
        cube = self.get_argument('cube')
        objects = self.get_argument('objects')
        update = self.get_argument('update')
        return etl_api.save_objects(cube=cube, objects=objects, update=update)


class CubesHandler(MetriqueInitialized):
    '''
        RequestHandler for querying about
        available cubes and cube.fields
    '''
    @auth('r')
    @async
    def get(self):
        cube = self.get_argument('cube')
        if cube is None:
            # return a list of cubes
            return self.proxy.get_cubes()
        else:
            # return a list of fields in a cube
            result = self.proxy.get_fields(cube)
            return result
