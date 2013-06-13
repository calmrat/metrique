#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import os
import requests as rq
import simplejson as json
from threading import Thread

from metrique.client.result import Result
from metrique.client.config import Config
from metrique.tools.json import Encoder

CONFIG_FILE = 'client_http'

# FIXME: IDEAS
# commands should return back an object immediately which
# runs the command and sets obj.result when complete
# fetch results could be an iterator? fetching only X items at a time


class BaseClient(object):
    '''
    Base class that other metrique api wrapper sub-classes
    use to call special, shared call of _get (http request)
    '''
    def __init__(self, config_dir=None, config_file=None, *args, **kwargs):
        if not config_file:
            config_file = CONFIG_FILE
        self._config_dir = config_dir
        self._config_file = config_file
        self.config = Config(config_file, config_dir)
        self._command = ''
        self.background = False

    def _get(self, *args, **kwargs):
        # arguments are expected to be json encoded!
        kwargs_json = dict([(k, json.dumps(v, cls=Encoder, ensure_ascii=False))
                            for k, v in kwargs.items()])

        url = os.path.join(self.config.api_url, self._command, *args)
        logger.debug("Connecting to URL: %s" % url)
        if self.background:
            # FIXME: save the threads and use (eg)
            # multiprocessing.pool.ThreadPool... result.get()
            # to get back the results later if the client wants...
            t = Thread(target=rq.get, kwargs={'url': url,
                                              'params': kwargs_json,
                                              'verify': False})
            t.daemon = True
            t.start()
            return
        else:
            # verify = False means we don't care about SSL CA
            try:
                _response = rq.get(url, params=kwargs_json, verify=False)
            except rq.exceptions.ConnectionError:
                raise rq.exceptions.ConnectionError(
                    'Failed to connect (%s). Try https://?' % url)
            if _response.status_code == 401:
                # authentication request
                user = self.config.api_username
                password = self.config.api_password
                _response = rq.get(url, params=kwargs_json,
                                   verify=False,
                                   auth=rq.auth.HTTPBasicAuth(
                                       user, password))
                _response.raise_for_status()

            try:
                # responses are always expected to be json encoded
                response = json.loads(_response.text)
            except Exception:
                response = _response.text
            else:
                return response


class Query(BaseClient):
    ''' Container for query related methods '''
    def __init__(self, config_dir=None, config_file=None):
        super(Query, self).__init__(config_dir, config_file)
        self._command = 'query'

    def aggregate(self, cube, pipeline):
        '''
        Proxy for pymongodb's .aggregate framework call
        on a given cube

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        pipeline : list
            The aggregation pipeline. $match, $project, etc.
        '''
        result = self._get('aggregate', cube=cube, pipeline=pipeline)
        try:
            return result['result']
        except Exception:
            raise RuntimeError(result)

    def count(self, cube, query):
        '''
        Run a `pql` based query on the given cube, but
        only return back the count (int)

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        query : str
            The query in pql
        #### COMING SOON - 0.1.4 ####
        date : str
            Date (date range) that should be queried:
                date -> 'd', '~d', 'd~', 'd~d'
                d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
        most_recent : boolean
            If true and there are multiple historical version of a single
            object matching the query then only the most recent one will
            be returned
        '''
        return self._get('count', cube=cube, query=query)

    def find(self, cube, query, fields=None, date=None, most_recent=False):
        '''
        Run a `pql` based query on the given cube.
        Optionally:
        * return back accompanying field meta data for
        * query again arbitrary datetimes in the past, if the
        * return back only the most recent date objects which
          match any given query, rather than all.

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        query : str
            The query in pql
        fields : str, or list of str, or str of comma-separated values
            Fields that should be returned
        date : str
            Date (date range) that should be queried:
                date -> 'd', '~d', 'd~', 'd~d'
                d -> '%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'
        most_recent : boolean
            If true and there are multiple historical version of a single
            object matching the query then only the most recent one will
            be returned
        '''
        result = self._get('find', cube=cube, query=query,
                           fields=fields, date=date, most_recent=most_recent)
        result = Result(result)
        result.date(date)
        return result

    def fetch(self, cube, fields, skip=0, limit=0, ids=[]):
        '''
        Fetch field values for (potentially) all objects
        of a given, with skip, limit, id "filter" arguments

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        fields : str, or list of str, or str of comma-separated values
            Fields that should be returned
        skip : int
            number of items (sorted ASC) to skip
        limit : int
            number of items total to return, given skip
        ids : list
            specific list of ids we should fetch
        '''
        result = self._get('fetch', cube=cube, fields=fields,
                           skip=skip, limit=limit, ids=ids)
        return Result(result)


class JobManage(BaseClient):
    ''' Container for Managing "Metrique Job" related methods '''
    def __init__(self, config_dir=None, config_file=None):
        super(JobManage, self).__init__(config_dir, config_file)
        self._command = 'job'

    def status(self, job_key):
        '''
        Fetch job status for a given metrique job
        identified by the job_key argument

        Paremeters
        ----------
        job_key : int
            id of the job
        '''
        action = 'status'
        return self._get(action, job_key)

    def __kill(self, job_key):
        raise NotImplementedError()


class AdminUsers(BaseClient):
    ''' Container for Managing "Metrique User Management" related methods '''
    def __init__(self, config_dir=None, config_file=None):
        super(AdminUsers, self).__init__(config_dir, config_file)
        self._command = 'admin/users'

    def add(self, cube, user, password, permissions):
        '''
        Add user permissions (or update if exists)
        Assigne that user a password (salt+hash)

        permissions are, as of v0.1::
        * r, rw, admin
        * inherent right (r <- rw <- admin)

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        user : str
            Name of the user you're managing
        password : str
            Password (plain text), if any of user
        permission : str
            Permission set, as of v0.1 (r, rw, admin)
            Permissions decorate tornado object methods (result?)
            and add 'auth'
        '''
        return self._get('add', cube=cube, user=user,
                         password=password, permissions=permissions)


class AdminETL(BaseClient):
    ''' Container for Managing "Metrique ETL" related methods '''
    def __init__(self, config_dir=None, config_file=None):
        super(AdminETL, self).__init__(config_dir, config_file)
        self._command = 'admin/etl'

    def index_warehouse(self, cube, fields="", force=0):
        '''
        Index particular fields of a given cube, assuming
        indexing is enabled for the cube.fields

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        fields : str, or list of str, or str of comma-separated values
            Fields that should be indexed
        '''
        if not fields:
            fields = client(self._config_dir, self._config_file).fields(cube)
        elif isinstance(fields, basestring):
            fields = [s.strip() for s in fields.split(',')]
        elif type(fields) is not list:
            raise TypeError("fields expected to be list or csv string")

        result = {}
        for field in fields:
            result[field] = self._get('index/warehouse', cube=cube,
                                      field=field, force=force)
        return result

    # FIXME: remove passing snapshot argument; snapshots should be
    # called explicitly
    def extract(self, cube, fields="", force=False, id_delta="",
                index=False, snapshot=False):
        '''
        Run the cube.extract_func command to pull data and dump
        to the warehouse

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        fields : str, or list of str, or str of comma-separated values
            Fields that should be indexed
        force : bool
            True runs without deltas. When False (default),
            and deltas supported, cube is expected to apply them
        id_delta : list of cube object ids or str of comma-separated ids
            Specifically list object ids to extract (if supported)
        index : bool
            Run warehouse ensure indexing after extraction
        snapshot : bool
            Run warehouse -> timeline snapshot after extraction

        '''
        result = self._get('extract', cube=cube, fields=fields,
                           force=force, id_delta=id_delta)
        if index:
            self.index_warehouse(cube, fields)
        if snapshot:
            self.snapshot(cube, index=index)
        return result

    def snapshot(self, cube, ids=None):
        '''
        Run a warehouse -> timeline (datetimemachine) snapshot
        of the data as it existed in the warehouse and dump
        copies of objects into the timeline, one new object
        per unique state in time.

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        ids : list of cube object ids or str of comma-separated ids
            Specificly run snapshot for this list of object ids
        '''
        return self._get('snapshot', cube=cube, ids=ids)

    def activity_import(self, cube, ids=None):
        '''
        Run the activity import for a given cube, if the
        cube supports it.

        Essentially, recreate object histories from
        a cubes 'activity history' table row data,
        and dump those pre-calcultated historical
        state object copies into the timeline.

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        ids : list of cube object ids or str of comma-separated ids
            Specificly run snapshot for this list of object ids
        '''
        return self._get('activityimport', cube=cube, ids=ids)

    def save_object(self, cube, obj, _id=None):
        '''
        Save a single object the given metrique.cube

        THIS IS OBSOLETE; WILL BE REPLACED WITH save_objects()
        note the plural.

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        obj : dict with 1+ field:value and _id defined
            These objects match the given cube(driver) definition
        _id : str
            The field which contains the _id value of each obj
        '''
        return self._get('saveobject', cube=cube, obj=obj, _id=_id)

    def get_template(self, cube, types=False):
        return self._get('gettemplate', cube=cube, types=types)

class Admin(BaseClient):
    ''' Container for all rw+ Metrique sub-containers '''
    def __init__(self, config_dir=None, config_file=None):
        super(Admin, self).__init__(config_dir, config_file)
        self.etl = AdminETL(config_dir, config_file)
        self.users = AdminUsers(config_dir, config_file)


class client(BaseClient):
    ''' Container for all metrique client api sub-containers '''
    def __init__(self, config_dir=None, config_file=None):
        super(client, self).__init__(config_dir, config_file)
        self.jobs = JobManage(config_dir, config_file)
        self.query = Query(config_dir, config_file)
        self.admin = Admin(config_dir, config_file)

    def ping(self):
        return self._get('ping')

    @property
    def cubes(self):
        ''' List all valid cubes for a given metrique instance '''
        return self._get('cubes')

    def fields(self, cube, details=False):
        ''' List all valid fields for a given cube

        Paremeters
        ----------
        cube : str
            Name of the cube you want to query
        details : bool
            return back dict of additional cube.field metadata
        '''
        return self._get('cubes', cube=cube, details=details)
