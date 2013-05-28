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
        kwargs_json = dict([(k, json.dumps(v, cls=Encoder, ensure_ascii=False)) for k, v in kwargs.items()])

        url = os.path.join(self.config.metrique_api_url, self._command, *args)
        if self.background:
            # FIXME: save the threads and use (eg) multiprocessing.pool.ThreadPool... result.get()
            # to get back the results later if the client wants...
            t = Thread(target=rq.get, kwargs={'url': url, 'params': kwargs_json})
            t.daemon = True
            t.start()
            return
        else:
            try:
                _response = rq.get(url, params=kwargs_json)
            except KeyboardInterrupt:
                return

            try:
                # responses are always expected to be json encoded
                response = json.loads(_response.text)
            except Exception:
                response = _response.text
            else:
                return response


class Query(BaseClient):
    def __init__(self, config_dir=None, config_file=None):
        super(Query, self).__init__(config_dir, config_file)
        self._command = 'query'

    def aggregate(self, cube, pipeline):
        return self._get('aggregate', cube=cube, pipeline=pipeline)

    def count(self, cube, query):
        '''
        '''
        return self._get('count', cube=cube, query=query)

    def find(self, cube, query, fields=''):
        '''
        '''
        result = self._get('find', cube=cube, query=query,
                           fields=fields)
        return Result(result)

    def fetch(self, cube, fields, skip=0, limit=0, ids=[]):
        result = self._get('fetch', cube=cube, fields=fields,
                           skip=skip, limit=limit, ids=ids)
        return Result(result)


class JobManage(BaseClient):
    def __init__(self, config_dir=None, config_file=None):
        super(JobManage, self).__init__(config_dir, config_file)
        self._command = 'job'

    def status(self, job_key):
        action = 'status'
        return self._get(action, job_key)

    def kill(self, job_key):
        raise NotImplementedError()


class AdminLog(BaseClient):
    def __init__(self, config_dir=None, config_file=None):
        super(AdminLog, self).__init__(config_dir, config_file)
        self._command = 'admin/log'

    def formats(self):
        raise NotImplementedError
        action = 'formats'
        return self._get(action)

    def tail(self, n=10, spec='', follow=0, module_name='',
             format_='%(when)s:%(processName)s:%(message)s'):
        action = 'tail'
        if spec:
            spec = json.loads(spec)
        else:
            spec = {}

        if module_name:
            spec_name = spec.get('name', '')
            if spec_name:
                spec_name = [spec_name, module_name]
            else:
                spec_name = [module_name]
            spec.update({'name': {'$regex': '|'.join(spec_name)}})

        return self._get(action, spec=spec, limit=n, format=format_)


class AdminETL(BaseClient):
    def __init__(self, config_dir=None, config_file=None):
        super(AdminETL, self).__init__(config_dir, config_file)
        self._command = 'admin/etl'

    def index_warehouse(self, cube, fields="", force=0):
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

    def extract(self, cube, fields="", force=0, id_delta="", index=0):
        return self._get('extract', cube=cube, fields=fields,
                         force=force, id_delta=id_delta, index=index)

    def snapshot(self, cube):
        return self._get('snapshot', cube=cube)

    def activity_import(self, cube):
        return self._get('activityimport', cube=cube)


class AdminAuth(BaseClient):
    def __init__(self, config_dir=None, config_file=None):
        super(AdminAuth, self).__init__(config_dir, config_file)
        self._command = 'admin/auth'

    def list(self, fingerprint=""):
        return self._get('list', fingerprint=fingerprint)

    def auth(self, fingerprint="", role="", cube="", field=""):
        return self._get('auth', fingerprint=fingerprint, role=role,
                         cube=cube, field=field)

    def add(self, fingerprint="", role="", cube="", field="", mask=0):
        return self._get('add', fingerprint=fingerprint, role=role,
                         cube=cube, field=field, mask=mask)

    def drop(self, fingerprint="", role="", cube="", field=""):
        return self._get('drop', fingerprint=fingerprint, role=role,
                         cube=cube, field=field)

    def mask(self, fingerprint="", role="", mask=0):
        return self._get('mask', fingerprint=fingerprint, role=role,
                         mask=mask)


class Admin(BaseClient):
    def __init__(self, config_dir=None, config_file=None):
        super(Admin, self).__init__(config_dir, config_file)
        self.auth = AdminAuth(config_dir, config_file)
        self.etl = AdminETL(config_dir, config_file)
        self.log = AdminLog(config_dir, config_file)


class client(BaseClient):
    def __init__(self, config_dir=None, config_file=None):
        super(client, self).__init__(config_dir, config_file)
        self.jobs = JobManage(config_dir, config_file)
        self.query = Query(config_dir, config_file)
        self.admin = Admin(config_dir, config_file)

    def ping(self):
        return self._get('ping')

    @property
    def cubes(self):
        return self._get('cubes')

    def fields(self, cube, details=False):
        return self._get('cubes', cube=cube, details=details)
