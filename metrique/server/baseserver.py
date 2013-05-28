#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger()
logging.basicConfig()
logger.propagate = False

from bson.code import Code
from bson.objectid import ObjectId
from dateutil.parser import parse as dt_parse
from operator import itemgetter
import simplejson as json

from metrique.server.config import metrique, mongodb
from metrique.server.defaults import METRIQUE_CONF, MONGODB_CONF
from metrique.server.job import get_job
from metrique.server import query as query_live
from metrique.server import etl


def job_save(name):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            job = get_job(name)
            logger.debug('Running: %s' % name)
            result = func(self, *args, **kwargs)
            job.complete()
            return result
        return wrapper
    return decorator


class BaseServer(object):
    def __init__(self, config_dir=None,
                 metrique_config_file=None, mongodb_config_file=None):
        if not metrique_config_file:
            metrique_config_file = METRIQUE_CONF
        if not mongodb_config_file:
            mongodb_config_file = MONGODB_CONF

        self._config_dir = config_dir

        self._metrique_config_file = metrique_config_file
        self.metrique_config = metrique(metrique_config_file, config_dir)

        self._mongodb_config_file = metrique_config_file
        self.mongodb_config = mongodb(mongodb_config_file, config_dir)


class Admin(BaseServer):
    def __init__(self):
        self.etl = ETL()
        self.log = Log()
        self.mongo = Mongo()


class Mongo(BaseServer):
    @job_save('add_user')
    def add_user(self, name, password, admin=False):
        return self.warehouse_admin.add_user(name, password, admin)


class Log(BaseServer):
    @job_save('log_get_formats')
    def get_formats(self):
        map = Code(
            "function() { for (var key in this) { emit(key, null); } }")
        reduce = Code("function(key, stuff) { return null; }")
        impr = self.mongodb_config.c_logs.inline_map_reduce
        return [doc['_id'] for doc in impr(map, reduce)]

    @job_save('log_tail')
    def tail(self, spec=None, limit=None, format_=None):
        if not spec:
            spec = {}
        else:
            spec = json.loads(spec)

        if not format_:
            format_ = '%(processName)s:%(message)s'

        # spec 'when' key needs to be converted from string to datetime
        if 'when' in spec:
            spec['when']['$gt'] = dt_parse(spec['when']['$gt'])

        if not limit:
            limit = 20
        else:
            limit = int(limit)
            if limit < 0:
                raise ValueError("limit must be an integer value > 0")

        docs = self.mongodb_config.c_logs.find(spec, limit=limit, sort=[('when', -1)])

        _result = sorted([doc for doc in docs], key=itemgetter('when'))

        try:
            # get the last log.when so client knows from where to
            # start next...
            last_when = _result[-1]['when']
            meta = last_when
            result = '\n'.join([format_ % doc for doc in _result])
        except KeyError:
            raise KeyError("Invalid log format key (%s)" % format_)
        except ValueError:
            raise ValueError("Invalid log format string (%s)" % format_)
        except IndexError:
            result = None
            meta = None

        return result, meta


class JobManage(BaseServer):
    @job_save('job_status')
    def status(self, job_key):
        _id = ObjectId(job_key)
        spec = {'_id': _id}
        return self.mongodb_config.c_job_activity.find_one(spec)


class ETL(BaseServer):
    @job_save('etl_index_timeline')
    def index_timeline(self, cube, force=None):
        return etl.index_timeline(cube, force)

    @job_save('etl_index_warehouse')
    def index_warehouse(self, cube, field=None, force=None):
        return etl.index_warehouse(cube, field, force)

    @job_save('etl_extract')
    def extract(self, cube, fields="", force=0, id_delta=None, index=0):
        return etl.extract(cube, fields=fields,
                           force=force,
                           id_delta=id_delta,
                           index=index)

    @job_save('etl_snapshot')
    def snapshot(self, cube):
        return etl.snapshot(cube)

    @job_save('etl_import_history')
    def import_history(self, cube):
        return etl.activity_history_import(cube)


class Query(BaseServer):
    @job_save('count')
    def count(self, cube, query):
        return query_live.count(cube, query)

    @job_save('find')
    def find(self, cube, query, fields=None):
        return query_live.find(cube, query, fields)

    @job_save('aggregate')
    def aggregate(self, cube, pipeline):
        return query_live.aggregate(cube, pipeline)

    @job_save('fetch')
    def fetch(self, cube, fields, skip=0, limit=0, ids=[]):
        return query_live.fetch(cube=cube, fields=fields,
                                skip=skip, limit=limit, ids=ids)
