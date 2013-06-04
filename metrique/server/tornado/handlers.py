#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from functools import wraps
import re
import simplejson as json
import tornado

from metrique.server.drivers.drivermap import get_cube, get_cubes

from metrique.tools.constants import HAS_SRE_PATTERN
from metrique.tools.json import Encoder


def async(f):
    @tornado.web.asynchronous
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.proxy.async:
            def future_end(future):
                try:
                    _result = future.result()
                    # Result is always expected to be json encoded!
                    result = json.dumps(_result, cls=Encoder,
                                        ensure_ascii=False)
                except Exception as e:
                    result = json.dumps(str(e))
                    raise
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


class MetriqueInitialized(tornado.web.RequestHandler):
    def initialize(self, proxy):
        self.proxy = proxy

    @staticmethod
    def _regex_parser(item):
        # FIXME: this only works for 1 level deep nested conditions
        if type(item) is dict and item.get('$match'):
            for k, v in item['$match'].iteritems():
                if isinstance(v, basestring) and HAS_SRE_PATTERN.search(v):
                    item['$match'][k] = re.compile(HAS_SRE_PATTERN.sub(r'\1',
                                                                       v))
        return item

    def get_argument(self, key, default=None):
        # arguments are expected to be json encoded!
        _arg = super(MetriqueInitialized, self).get_argument(key, default)

        if _arg is None:
            return _arg

        try:
            arg = json.loads(_arg, object_hook=self._regex_parser)
        except Exception as e:
            raise ValueError("Invalid JSON content (%s): %s" % (type(_arg), e))
        return arg


class PingHandler(MetriqueInitialized):
    @async
    def get(self):
        return self.proxy.ping()


class MongoUserAddHandler(MetriqueInitialized):
    # FIXME: This should be POST, not GET
    @async
    def get(self, user):
        name = self.get_argument('name')
        password = self.get_argument('password')
        admin = self.get_argument('admin', 0)
        return self.proxy.mongo.add_user(name, password, admin)


class JobStatusHandler(MetriqueInitialized):
    @async
    def get(self, job_key):
        job_json = self.proxy.job.status(job_key)
        job = json.loads(job_json)
        return job['result']


class QueryAggregateHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        pipeline = self.get_argument('pipeline', '[]')
        return self.proxy.query.aggregate(cube, pipeline)


class QueryFetchHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        fields = self.get_argument('fields')
        skip = self.get_argument('skip', 0)
        limit = self.get_argument('limit', 0)
        ids = self.get_argument('ids', [])
        return self.proxy.query.fetch(cube=cube, fields=fields,
                                      skip=skip, limit=limit, ids=ids)


class QueryCountHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        query = self.get_argument('query')
        return self.proxy.query.count(cube, query)


class QueryFindHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        query = self.get_argument('query')
        fields = self.get_argument('fields', '')
        date = self.get_argument('date')
        most_recent = self.get_argument('most_recent', True)
        return self.proxy.query.find(cube, query, fields, date, most_recent)


class LogTailHandler(MetriqueInitialized):
    @async
    def get(self):
        spec = self.get_argument('spec', '{}')
        limit = self.get_argument('limit', 20)
        format_ = self.get_argument('format', '')
        return self.proxy.admin.log.tail(spec, limit, format_=format_)


class ETLIndexWarehouseHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        field = self.get_argument('field', '')
        force = self.get_argument('force', 0)
        return self.proxy.admin.etl.index_warehouse(cube, field,
                                                    force)


class ETLIndexTimelineHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        return self.proxy.admin.etl.index_timeline(cube)


class ETLExtractHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        fields = self.get_argument('fields', "")
        force = self.get_argument('force', False)
        id_delta = self.get_argument('id_delta', "")
        return self.proxy.admin.etl.extract(cube=cube, fields=fields,
                                            force=force, id_delta=id_delta)


class ETLSnapshotHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        ids = self.get_argument('ids')
        return self.proxy.admin.etl.snapshot(cube=cube, ids=ids)


class ETLActivityImportHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        ids = self.get_argument('ids')
        return self.proxy.admin.etl.activity_import(cube=cube, ids=ids)


class CubesHandler(MetriqueInitialized):
    @async
    def get(self):
        cube = self.get_argument('cube')
        details = self.get_argument('details', None)
        if cube is None:
            # return a list of cubes
            return get_cubes()
        else:
            # return a list of fields in a cube
            if details:
                result = get_cube(cube).fields
            else:
                result = sorted(get_cube(cube).fields.keys())
            return result
