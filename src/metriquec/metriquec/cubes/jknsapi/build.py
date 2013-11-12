#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Basic Jenkins.build cube for extracting BUILD data from Jenkins
'''

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dateutil.parser import parse as dt_parse
from functools import partial
import requests
import re
import simplejson as json

from metrique.core_api import HTTPClient
from metriqueu.utils import dt2ts

DEFAULT_CONFIG = {
    'uri': ['http://builds.apache.org'],
    'port': '80',
    'api_path': 'api/json',
}

MAX_WORKERS = 25

SSL_VERIFY = True

rget = partial(requests.get, verify=SSL_VERIFY)

# FIXME: add a version check; this supports jkns 1.529; but doesn't 1.480
# FIXME: subclass jsondata_objs cube?


def obj_hook(dct):
    _dct = {}
    for k, v in dct.items():
        _k = str(k).replace('.', '_')
        if k == 'timestamp':
            try:
                # convert milliseconds to seconds
                v = v / 1000. if v else v
            except:
                # some cases timestamp is a datetime str
                v = dt2ts(v)
        if k == 'date':
            v = dt2ts(v)
        _dct[_k] = v
    return _dct


def id_when(id):
    if isinstance(id, datetime):
        return id
    else:
        when = re.sub(
            '[_ T](\d\d)[:-](\d\d)[:-](\d\d)$',
            'T\g<1>:\g<2>:\g<3>', id)
        return dt_parse(when)


class Build(HTTPClient):
    """
    Object used for communication with Jenkins Build (job detail) interface
    """
    name = 'jknsapi_build'

    def extract(self, uri=None, port=None, api_path=None,
                force=False, async=None, workers=None, **kwargs):
        if async is None:
            async = self.config.async
        if workers is None:
            workers = MAX_WORKERS

        if not uri:
            uri = DEFAULT_CONFIG['uri']
        if not isinstance(uri, list):
            uri = [uri]

        if not port:
            port = DEFAULT_CONFIG['port']
        self.port = port

        if not api_path:
            api_path = DEFAULT_CONFIG['api_path']
        self.api_path = api_path

        results = {}
        for _uri in uri:
            args = 'tree=jobs[name,builds[number]]'
            _uri_jobs = '%s:%s/%s?%s' % (_uri, self.port, self.api_path, args)
            self.logger.info("Getting Jenkins Job details (%s)" % _uri_jobs)
            # get all known jobs
            content = rget(_uri_jobs).content
            content = json.loads(content, strict=False)
            jobs = content['jobs']
            self.logger.info("... %i jobs found." % len(jobs))

            if async:
                results[_uri] = self._extract_async(_uri, jobs, force, workers)
            else:
                results[_uri] = self._extract(_uri, jobs, force)
        return results

    def _extract(self, uri, jobs, force):
        results = defaultdict(list)
        for k, job in enumerate(jobs, 1):
            job_name = job['name']
            self.logger.debug(
                '%s: %s of %s with %s builds' % (job_name, k,
                                                 len(jobs),
                                                 len(job['builds'])))
            nums = [b['number'] for b in job['builds']]
            builds = [self.get_build(uri, job_name, n, force) for n in nums]
            try:
                results[job_name] += self.cube_save(builds)
            except Exception as e:
                self.logger.error(
                    '(%s) Failed to save: %s:%s' % (e, job_name, builds))
        return results

    def _extract_async(self, uri, jobs, force, workers):
        with ThreadPoolExecutor(workers) as executor:
            results = defaultdict(list)
            for k, job in enumerate(jobs, 1):
                job_name = job['name']
                self.logger.debug(
                    '%s: %s of %s with %s builds' % (job_name, k,
                                                     len(jobs),
                                                     len(job['builds'])))
                future_builds = []
                nums = [b['number'] for b in job['builds']]
                for n in nums:
                    future_builds.append(
                        executor.submit(self.get_build, uri, job_name,
                                        n, force))

                for future in as_completed(future_builds):
                    try:
                        build = future.result()
                        if build:
                            results[job_name] += self.cube_save([build])
                    except Exception as e:
                        self.logger.error(
                            '(%s) Failed to save: %s' % (e, job_name))
        return results

    def get_build(self, uri, job_name, build_number, force=False):
        _oid = '%s #%s' % (job_name, build_number)

        ## Check if we the job_build was already done building
        ## if it was, skip the import all together
        query = '_oid == "%s" and building != True' % _oid
        if not force and self.count(query) > 0:
            self.logger.debug('(CACHED) BUILD: %s' % _oid)
            return None

        _build = {}
        #_args = 'tree=%s' % ','.join(self.fields)
        _args = 'depth=1'
        _job_path = '/job/%s/%s' % (job_name, build_number)

        job_uri = '%s:%s%s/%s?%s' % (uri, self.port, _job_path,
                                     self.api_path, _args)
        self.logger.debug('Loading (%s)' % job_uri)
        try:
            _page = rget(job_uri).content
            build_content = json.loads(_page,
                                       strict=False,
                                       object_hook=obj_hook)
        except Exception as e:
            self.logger.error('OOPS! (%s) %s' % (job_uri, e))
            build_content = {'load_error': e}

        _build['_oid'] = _oid
        _build['job_name'] = job_name
        _build['job_uri'] = job_uri
        _build['number'] = build_number
        _build.update(build_content)

        report_uri = '%s:%s%s/testReport/%s?%s' % (uri, self.port,
                                                   _job_path, self.api_path,
                                                   _args)
        self.logger.debug('Loading (%s)' % report_uri)
        try:
            _page = rget(report_uri).content
            report_content = json.loads(_page,
                                        strict=False,
                                        object_hook=obj_hook)
        except Exception as e:
            self.logger.error('OOPS! (%s) %s' % (report_uri, e))
            report_content = {'load_error': e}

        _build['report_uri'] = report_uri
        _build['report'] = report_content

        return _build


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Build)
