#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dateutil.parser import parse as dt_parse
from urllib2 import urlopen, HTTPError
import re
import simplejson as json

from metrique.client.cubes.basejson import BaseJSON
from metrique.client.utils import milli2sec, dt2ts

DEFAULT_CONFIG = {
    'uri': 'http://builds.apache.org',
    'port': '80',
    'api_path': 'api/json',
}

MAX_WORKERS = 25


def obj_hook(dct):
    _dct = {}
    for k, v in dct.items():
        _k = str(k).replace('.', '_')
        if k == 'timestamp':
            try:
                v = milli2sec(v)
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


class Build(BaseJSON):
    """
    Object used for communication with Jenkins Build (job detail) interface
    """
    name = 'jkns_build'

    def extract(self, uri=None, port=None, api_path=None,
                force=False, async=None, workers=None, **kwargs):
        if async is None:
            async = self.config.async
        if workers is None:
            workers = MAX_WORKERS

        if not uri:
            uri = DEFAULT_CONFIG['uri']
        self.uri = uri

        if not port:
            port = DEFAULT_CONFIG['port']
        self.port = port

        if not api_path:
            api_path = DEFAULT_CONFIG['api_path']
        self.api_path = api_path

        args = 'tree=jobs[name,builds[number]]'
        uri = '%s:%s/%s?%s' % (self.uri, self.port, self.api_path, args)
        self.logger.info("Getting Jenkins Job details (%s)" % uri)
        # get all known jobs
        content = json.loads(urlopen(uri).readlines()[0],
                             strict=False)
        jobs = content['jobs']
        self.logger.info("... %i jobs found." % len(jobs))

        if async:
            results = self._extract_async(jobs, force, workers)
        else:
            results = self._extract(jobs, force)
        return results

    def _extract(self, jobs, force):
        results = defaultdict(list)
        for k, job in enumerate(jobs, 1):
            job_name = job['name']
            self.logger.debug(
                '%s: %s of %s with %s builds' % (job_name, k,
                                                 len(jobs),
                                                 len(job['builds'])))
            nums = [b['number'] for b in job['builds']]
            builds = [self.get_build(job_name, n, force) for n in nums]
            try:
                results[job_name] += self.save_objects(builds)
            except Exception as e:
                self.logger.error(
                    '(%s) Failed to save: %s:%s' % (e, job_name, builds))
        return results

    def _extract_async(self, jobs, force, workers):
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
                        executor.submit(self.get_build, job_name, n, force))

                for future in as_completed(future_builds):
                    try:
                        build = future.result()
                        if build:
                            results[job_name] += self.save_objects([build])
                    except Exception as e:
                        self.logger.error(
                            '(%s) Failed to save: %s' % (e, job_name))
        return results

    def get_build(self, job_name, build_number, force=False):
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

        job_uri = '%s:%s%s/%s?%s' % (self.uri, self.port, _job_path,
                                     self.api_path, _args)
        self.logger.debug('Loading (%s)' % job_uri)
        try:
            _page = list(urlopen(job_uri))
            build_content = json.loads(_page[0],
                                       strict=False,
                                       object_hook=obj_hook)
        except HTTPError as e:
            self.logger.error('OOPS! (%s) %s' % (job_uri, e))
            build_content = {'load_error': e}

        _build['_oid'] = _oid
        _build['job_name'] = job_name
        _build['job_uri'] = job_uri
        _build['number'] = build_number
        _build.update(build_content)

        report_uri = '%s:%s%s/testReport/%s?%s' % (self.uri, self.port,
                                                   _job_path, self.api_path,
                                                   _args)
        self.logger.debug('Loading (%s)' % report_uri)
        try:
            report_content = json.loads(urlopen(report_uri).readlines()[0],
                                        strict=False,
                                        object_hook=obj_hook)
        except HTTPError as e:
            self.logger.error('OOPS! (%s) %s' % (report_uri, e))
            report_content = {'load_error': e}

        _build['report_uri'] = report_uri
        _build['report'] = report_content

        return _build


if __name__ == '__main__':
    from metrique.client.argparsers import cube_cli
    a = cube_cli.parse_args()
    kwargs = {}
    kwargs.update(a.cube_init_kwargs_config_file)
    if a.debug:
        kwargs.update({'debug': a.debug})
    obj = Build(config_file=a.cube_config_file, **kwargs)
    obj.extract(force=a.force)
