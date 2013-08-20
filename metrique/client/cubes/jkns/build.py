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

DEFAULT_CONFIG = {
    'uri': 'http://builds.apache.org',
    'port': '80',
    'api_path': '/api/json',
}

MAX_WORKERS = 20


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

    def extract(self, uri=None, port=None, api_path=None, **kwargs):
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
        uri = '%s:%s%s?%s' % (self.uri, self.port, self.api_path, args)
        self.logger.debug("Getting Jenkins Job details (%s)" % uri)
        # get all known jobs
        content = json.loads(urlopen(uri).readlines()[0], strict=False)
        jobs = content['jobs']

        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            results = defaultdict(int)
            for k, job in enumerate(jobs, 1):
                job_name = job['name']
                self.logger.debug(
                    'JOB (%s): %s of %s with %s builds' % (job_name, k,
                                                           len(jobs),
                                                           len(job['builds'])))
                future_builds = []
                nums = [b['number'] for b in job['builds']]
                for n in nums:
                    future_builds.append(
                        executor.submit(self.get_build, job_name, n))

                builds = []
                for future in as_completed(future_builds):
                    obj = future.result()
                    if obj:
                        builds.append(obj)

                if builds:
                    results[job_name] += self.save_objects(builds)
        return results

    def get_build(self, job_name, build_number):
        _oid = '%s #%s' % (job_name, build_number)

        ## Check if we the job_build was already done building
        ## if it was, skip the import all together
        query = '_oid == "%s" and building != True'
        if self.count(query):
            #self.logger.debug('BUILD: %s (CACHED)' % _oid)
            return {}

        _build = {}
        self.logger.debug('BUILD: %s' % _oid)
        _args = 'tree=%s' % ','.join(self.fields)
        _args += ',actions[*]'  # for pulling build name info
        _job_path = '/job/%s/%s' % (job_name, build_number)

        job_uri = '%s:%s%s%s?%s' % (self.uri, self.port, _job_path,
                                    self.api_path, _args)
        try:
            build_content = json.loads(urlopen(job_uri).readlines()[0],
                                       strict=False)
        except HTTPError:
            return {}

        _build['_oid'] = _oid
        _build['number'] = build_number
        _build['job_name'] = job_name
        _build['id'] = build_content.get('id')

        actions = build_content.get('actions')
        if not actions:
            actions = []

        try:
            # holds 'build' info we're looking for
            _build['build'] = actions.pop(-2).get('text').strip()
        except (KeyError, AttributeError, IndexError, TypeError):
            _build['build'] = None

        _build['building'] = build_content.get('building')
        _build['builtOn'] = build_content.get('builtOn')
        _build['description'] = build_content.get('description')
        _build['result'] = build_content.get('result')
        _build['executor'] = build_content.get('executor')
        _build['duration'] = build_content.get('duration')
        _build['estimatedDuration'] = build_content.get('estimatedDuration')
        ts = build_content.get('timestamp')  # from milliseconds ...
        _build['timestamp'] = datetime.fromtimestamp(ts / 1000)  # to seconds
        _build['uri'] = build_content.get('uri')

        report_url = '%s:%s%s/testReport/%s?%s' % (self.uri, self.port,
                                                   _job_path, self.api_path,
                                                   _args)
        try:
            report_content = json.loads(urlopen(report_uri).readlines()[0],
                                        strict=False)
        except HTTPError:
            report_content = {}

        _build['failCount'] = report_content.get('failCount')
        _build['passCount'] = report_content.get('passCount')
        _build['skipCount'] = report_content.get('skipCount')

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
