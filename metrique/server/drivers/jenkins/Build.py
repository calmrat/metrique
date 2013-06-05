#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dateutil.parser import parse as dt_parse
import logging
logger = logging.getLogger(__name__)
from urllib2 import urlopen, HTTPError
import re
import simplejson as json

from metrique.server.drivers.json import JSON
from metrique.server.etl import save_objects

MAX_WORKERS = 20


def id_when(id):
    if isinstance(id, datetime):
        return id
    else:
        when = re.sub('[_ T](\d\d)[:-](\d\d)[:-](\d\d)$', 'T\g<1>:\g<2>:\g<3>', id)
        return dt_parse(when)


class Build(JSON):
    """
    Object used for communication with Jenkins Build (job detail) interface
    """
    def __init__(self, url, port, api_path, **kwargs):
        super(Build, self).__init__(**kwargs)
        self._url = url
        self._port = port
        self._api_path = api_path

        self.cube = {
            'defaults': {
                'index': True,
            },

            'fielddefs': {
                'building': {
                    'type': float,
                    'help': '',
                },

                'builtOn': {
                    'help': '',
                },

                'description': {
                    'help': '',
                },

                'duration': {
                    'type': float,
                    'help': '',
                },

                'executor': {
                    'help': '',
                },

                'estimatedDuration': {
                    'type': float,
                    'help': '',
                },

                'failCount': {
                    'type': float,
                    'help': '',
                },

                'passCount': {
                    'type': float,
                    'help': '',
                },

                'skipCount': {
                    'type': float,
                    'help': '',
                },

                'id': {
                    'help': '',
                },

                'job_name': {
                    'help': '',
                },

                'job_build': {
                    'help': '',
                },

                'number': {
                    'type': float,
                    'help': '',
                },

                'result': {
                    'help': '',
                },

                'timestamp': {
                    'convert': lambda x: datetime.fromtimestamp(x / 1000),
                    'type': float,
                    'help': '',
                },

                'url': {
                    'help': '',
                },

            }
        }

    def extract_func(self, **kwargs):
        # get all known jobs
        args = 'tree=jobs[name,builds[number]]'
        url = '%s:%s%s?%s' % (self._url, self._port, self._api_path, args)
        logger.debug("Getting Jenkins Job details (%s)" % url)
        content = json.loads(urlopen(url).readlines()[0], strict=False)
        jobs = content['jobs']

        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            results = defaultdict(int)
            for k, job in enumerate(jobs, 1):
                job_name = job['name']
                logger.debug('JOB (%s): %s of %s with %s builds' % (job_name, k,
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
                    results[job_name] += save_objects(self.name, builds)
        return results

    def get_build(self, job_name, build_number):
        c = self.get_collection()
        _id = '%s #%s' % (job_name, build_number)

        ## Check if we the job_build was already done building
        ## if it was, skip the import all together
        field_spec = {'_id': _id,
                      'building': {'$ne': True}}
        if c.find(field_spec).count():
            #logger.debug('BUILD: %s (CACHED)' % _id)
            return {}

        _build = {}
        logger.debug('BUILD: %s' % _id)
        _args = 'tree=%s' % ','.join(self.fields)
        _job_path = '/job/%s/%s' % (job_name, build_number)

        job_url = '%s:%s%s%s?%s' % (self._url, self._port, _job_path, self._api_path, _args)
        try:
            build_content = json.loads(urlopen(job_url).readlines()[0], strict=False)
        except HTTPError:
            return {}

        _build['_id'] = _id
        _build['job_build'] = _id
        _build['number'] = build_number
        _build['job_name'] = job_name
        _build['id'] = build_content.get('id')
        _build['building'] = build_content.get('building')
        _build['builtOn'] = build_content.get('builtOn')
        _build['description'] = build_content.get('description')
        _build['result'] = build_content.get('result')
        _build['executor'] = build_content.get('executor')
        _build['duration'] = build_content.get('duration')
        _build['estimatedDuration'] = build_content.get('estimatedDuration')
        _build['timestamp'] = build_content.get('timestamp')
        _build['url'] = build_content.get('url')

        report_url = '%s:%s%s/testReport/%s?%s' % (self._url, self._port,
                                                   _job_path, self._api_path, _args)
        try:
            report_content = json.loads(urlopen(report_url).readlines()[0], strict=False)
        except HTTPError:
            report_content = {}

        _build['failCount'] = report_content.get('failCount')
        _build['passCount'] = report_content.get('passCount')
        _build['skipCount'] = report_content.get('skipCount')

        return _build
