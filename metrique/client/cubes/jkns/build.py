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

from metrique.client.cubes.basejson import BaseJSON

DEFAULT_CONFIG = {
    'url': 'http://builds.apache.org',
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

    defaults = {
        'index': True,
    }

    fields = {
        'building': {
            'type': float,
        },
        'build': {},
        'builtOn': {},
        'description': {},
        'duration': {
            'type': float,
        },
        'executor': {},
        'estimatedDuration': {
            'type': float,
        },
        'failCount': {
            'type': float,
        },
        'passCount': {
            'type': float,
        },
        'skipCount': {
            'type': float,
        },
        'id': {},
        'job_name': {},
        'job_build': {},
        'number': {
            'type': float,
        },
        'result': {},
        'timestamp': {
            'convert': lambda x: datetime.fromtimestamp(x / 1000),
            'type': float,
        },
        'url': {},
    }

    def __init__(self, url=None, port=None, api_path=None, **kwargs):
        super(Build, self).__init__(**kwargs)
        if not url:
            url = DEFAULT_CONFIG['url']
        self.url = url

        if not port:
            port = DEFAULT_CONFIG['port']
        self.port = port

        if not api_path:
            api_path = DEFAULT_CONFIG['api_path']
        self.api_path = api_path

    def extract(self, **kwargs):
        # get all known jobs
        args = 'tree=jobs[name,builds[number]]'
        url = '%s:%s%s?%s' % (self.url, self.port, self.api_path, args)
        logger.debug("Getting Jenkins Job details (%s)" % url)
        content = json.loads(urlopen(url).readlines()[0], strict=False)
        jobs = content['jobs']

        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            results = defaultdict(int)
            for k, job in enumerate(jobs, 1):
                job_name = job['name']
                logger.debug(
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
        # FIXME: delta implemntation needs to be updated
        #c = self.get_collection()
        _id = '%s #%s' % (job_name, build_number)

        ## Check if we the job_build was already done building
        ## if it was, skip the import all together
        #field_spec = {'_id': _id,
        #              'building': {'$ne': True}}
        #if c.find(field_spec).count():
        #    #logger.debug('BUILD: %s (CACHED)' % _id)
        #    return {}

        _build = {}
        logger.debug('BUILD: %s' % _id)
        _args = 'tree=%s' % ','.join(self.fields)
        _args += ',actions[*]'  # for pulling build name info
        _job_path = '/job/%s/%s' % (job_name, build_number)

        job_url = '%s:%s%s%s?%s' % (self.url, self.port, _job_path,
                                    self.api_path, _args)
        try:
            build_content = json.loads(urlopen(job_url).readlines()[0],
                                       strict=False)
        except HTTPError:
            return {}

        _build['_id'] = _id
        _build['job_build'] = _id
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
        _build['timestamp'] = build_content.get('timestamp')
        _build['url'] = build_content.get('url')

        report_url = '%s:%s%s/testReport/%s?%s' % (self.url, self.port,
                                                   _job_path, self.api_path,
                                                   _args)
        try:
            report_content = json.loads(urlopen(report_url).readlines()[0],
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
    obj = Build(**vars(a))
    obj.extract()
