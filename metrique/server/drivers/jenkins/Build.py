#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

#from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor
import logging
logger = logging.getLogger(__name__)
from urllib2 import urlopen, HTTPError
import simplejson as json

from metrique.server.drivers.drivermap import get_cube
from metrique.server.drivers.json import JSON
from metrique.server.etl import save_object

#from converters import id_when
MAX_WORKERS = 20


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
                'id_x': 'job_build',
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
                    # fixme: this is in milliseconds... datetime.fromtimestamp excepts
                    'type': float,
                    'help': '',
                },

                'url': {
                    'help': '',
                },

            }
        }

    def extract_func(self, **kwargs):
        with ProcessPoolExecutor(MAX_WORKERS) as executor:
            future = executor.submit(_extract_func, self.name, **kwargs)
        return future.result()

    def _save_build(self, job_name, build_number):
        c = self.get_collection()
        _id = '%s #%s' % (job_name, build_number)
        ## Check if we the job_build was already done building
        ## if it was, skip the import all together
        field_spec = {'_id': _id,
                      'fields.building.tokens': False}
        if c.find_one(field_spec, fields={'_id': 1}):
            logger.debug('BUILD (%s) (CACHED)' % _id)
            return 0

        _build = {}
        logger.debug('BUILD (%s)' % _id)
        _args = 'tree=%s' % ','.join(self.fields)
        _job_path = '/job/%s/%s' % (job_name, build_number)

        job_url = '%s:%s%s%s?%s' % (self._url, self._port, _job_path, self._api_path, _args)
        try:
            build_content = json.loads(urlopen(job_url).readlines()[0], strict=False)
        except HTTPError:
            return 0

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

        return save_object(c.name, _build, _id='job_build')


def _extract_func(cube, **kwargs):
    c = get_cube(cube)
    saved = 0
    # get all known jobs
    args = 'tree=jobs[name,builds[number]]'
    url = '%s:%s%s?%s' % (c._url, c._port, c._api_path, args)
    logger.debug("Getting Jenkins Job details (%s)" % url)
    content = json.loads(urlopen(url).readlines()[0], strict=False)
    jobs = content['jobs']

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        for k, job in enumerate(jobs, 1):
            job_name = job['name']
            logger.debug('JOB (%s): %s of %s with %s builds' % (job_name, k,
                                                                len(jobs),
                                                                len(job['builds'])))
            nums = [b['number'] for b in job['builds']]
            future_build = []
            for n in nums:
                future_build.append(executor.submit(c._save_build, job_name, n))

            for future in as_completed(future_build):
                    try:
                        saved += future.result()
                    except Exception as exc:
                        print('Exception: %s' % exc)
    return str(saved)
