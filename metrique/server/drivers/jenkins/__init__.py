#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from metrique.server.drivers.basedrivermap import BaseDriverMap
from metrique.tools.jsonconfig import JSONConfig

DEFAULT_CONFIG = {
    'url': 'http://builds.apache.org',
    'port': '80',
    'api_path': '/api/json',
}

PREFIX = 'jkns'


class Jenkins(BaseDriverMap):
    config = JSONConfig(PREFIX, default=DEFAULT_CONFIG)
    prefix = PREFIX
    _dict = {
        'build': 'jenkins.Build',
    }

    def __init__(self, url=None, port=None, api_path=None):
        url = self.config['url'] if url is None else url
        port = self.config['port'] if port is None else port
        api_path = self.config['api_path'] if api_path is None else api_path
        super(Jenkins, self).__init__(url=url, port=port, api_path=api_path)
