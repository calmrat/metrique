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

    def __init__(self):
        super(Jenkins, self).__init__(**DEFAULT_CONFIG)
