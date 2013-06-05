#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from metrique.server.drivers.basedrivermap import BaseDriverMap
from metrique.tools.jsonconfig import JSONConfig

DEFAULT_CONFIG = {
    "repos": {
        "metrique": "https://github.com/drpoovilleorg/metrique.git"
    }
}

PREFIX = 'git'


class GIT(BaseDriverMap):
    config = JSONConfig(PREFIX, default=DEFAULT_CONFIG)
    prefix = PREFIX
    _dict = {
        'commit': 'git.Commit',
    }

    def __init__(self, repos=None):
        repos = self.config['repos'] if repos is None else repos
        super(GIT, self).__init__(repos=repos)
