#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

from metrique.server.drivers.basegitobject import BaseGitObject
from metrique.server.drivers.drivermap import get_cube
from metrique.server.drivers.git.converters import ts
from metrique.server.etl import save_object

MAX_WORKERS = 20


class Commit(BaseGitObject):
    """
    Object used for communication with Git Commit interface
    """
    def __init__(self, repos, **kwargs):
        super(Commit, self).__init__(**kwargs)
        self.repos = repos

        self.cube = {
            'defaults': {
                'timeline': False,
                'id_x': 'hexsha',
            },

            'fielddefs': {
                'author': {
                    'help': '',
                },

                'author_ts': {
                    'type': datetime,
                    'convert': ts,
                    'help': '',
                },

                'committer': {
                    'help': '',
                },

                'committer_ts': {
                    'type': datetime,
                    'convert': ts,
                    'help': '',
                },

                'hexsha': {
                    'help': '',
                },

                'message': {
                    'help': '',
                },
                'parents': {
                    'help': '',
                },

                'resolves': {
                    'help': '',
                },

                'related': {
                    'help': '',
                },

                'signed_off_by': {
                    'help': '',
                },

                'tree': {
                    'help': '',
                },

                'uri': {
                    'help': '',
                },

            }
        }

    def extract_func(self, **kwargs):
        with ProcessPoolExecutor(MAX_WORKERS) as executor:
            future = executor.submit(_extract_func, self.name, **kwargs)
        return future.result()
        #return _extract_func(self.name, **kwargs)


def _extract_func(cube, **kwargs):
    c = get_cube(cube)
    result = defaultdict(int)
    for repo, uri in sorted(c.repos.items()):
        logger.debug("Loading GIT: %s" % repo)
        c.load_commits(uri, repo)
        for i in c.reader:
            result[uri] += save_object(cube, i, 'hexsha')
    return result
