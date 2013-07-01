#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from dulwich.repo import Repo   # , GitCmdObjectDB
import logging
logger = logging.getLogger(__name__)
import os
import subprocess
import re

from metrique.client.cubes.basecube import BaseCube
from metrique.tools.decorators import memo

TMP_DIR = '/tmp'
DEFAULT_OBJECTS_PATH = '.git/objects'

hash_re = re.compile('[0-9a-f]{40}', re.I)
files_re = re.compile('^([0-9]+) file.*', re.I)
insertions_re = re.compile('.* ([0-9]+) insertion.*', re.I)
deletions_re = re.compile('.* ([0-9]+) deletion.*', re.I)


class BaseGitObject(BaseCube):
    """
    Driver to help extract data from GIT repos
    """
    def __init__(self, **kwargs):
        super(BaseGitObject, self).__init__(**kwargs)

    @memo
    def fetch_repo(self, uri, repo):
        repo_path = os.path.join(TMP_DIR, str(abs(hash(uri))))
        logger.info('GIT URI: %s' % uri)
        if not os.path.exists(repo_path):
            logger.info('Cloning git repo to %s' % repo_path)
            cmd = 'git clone %s %s' % (uri, repo_path)
            rc = subprocess.call(cmd.split(' '))
            if rc != 0:
                raise IOError("Failed to clone repo")
        else:
            os.chdir(repo_path)
            logger.info(' ... Fetching git repo (%s)' % repo_path)
            cmd = 'git fetch'
            rc = subprocess.call(cmd.split(' '))
            if rc != 0:
                raise RuntimeError('Failed to fetch repo')
            logger.info(' ... Fetch complete')

        return Repo(repo_path)

    def walk_commits(self, uri, last_dt=None, branch='master'):
        repo = uri.split('/')[-1]
        gitdb = self.fetch_repo(uri, repo)
        logger.debug("Iterating through object db (%s)" % repo)
        if last_dt:
            # and filter starting after the last object we've already
            return gitdb.get_walker(since=last_dt)
        else:
            # ... imported, if any; or get them all
            return gitdb.get_walker()
