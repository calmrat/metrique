#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from git import Repo   # , GitCmdObjectDB
import logging
logger = logging.getLogger(__name__)
import os
import subprocess

from metrique.client.cubes.basecube import BaseCube
from metrique.tools.decorators import memo

TMP_DIR = '/tmp'
DEFAULT_OBJECTS_PATH = '.git/objects'


class BaseGitObject(BaseCube):
    """
    Driver to help extract data from GIT repos
    """
    def __init__(self):
        super(BaseGitObject, self).__init__()

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
        obj_path = os.path.join(repo_path, DEFAULT_OBJECTS_PATH)
        #return Repo(obj_path, odbt=GitCmdObjectDB)
        return Repo(obj_path)

    def walk_commits(self, uri, last_dt=None, branch='master'):
        repo = uri.split('/')[-1]
        gitdb = self.fetch_repo(uri, repo)
        logger.debug("Iterating through object db (%s)" % repo)
        # by default, we're sorted DESC; we want ASC
        if last_dt:
            # and filter starting after the last object we've already
            return gitdb.iter_commits(branch, reverse=True, after=last_dt)
        else:
            # ... imported, if any; or get them all
            return gitdb.iter_commits(branch, reverse=True)
