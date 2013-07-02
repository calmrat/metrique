#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from dulwich.repo import Repo   # , GitCmdObjectDB
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
    def __init__(self, **kwargs):
        super(BaseGitObject, self).__init__(**kwargs)

    @memo
    def get_repo(self, uri, fetch=True):
        repo_path = os.path.join(TMP_DIR, str(abs(hash(uri))))
        self.repo_path = repo_path
        logger.info('GIT URI: %s' % uri)
        if fetch:
            if not os.path.exists(repo_path):
                logger.info('Cloning git repo to %s' % repo_path)
                cmd = 'git clone %s %s' % (uri, repo_path)
                rc = subprocess.call(cmd.split(' '))
                if rc != 0:
                    raise IOError("Failed to clone repo")
            else:
                os.chdir(repo_path)
                logger.info(' ... Fetching git repo (%s)' % repo_path)
                cmd = 'git pull'
                rc = subprocess.call(cmd.split(' '))
                if rc != 0:
                    raise RuntimeError('Failed to fetch repo')
                logger.info(' ... Fetch complete')

        return Repo(repo_path)
