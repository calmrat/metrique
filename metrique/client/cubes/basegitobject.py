#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

try:
    from gittle.gittle import Gittle
except Exception as e:
    # FIXME: TEMP override for fedora 18's (17?)
    # insecure version of some deps needed by dulwhich
    # '"Install paramiko to have better SSH support...'
    import traceback
    import sys
    tb = traceback.format_exc(sys.exc_info())
    print 'SECURITY WARNING (upgrade : %s' % e
    print tb
    print 'are you shure you want to continue? Enter or ^C to quit '
    raw_input()
    from gittle.gittle import Gittle

import os
import subprocess

from metrique.client.cubes.basecube import BaseCube
from metrique.client.config import DEFAULT_CONFIG_DIR

TMP_DIR = os.path.join(DEFAULT_CONFIG_DIR, 'gitrepos/')


class BaseGitObject(BaseCube):
    """
    Driver to help extract data from GIT repos
    """
    def __init__(self, **kwargs):
        super(BaseGitObject, self).__init__(**kwargs)

    def get_repo(self, uri, fetch=True, tmp_dir=None):
        if tmp_dir is None:
            tmp_dir = TMP_DIR
        tmp_dir = os.path.expanduser(tmp_dir)
        repo_path = os.path.join(tmp_dir, str(abs(hash(uri))))
        self.repo_path = os.path.expanduser(repo_path)
        self.logger.debug('GIT URI: %s' % uri)
        if fetch:
            if not os.path.exists(repo_path):
                self.logger.info('Cloning git repo to %s' % repo_path)
                cmd = 'git clone %s %s' % (uri, repo_path)
                rc = subprocess.call(cmd.split(' '))
                if rc != 0:
                    raise IOError("Failed to clone repo")
            else:
                os.chdir(repo_path)
                self.logger.info(' ... Fetching git repo (%s)' % repo_path)
                cmd = 'git fetch'
                rc = subprocess.call(cmd.split(' '))
                if rc != 0:
                    raise RuntimeError('Failed to fetch repo')
                self.logger.debug(' ... Fetch complete')

        self.logger.debug('Loading repo: %s ' % Gittle)
        return Gittle(repo_path)
