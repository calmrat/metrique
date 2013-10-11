#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Base cube for extracting data from GIT repositories
'''

try:
    eval('{x: x for x in range(1)}')
except SyntaxError:
    # gittle has tons of dict comprehensions...
    raise RuntimeError("This cube requires python 2.7+")
else:
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
        print 'are you shure you want to continue? Enter or CTRL-C to quit '
        raw_input()
        # importing a second time; it'll work this time
        from gittle.gittle import Gittle

import os
import subprocess

from metrique.core_api import HTTPClient

TMP_DIR = '~/.metrique/gitrepos'


class BaseGitRepo(HTTPClient):
    """
    Driver to help extract data from GIT repos
    """
    def __init__(self, **kwargs):
        super(BaseGitRepo, self).__init__(**kwargs)
        self.tmp_dir = os.path.expanduser(TMP_DIR)

    def get_repo(self, uri, fetch=True, tmp_dir=None):
        # FIXME: use gittle to clone repos; bare=True
        if tmp_dir is None:
            tmp_dir = self.tmp_dir
        tmp_dir = os.path.expanduser(tmp_dir)
        # make the uri safe for filesystems
        _uri = "".join(x for x in uri if x.isalnum())
        repo_path = os.path.join(tmp_dir, _uri)
        self.repo_path = repo_path = os.path.expanduser(repo_path)
        self.logger.debug('GIT URI: %s' % uri)
        if fetch:
            if not os.path.exists(repo_path):
                self.logger.info('Cloning git repo to %s' % repo_path)
                cmd = 'git clone %s %s' % (uri, repo_path)
                rc = subprocess.call(cmd.split())
                if rc != 0:
                    raise IOError("Failed to clone repo")
            else:
                os.chdir(repo_path)
                self.logger.info(' ... Fetching git repo (%s)' % repo_path)
                cmd = 'git fetch'
                rc = subprocess.call(cmd.split())
                if rc != 0:
                    raise RuntimeError('Failed to fetch repo')
                self.logger.debug(' ... Fetch complete')
        return Gittle(repo_path)
