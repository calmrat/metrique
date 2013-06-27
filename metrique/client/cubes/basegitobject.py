#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from git import Repo   # , GitCmdObjectDB
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
        # Get diffs for commits (because stats in GitPython is slow):
        self._get_diff_stats(repo_path)

        obj_path = os.path.join(repo_path, DEFAULT_OBJECTS_PATH)
        #return Repo(obj_path, odbt=GitCmdObjectDB)
        return Repo(obj_path)

    def _get_diff_stats(self, repo_path):
        os.chdir(repo_path)
        cmd = 'git log master --format=%H --shortstat'
        proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
        self.stats = {}
        while True:
            line = proc.stdout.readline()
            if line == '':
                break
            line = line.strip()
            if hash_re.match(line) is not None:
                last_hash = line
                self.stats[line] = {}
                self.stats[line]['deletions'] = 0
                self.stats[line]['insertions'] = 0
                self.stats[line]['lines'] = 0
                self.stats[line]['files'] = 0
            files = files_re.findall(line)
            if len(files) > 0:
                ins = insertions_re.findall(line)
                ins = int(ins[0]) if len(ins) > 0 else 0
                dels = deletions_re.findall(line)
                dels = int(dels[0]) if len(dels) > 0 else 0
                self.stats[last_hash]['deletions'] = dels
                self.stats[last_hash]['insertions'] = ins
                self.stats[last_hash]['lines'] = dels + ins
                self.stats[last_hash]['files'] = int(files[0])

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
