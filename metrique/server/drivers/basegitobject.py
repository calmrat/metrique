#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from gitdb.db.git import GitDB
import logging
logger = logging.getLogger(__name__)
import os
import subprocess
import re

from metrique.server.etl import last_known_warehouse_mtime
from metrique.server.drivers.basedriver import BaseDriver

TMP_DIR = '/tmp'
DEFAULT_OBJECTS_PATH = '.git/objects'

tree_re = re.compile('tree ([0-9a-f]{5,40})')
parent_re = re.compile('parent ([0-9a-f]{5,40})')
author_ts_re = re.compile('author ([^>]+>)\s(\d+\s[+-]\d{4})')
committer_ts_re = re.compile('committer ([^>]+>)\s(\d+\s[+-]\d{4})')

related_re = re.compile('Related: (.+)$')
resolves_re = re.compile('Resolves: (.+)$')
signed_off_by_re = re.compile('Signed-off-by: (.+)')


class BaseGitObject(BaseDriver):
    """
    Object used for communication with CSV files
    """
    def __init__(self, *args, **kwargs):
        super(BaseGitObject, self).__init__(*args, **kwargs)

    def load_commits(self, uri, repo):
        last_update_dt = last_known_warehouse_mtime(self.name)
        ts_convert = self.get_field_property('convert', 'committer_ts')
        if not hasattr(self, '_commits'):
            self._commits = {}
        if uri not in self._commits:
            self._commits.setdefault(uri, [])
            repo = uri.split('/')[-1]
            repo_path = self.fetch_repo(uri, repo)
            obj_path = os.path.join(repo_path, DEFAULT_OBJECTS_PATH)
            gitdb = GitDB(obj_path)
            logger.debug("Iterating through object db")
            for sha in gitdb.sha_iter():
                obj = gitdb.stream(sha)
                obj_dump = self._extract_commit(obj)
                if not obj_dump:
                    continue
                else:
                    self._commits[uri].append(obj_dump)

        self.reader = []
        logger.debug("Walking object list")
        for obj_dump in reversed(self._commits[uri]):
            when = ts_convert(obj_dump['committer_ts'])
            if last_update_dt and when < last_update_dt:
                # going in reverse, so break out if 'committer_ts' > last_update_dt
                # cause it means everything else is going to be skipped anyway
                break
            else:
                obj_dump.update({'uri': uri})
                self.reader.append(obj_dump)

    def _extract_commit(self, obj):
        if obj.type != 'commit':
            return None
        hexsha = obj.hexsha
        commit_i = obj.read().split('\n')
        t_ix = 0
        tree = commit_i[t_ix]
        tree = tree_re.match(tree).group(1)

        parents = []
        p_ix = t_ix + 1
        parent = commit_i[p_ix]
        p_matcher = parent_re.match(parent)
        while p_matcher:
            parents.append(p_matcher.group(1))
            p_ix += 1
            p_matcher = parent_re.match(commit_i[p_ix])

        a_ts_ix = p_ix
        author_ts = commit_i[a_ts_ix]
        author, author_ts = author_ts_re.match(author_ts).group(1, 2)

        c_ts_ix = a_ts_ix + 1
        committer_ts = commit_i[c_ts_ix]
        committer, committer_ts = committer_ts_re.match(committer_ts).group(1, 2)

        b_ix = c_ts_ix + 1
        blank = commit_i[b_ix]
        assert blank == ''
        co_ix = b_ix + 1
        message = commit_i[co_ix:]
        message_str = '\n'.join(message)

        signed_off_by = signed_off_by_re.findall(message_str)
        if not signed_off_by:
            signed_off_by = None

        # These need to be further cut up into individual ids
        # rather than a list of groups of strings...
        resolves = resolves_re.findall(message_str)
        if not resolves:
            resolves = None

        related = related_re.findall(message_str)
        if not related:
            related = None

        return {'hexsha': hexsha, 'tree': tree, 'parents': parents,
                'author': author, 'author_ts': author_ts,
                'committer': committer, 'committer_ts': committer_ts,
                'message': message_str, 'related': related,
                'resolves': resolves, 'signed_off_by': signed_off_by}

    # FIXME: redirect output to logger
    def fetch_repo(self, uri, repo):
        repo_path = os.path.join(TMP_DIR, repo)
        if not hasattr(self, '_fetched'):
            logger.info('GIT URI: %s' % uri)
            if not os.path.exists(repo_path):
                logger.info('Cloning git repo')
                cmd = 'git clone %s %s' % (uri, repo_path)
                rc = subprocess.call(cmd.split(' '))
                if rc != 0:
                    raise IOError("Failed to clone repo")
            os.chdir(repo_path)
            logger.info(' ... Pulling git repo')
            cmd = 'git pull'
            rc = subprocess.call(cmd.split(' '))
            if rc != 0:
                raise RuntimeError('Failed to pull repo')
            self._fetched = True
            logger.info(' ... Pull complete')
        return repo_path
