#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metrique.cubes.gitdata.repo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the generic metrique cube used
for exctacting data from a git repository.

.. note:: This cube requires python 2.7+
'''

import logging
logger = logging.getLogger(__name__)

try:
    from gittle.gittle import Gittle
    from gittle.utils.git import commit_info
    HAS_GITTLE = True
except ImportError:
    logger.warn("gittle package not found!")
    HAS_GITTLE = False

import os
import re
import tempfile
import subprocess

from metrique import pyclient

related_re = re.compile('Related: (.+)$', re.I)
resolves_re = re.compile('Resolves: (.+)$', re.I)
signed_off_by_re = re.compile('Signed-off-by: (.+)', re.I)
acked_by_re = re.compile('Acked-by: (.+)', re.I)
hash_re = re.compile('[0-9a-f]{40}', re.I)

TMP_DIR = os.environ['METRIQUE_TMP'] or tempfile.gettempdir()


class Repo(pyclient):
    '''
    Basic gitrepo cube for extracting git object data from git repos

    Currently supports extracting the following::
        * commit
    '''
    name = 'gitdata_repo'

    def __init__(self, cache_dir=None, **kwargs):
        if not HAS_GITTLE:
            raise ImportError("`pip install gittle` required")
        super(Repo, self).__init__(**kwargs)
        self.cache_dir = os.path.expanduser(cache_dir or TMP_DIR)

    def get_repo(self, uri, pull=True):
        '''
        Given a git repo, clone (cache) it locally and
        return back a Gittle object instance to caller.

        :param uri: git repo uri
        :param pull: whether to pull after cloning (or loading cache)
        '''
        # FIXME: use gittle to clone repos; bare=True
        # make the uri safe for filesystems
        _uri = "".join(x for x in uri if x.isalnum())
        repo_path = os.path.join(self.cache_dir, _uri)
        self.repo_path = repo_path = os.path.expanduser(repo_path)
        logger.debug('GIT URI: %s' % uri)
        if pull:
            logger.info('git repo tmp path %s' % repo_path)
            if not os.path.exists(repo_path):
                logger.info(' ... cloning git repo')
                cmd = 'git clone %s %s' % (uri, repo_path)
                rc = subprocess.call(cmd.split(), stderr=subprocess.PIPE,
                                     stdout=subprocess.PIPE)
                if rc != 0:
                    raise IOError("Failed to clone repo (%s)" % cmd)
                logger.info(' ... clone complete')
            else:
                os.chdir(repo_path)
                logger.info(' ... fetching git repo (%s)' % repo_path)
                cmd = 'git pull'
                rc = subprocess.call(cmd.split(), stderr=subprocess.PIPE,
                                     stdout=subprocess.PIPE)
                if rc != 0:
                    raise RuntimeError('Failed to pull repo (%s)' % cmd)
                logger.debug(' ... ... fetch complete')
        return Gittle(repo_path)

    def _build_commits(self, delta_shas, uri):
        cmd = 'git --no-pager log --all --format=sha:%H --numstat'
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        all_logs = p.communicate()[0]
        all_logs = re.sub('\n+', '\n', all_logs)
        with open('/tmp/f', 'w') as f:
            f.write(all_logs)
        c_logs = [x for x in [s.strip() for s in all_logs.split('sha:')] if x]

        commits = []
        for c_log in c_logs:
            sha, s, all_changes = c_log.partition('\n')
            if not sha in delta_shas:
                continue
            # and some basic stuff...
            obj = commit_info(self.repo[sha])
            obj['_oid'] = uri + sha
            obj['repo_uri'] = uri
            for _file in all_changes.split('\n'):
                _file = _file.strip()
                obj.setdefault('files', [])
                if not _file:
                    added, removed, fname = 0, 0, None
                    obj['files'].append({})
                else:
                    added, removed, fname = _file.split('\t')
                    added = 0 if added == '-' else int(added)
                    removed = 0 if removed == '-' else int(removed)
                    changes = {'name': fname,
                               'added': added,
                               'removed': removed}
                    obj['files'].append(changes)

            # file +/- totals
            obj['added'] = sum(
                [v.get('added', 0) for v in obj['files']])
            obj['removed'] = sum(
                [v.get('removed', 0) for v in obj['files']])

            # extract interesting bits from the message
            msg = obj['message']
            obj['acked_by'] = acked_by_re.findall(msg)
            obj['signed_off_by'] = signed_off_by_re.findall(msg)
            obj['resolves'] = resolves_re.findall(msg)
            obj['related'] = related_re.findall(msg)
            commits.append(obj)
        # pull objects out of indexed dict and into an array
        return commits

# FIXME: MOVE THIS TO separate gitrepo.commits cube
#    def get_objects(self, uri, fetch=True, **kwargs):
#        '''
#        Walk through repo commits to generate a list of repo commit
#        objects.
#
#        Each object has the following properties:
#            * repo uri
#            * general commit info (see gittle.utils.git.commit_info)
#            * files added, removed fnames
#            * lines added, removed
#            * acked_by
#            * signed_off_by
#            * resolves
#            * related
#        '''
#        logger.debug("Extracting GIT repo: %s" % uri)
#        self.repo = self.get_repo(uri, fetch)
#        cmd = 'git rev-list --all'
#        p = subprocess.Popen(cmd.split(), stderr=subprocess.PIPE,
#                             stdout=subprocess.PIPE)
#        p = p.communicate()[0]
#        repo_shas = set(x for x in p.split('\n') if x)
#        logger.debug("Total Commits: %s" % len(repo_shas))
#        self.objects = self._build_commits(repo_shas, uri)
#        super(Repo, self).get_objects(**kwargs)
#        return self


if __name__ == '__main__':
    from metrique.argparsers import cube_cli
    cube_cli(Repo)
