#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Basic gitrepo cube for extracting git object data from git repos

Currently supports extracting the following::

    commit
'''

import re
import subprocess
from gittle.utils.git import commit_info

from metriquec.basegitrepo import BaseGitRepo

related_re = re.compile('Related: (.+)$', re.I)
resolves_re = re.compile('Resolves: (.+)$', re.I)
signed_off_by_re = re.compile('Signed-off-by: (.+)', re.I)
acked_by_re = re.compile('Acked-by: (.+)', re.I)
hash_re = re.compile('[0-9a-f]{40}', re.I)


class Commit(BaseGitRepo):
    """
    Object used for extracting objects from git repos
    """
    name = 'git_commit'

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

    def _extract(self, uri, fetch, delta_shas, force, **kwargs):
        if delta_shas is None:
            delta_shas = []
        self.logger.debug("Extracting GIT repo: %s" % uri)
        self.repo = self.get_repo(uri, fetch)
        known_shas = set()
        if not (force or delta_shas):
            known_shas = self.find('repo_uri == "%s"' % uri,
                                   fields='sha', raw=True)
            known_shas = set([e['sha'] for e in known_shas])
            self.logger.debug("Known Commits: %s" % len(known_shas))
        #repo_shas = set(self.repo.commits())
        cmd = 'git rev-list --all'
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        p = p.communicate()[0]
        repo_shas = set(x for x in p.split('\n') if x)
        self.logger.debug("Total Commits: %s" % len(repo_shas))
        delta_shas = repo_shas - known_shas
        commits = self._build_commits(delta_shas, uri)
        return self.save_objects(commits)

    def extract(self, uri, fetch=True, shas=None, force=False, **kwargs):
        if not isinstance(uri, (list, tuple)):
            uri = [uri]
        commits = dict([(_uri,
                         self._extract(_uri, fetch,
                                       shas, force)) for _uri in uri])
        return commits


if __name__ == '__main__':
    from metrique.argparsers import cube_cli
    cube_cli(Commit)
