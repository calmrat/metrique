#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from datetime import datetime
import re
import os
import subprocess

from metrique.client.cubes.basegitobject import BaseGitObject
from metrique.tools import oid as new_oid

related_re = re.compile('Related: (.+)$', re.I)
resolves_re = re.compile('Resolves: (.+)$', re.I)
signed_off_by_re = re.compile('Signed-off-by: (.+)', re.I)
acked_by_re = re.compile('Acked-by: (.+)', re.I)

hash_re = re.compile('[0-9a-f]{40}', re.I)


class Commit(BaseGitObject):
    """
    Object used for communication with Git Commit interface
    """
    name = 'git_commit'

    def extract(self, uri, fetch=True, shas=None, force=False,
            **kwargs):
        if shas is None:
            shas = []
        self.logger.debug("Extracting GIT repo: %s" % uri)
        self.repo = self.get_repo(uri, fetch)
        self.commits = tuple(self.repo.commits())
        if not (force or shas):
            last_ts = self.find(
                    'uri == "%s"' % uri, fields='_commit_ts',
                    sort=[('_commit_ts', -1)], one=True, raw=True)
            if last_ts:
                last_ts = last_ts['_commit_ts'] + 0.1
            self.logger.debug("Last Commit Date: %s" % last_ts)
        self.stats = {}
        batch = []
        objs = self.repo.commit_info()
        for obj in objs:
            obj['_oid'] = new_oid()
            obj['uri'] = uri
            sha = obj['sha']
            index = self.commits.index(sha)
            obj['stats'] = self.get_stats(sha)
            msg = obj['message']

            obj['acked_by'] = acked_by_re.findall(msg)
            obj['signed_off_by'] = signed_off_by_re.findall(msg)
            obj['resolves'] = resolves_re.findall(msg)
            obj['related'] = related_re.findall(msg)

            obj['diffs'] = self.repo.diff(sha)

            batch.append(obj)
        return self.save_objects(batch)

    def save_stats(self, sha, stats):
        files = []
        ins, dels, n = 0, 0, 0
        for st in stats:
            split = st.split('\t')
            if len(split) == 3:
                a, d, f = split
                a = 0 if a == '-' else int(a)
                d = 0 if d == '-' else int(d)
                n += 1
                ins += a
                dels += d
                files.append({'name': f, 'insertions': a, 'deletions': d})
        self.stats[sha] = {'files': files, 'stats': {'insertions': ins,
                                                     'deletions': dels,
                                                     'files': n}}

    def get_stats(self, commit):
        if len(self.stats) == 0:
            # import from log
            os.chdir(self.repo_path)
            cmd = 'git log --format=%H --numstat'
            proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
            last_hash, stats = None, []
            for line in proc.stdout.readlines():
                if hash_re.match(line) is not None:
                    if last_hash:
                        self.save_stats(last_hash, stats)
                    last_hash = line.strip()
                    stats = []
                else:
                    stats.append(line.strip())
            self.save_stats(last_hash, stats)

        if commit not in self.stats:
            os.chdir(self.repo_path)
            cmd = 'git diff --numstat %s' % commit
            proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
            self.save_stats(commit, proc.stdout.readlines())
        return self.stats[commit]

if __name__ == '__main__':
    from metrique.client.argparsers import cube_cli
    a = cube_cli.parse_args()
    obj = Commit(**vars(a))
    obj.extract()
