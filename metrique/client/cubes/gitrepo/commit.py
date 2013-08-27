#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import re
import os
import subprocess
import sys
import traceback

from metrique.client.cubes.basegitobject import BaseGitObject
from metrique.client.utils import new_oid

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

    def _build_obj(self, obj, **kwargs):
        obj.update(kwargs)

        sha = obj['sha']

        obj['repo_path'] = self.repo_path
        obj['index'] = self.commits.index(sha)
        obj['stats'] = self.get_stats(sha)
        obj['diffs'] = self.repo.diff(sha)

        msg = obj['message']
        obj['acked_by'] = acked_by_re.findall(msg)
        obj['signed_off_by'] = signed_off_by_re.findall(msg)
        obj['resolves'] = resolves_re.findall(msg)
        obj['related'] = related_re.findall(msg)
        return obj

    def _extract(self, uri, fetch, shas, force, **kwargs):
        if shas is None:
            shas = []
        self.logger.debug("Extracting GIT repo: %s" % uri)
        self.repo = self.get_repo(uri, fetch)
        self.commits = tuple(self.repo.commits())
        if not (force or shas):
            # FIXME: Get a list of all known shas (first 8char)
            # and do set diff against all shas in repo currently
            # then only extract the difference
            known_shas = self.find('uri == "%s"' % uri,
                                   fields='sha', raw=True)
            known_shas = set([e['sha'] for e in known_shas])
            self.logger.debug("Known Commits: %s" % len(known_shas))
        repo_shas = set(self.repo.commits())
        delta_shas = repo_shas - known_shas
        self.stats = {}
        batch = []
        objs = self.repo.commit_info()
        batch = [self._build_obj(obj, _oid=new_oid(),
                 uri=uri) for obj in objs if obj['sha'] in delta_shas]
        return self.save_objects(batch)

    def extract(self, uri, fetch=True, shas=None, force=False, **kwargs):
        if not isinstance(uri, (list, tuple)):
            uri = [uri]
        commits = []
        for _uri in uri:
            try:
                c = self._extract(_uri, fetch, shas, force)
            except Exception:
                tb = traceback.format_exc(sys.exc_info())
                self.logger.error('Extract FAILED: %s' % tb)
            else:
                commits.append(c)
        return commits

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
    kwargs = {}
    kwargs.update(a.cube_init_kwargs_config_file)
    if a.debug:
        kwargs.update({'debug': a.debug})
    obj = Commit(config_file=a.cube_config_file, **kwargs)
    obj.extract(force=a.force)
