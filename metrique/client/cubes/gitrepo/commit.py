#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from datetime import datetime
import re
import os
import subprocess

from metrique.client.cubes.basegitobject import BaseGitObject

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

    defaults = {
        'index': True,
    }

    stats = {}

    def extract(self, uri, fetch=True, **kwargs):
        logger.debug("Extracting GIT repo: %s" % uri)
        repo = self.get_repo(uri, fetch)
        last_ts = self.find('uri == "%s"' % uri, fields='_commit_ts',
                            sort=[('_commit_ts', -1)], one=True, raw=True)
        if last_ts:
            last_ts = last_ts['_commit_ts'] + 0.1
        logger.debug("Last Commit Date: %s" % last_ts)
        batch = []
        for walk_entry in repo.get_walker(since=last_ts):
            batch.append(self.get_commit(repo, walk_entry, uri))
        return self.save_objects(batch)

    def get_commit(self, repo, walk_entry, uri):
        c = walk_entry.commit
        if c.type != 1:
            raise TypeError(
                "Expected 'commit' type objects. Got (%s)" % c.type)

        msg = c.message

        acked_by = acked_by_re.findall(msg)
        signed_off_by = signed_off_by_re.findall(msg)
        resolves = resolves_re.findall(msg)
        related = related_re.findall(msg)

        obj = {
            'uri': uri,
            'id': c.id,
            'parents': c.parents,
            'author': c.author,
            'author_dt': datetime.fromtimestamp(c.author_time +
                                                c.author_timezone),
            'committer': c.committer,
            'commit_dt': datetime.fromtimestamp(c.commit_time +
                                                c.commit_timezone),
            '_commit_ts': c.commit_time,
            # FIXME: Can this be sped up?
            # These commented out are all very slow
            #'count': c.count(),
            #'name_rev': c.name_rev,
            #'size': c.size,
            'files': self.get_stats(c)['files'],
            'stats': self.get_stats(c)['stats'],
            'summary': c.message.split('\n', 1)[0],
            'message': c.message,
            'acked_by': acked_by,
            'signed_off_by': signed_off_by,
            'resolves': resolves,
            'related': related,
        }

        return obj

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

        if commit.id not in self.stats:
            os.chdir(self.repo_path)
            cmd = 'git diff --numstat %s' % commit.id
            proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
            self.save_stats(commit.id, proc.stdout.readlines())
        return self.stats[commit.id]

if __name__ == '__main__':
    from metrique.client.argparsers import cube_cli
    a = cube_cli.parse_args()
    obj = Commit(**vars(a))
    obj.extract()
