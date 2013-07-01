#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from datetime import datetime
import re

from metrique.client.cubes.basegitobject import BaseGitObject

related_re = re.compile('Related: (.+)$', re.I)
resolves_re = re.compile('Resolves: (.+)$', re.I)
signed_off_by_re = re.compile('Signed-off-by: (.+)', re.I)
acked_by_re = re.compile('Acked-by: (.+)', re.I)


class Commit(BaseGitObject):
    """
    Object used for communication with Git Commit interface
    """

    name = 'git_commit'

    defaults = {
        'index': True,
    }

    def extract(self, uri, name=None, **kwargs):
        logger.debug("Extracting GIT repo: %s" % uri)
        return self.save_commits(uri, name)

    def save_commits(self, uri, name=None):
        repo = self.fetch_repo(uri)
        last_ts = self.find('uri == "%s"' % uri, fields='_commit_ts',
                            sort=[('_commit_ts', -1)], one=True, raw=True)
        if last_ts:
            last_ts = last_ts['_commit_ts'] + 0.1
        logger.debug("Last Commit Date: %s" % last_ts)
        batch = []
        for walk_entry in repo.get_walker(since=last_ts):
            batch.append(self.get_commit(repo, walk_entry.commit, uri))
        return self.save_objects(batch)

    def get_commit(self, repo, commit, uri):
        c = commit
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
            #'files': dict(
            #    [(k.replace('.', '%2E'),
            #      v) for k, v in c.stats.files.iteritems()]),
            # stats is very slow too; but it's useful... arg.
            # 'stats': self.stats[c.hexsha],
            #'summary': c.summary,
            'message': c.message,
            'acked_by': acked_by,
            'signed_off_by': signed_off_by,
            'resolves': resolves,
            'related': related,
        }

        return obj


if __name__ == '__main__':
    from metrique.client.argparsers import cube_cli
    a = cube_cli.parse_args()
    obj = Commit(**vars(a))
    obj.extract()
