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

    fields = {
        'acked_by': {},
        'author_name': {},
        'author_email': {},
        'author_tz_offset': {
            'type': float,
        },
        'authored_dt': {
            'type': datetime,
            'convert': datetime.fromtimestamp,
        },
        'committer_name': {},
        'committer_email': {},
        'committer_tz_offset': {
            'type': float,
        },
        'committed_dt': {
            'type': datetime,
            'convert': datetime.fromtimestamp,
        },
        'count': {
            'enabled': False,
            'type': float,
        },
        'files': {
            'enabled': False,
            'type': dict,
        },
        'hexsha': {},
        'message': {},
        'name_rev': {
            'enabled': False,
        },
        'parents': {},
        'repo_name': {},
        'resolves': {},
        'related': {},
        'signed_off_by': {},
        'size': {
            'enabled': False,
            'type': float,
        },
        'stats': {
            'type': dict,
        },
        'summary': {},
        'uri': {},
    }

    def extract(self, uri, name=None, **kwargs):
        logger.debug("Extracting GIT repo: %s" % uri)
        return self.save_commits(uri, name)

    def save_commits(self, uri, name=None):
        if not name:
            name = uri.split('/')[-1].replace('.git', '')
        last_commit_dt = self.find('uri == "%s"' % uri,
                                   fields='committed_dt',
                                   sort=[('committed_dt', -1)],
                                   one=True, raw=True)
        logger.debug("Last Commit Date: %s" % last_commit_dt)
        commits = self.walk_commits(uri, last_commit_dt)
        batch = []
        for saved, commit in enumerate(commits):
            obj = self.get_commit(commit, uri)
            batch.append(obj)
            if len(batch) == 100:
                self.save_objects(batch)
                batch = []
        else:
            self.save_objects(batch)
        return saved

    def get_commit(self, commit, uri):
        c = commit
        if c.type != 'commit':
            raise TypeError(
                "Expected 'commit' type objects. Got (%s)" % c.type)

        msg = c.message

        acked_by = acked_by_re.findall(msg)
        signed_off_by = signed_off_by_re.findall(msg)
        resolves = resolves_re.findall(msg)
        related = related_re.findall(msg)

        obj = {
            'uri': uri,
            'hexsha': c.hexsha,
            'parents': [t.hexsha for t in c.parents],
            'author_name': c.author.name,
            'author_email': c.author.email,
            'author_tz_offset': c.author_tz_offset,
            'authored_dt': c.authored_date,
            'committer_name': c.committer.name,
            'committer_email': c.committer.email,
            'committer_tz_offset': c.committer_tz_offset,
            'committed_dt': c.committed_date,
            # FIXME: Can this be sped up?
            # These commented out are all very slow
            #'count': c.count(),
            #'name_rev': c.name_rev,
            #'size': c.size,
            #'files': dict(
            #    [(k.replace('.', '%2E'),
            #      v) for k, v in c.stats.files.iteritems()]),
            # stats is very slow too; but it's useful...
            'stats': c.stats.total,
            'summary': c.summary,
            'message': msg,
            'acked_by': acked_by,
            'signed_off_by': signed_off_by,
            'resolves': resolves,
            'related': related,
        }

        _obj = obj.copy()
        for f, v in _obj.iteritems():
            convert = self.get_property('convert', f)
            if convert:
                v = convert(v)

        return obj


if __name__ == '__main__':
    from metrique.client.argparsers import cube_cli
    a = cube_cli.parse_args()
    obj = Commit(**vars(a))
    obj.extract()
