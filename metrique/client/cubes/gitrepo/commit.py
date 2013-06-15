#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import re

from metrique.client.cubes.basegitobject import BaseGitObject

related_re = re.compile('Related: (.+)$', re.I)
resolves_re = re.compile('Resolves: (.+)$', re.I)
signed_off_by_re = re.compile('Signed-off-by: (.+)', re.I)
acked_by_re = re.compile('Acked-by: (.+)', re.I)

DEFAULT_CONFIG = {
    "metrique": "https://github.com/drpoovilleorg/metrique.git"
}


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

    def __init__(self, repos=None, **kwargs):
        super(Commit, self).__init__(**kwargs)
        if not repos:
            repos = DEFAULT_CONFIG
        self.repos = repos

    def extract(self, **kwargs):
        result = {}
        for uri in sorted(self.repos.values()):
            logger.debug("Processing repo: %s" % uri)
            result[uri] = self.save_commits(uri)
        return result

    def save_commits(self, uri):
        #c = self.get_collection()
        # FIXME: replace api.find() call
        last_commit_dt = None
        #last_commit_dt = c.find_one({'uri': uri},
        #                            {'_id': 0, 'committed_dt': 1},
        #                            sort=[('committed_dt', -1)])
        #logger.debug("Last Commit Date: %s" % last_commit_dt)
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
