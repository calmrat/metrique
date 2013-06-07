#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import re

from metrique.server.drivers.basegitobject import BaseGitObject
from metrique.server.etl import save_object2
from metrique.tools.type_cast import type_cast

related_re = re.compile('Related: (.+)$', re.I)
resolves_re = re.compile('Resolves: (.+)$', re.I)
signed_off_by_re = re.compile('Signed-off-by: (.+)', re.I)
acked_by_re = re.compile('Acked-by: (.+)', re.I)


class Commit(BaseGitObject):
    """
    Object used for communication with Git Commit interface
    """
    def __init__(self, repos, **kwargs):
        super(Commit, self).__init__(**kwargs)
        self.repos = repos

        self.cube = {
            'defaults': {
                'index': True,
            },

            'fielddefs': {
                'acked_by': {
                    'help': '',
                },

                'author_name': {
                    'help': '',
                },

                'author_email': {
                    'help': '',
                },

                'author_tz_offset': {
                    'type': float,
                    'help': '',
                },

                'authored_dt': {
                    'type': datetime,
                    'convert': datetime.fromtimestamp,
                    'help': '',
                },

                'committer_name': {
                    'help': '',
                },

                'committer_email': {
                    'help': '',
                },

                'committer_tz_offset': {
                    'type': float,
                    'help': '',
                },

                'committed_dt': {
                    'type': datetime,
                    'convert': datetime.fromtimestamp,
                    'help': '',
                },

                'count': {
                    'enabled': False,
                    'type': float,
                    'help': '',
                },

                'files': {
                    'enabled': False,
                    'type': dict,
                    'help': '',
                },

                'hexsha': {
                    'help': '',
                },

                'message': {
                    'help': '',
                },

                'name_rev': {
                    'enabled': False,
                    'help': '',
                },

                'parents': {
                    'help': '',
                },

                'repo_name': {
                    'help': '',
                },

                'resolves': {
                    'help': '',
                },

                'related': {
                    'help': '',
                },

                'signed_off_by': {
                    'help': '',
                },

                'size': {
                    'enabled': False,
                    'type': float,
                    'help': '',
                },

                'stats': {
                    'type': dict,
                    'help': '',
                },

                'summary': {
                    'help': '',
                },

                'uri': {
                    'help': '',
                },
            }
        }

    def extract_func(self, **kwargs):
        result = {}
        for uri in sorted(self.repos.values()):
            logger.debug("Processing repo: %s" % uri)
            result[uri] = self.save_commits(uri)
        return result

    def save_commits(self, uri):
        c = self.get_collection()
        last_commit_dt = c.find_one({'uri': uri},
                                    {'_id': 0, 'committed_dt': 1},
                                    sort=[('committed_dt', -1)])
        logger.debug("Last Commit Date: %s" % last_commit_dt)
        saved = 0
        for commit in self.walk_commits(uri, last_commit_dt):
            saved += self.save_commit(commit, uri)
        return saved

    def save_commit(self, commit, uri):
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
            #'files': dict([(k.replace('.', '%2E'), v) for k, v in c.stats.files.iteritems()]),
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
            convert = self.get_field_property('convert', f)
            _type = self.get_field_property('type', f)
            if convert:
                v = convert(v)
            obj[f] = type_cast(v, _type)
        return save_object2(self.name, obj)
