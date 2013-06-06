#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import re

from metrique.server.drivers.basegitobject import BaseGitObject
from metrique.server.drivers.utils import ts_tz2dt_tz
from metrique.server.etl import last_known_warehouse_mtime
from metrique.server.etl import save_objects
from metrique.tools.type_cast import type_cast

tree_re = re.compile('tree ([0-9a-f]{5,40})')
parent_re = re.compile('parent ([0-9a-f]{5,40})')
author_ts_re = re.compile('author ([^>]+>)\s(\d+\s[+-]\d{4})')
committer_ts_re = re.compile('committer ([^>]+>)\s(\d+\s[+-]\d{4})')

related_re = re.compile('Related: (.+)$')
resolves_re = re.compile('Resolves: (.+)$')
signed_off_by_re = re.compile('Signed-off-by: (.+)')


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
                'author': {
                    'help': '',
                },

                'author_ts': {
                    'type': datetime,
                    'convert': ts_tz2dt_tz,
                    'help': '',
                },

                'committer': {
                    'help': '',
                },

                'committer_ts': {
                    'type': datetime,
                    'convert': ts_tz2dt_tz,
                    'help': '',
                },

                'hexsha': {
                    'help': '',
                },

                'message': {
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

                'tree': {
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
        last_update_dt = last_known_warehouse_mtime(self.name, 'uri', uri)

        commits = []
        for obj in self.walk_objects(uri, 'commit'):
            commit = self.extract_commit(obj)
            if last_update_dt:
                committer_ts = commit['committer_ts']
                obj_cached = ts_tz2dt_tz(committer_ts) <= last_update_dt
                if obj_cached:
                    continue
            commit.update({'uri': uri})
            for f, v in commit.iteritems():
                convert = self.get_field_property('convert', f)
                _type = self.get_field_property('type', f)
                if convert:
                    commit[f] = convert(v)
                v = type_cast(v, _type)
            commits.append(commit)
        return save_objects(self.name, commits)

    def extract_commit(self, obj):
        if obj.type != 'commit':
            raise TypeError(
                "Expected 'commit' type objects. Got (%s)" % obj.type)
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
        committer, committer_ts = committer_ts_re.match(
            committer_ts).group(1, 2)

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
