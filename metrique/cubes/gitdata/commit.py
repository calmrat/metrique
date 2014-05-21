#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.cubes.gitdata.commits
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the generic metrique cube used
for exctacting commit data from a git repository.

'''

from __future__ import unicode_literals

import logging
logger = logging.getLogger('metrique')

import os
import re

from metrique import pyclient
from metrique.utils import ts2dt, git_clone, sys_call

related_re = re.compile('Related: (.+)$', re.I)
resolves_re = re.compile('Resolves: (.+)$', re.I)
signed_off_by_re = re.compile('Signed-off-by: (.+)', re.I)
acked_by_re = re.compile('Acked-by: (.+)', re.I)
hash_re = re.compile('[0-9a-f]{40}', re.I)


class Commit(pyclient):
    '''
    Basic gitrepo cube for extracting git object data from git repos

    Currently supports extracting the following::
        * commit
    '''
    name = 'gitdata_repo'

    def get_objects(self, uri, pull=True, **kwargs):
        '''
        Walk through repo commits to generate a list of repo commit
        objects.

        Each object has the following properties:
            * repo uri
            * general commit info
            * files added, removed fnames
            * lines added, removed
            * acked_by
            * signed_off_by
            * resolves
            * related
        '''
        repo = git_clone(uri, pull=pull, reflect=True)
        os.chdir(repo.path)
        # get a full list of all commit SHAs in the repo (all branches)
        cmd = 'git rev-list --all'
        output = sys_call(cmd)
        repo_shas = set(x.strip() for x in output.split('\n') if x)
        logger.debug("Total Commits: %s" % len(repo_shas))

        cmd = 'git --no-pager log --all --format=sha:%H --numstat'
        output = sys_call(cmd)
        all_logs = re.sub('\n+', '\n', output)
        c_logs = [x for x in [s.strip() for s in all_logs.split('sha:')] if x]

        for c_log in c_logs:
            sha, s, all_changes = c_log.partition('\n')
            c = repo.get_object(sha)

            # FIXME: not normalizing to UTC
            _start = ts2dt(c.commit_time)
            _end = None  # once was true, always is true...
            # and some basic stuff...
            obj = dict(_oid=sha, _start=_start, _end=_end,
                       repo_uri=uri, tree=c.tree, parents=c.parents,
                       author=c.author, committer=c.committer,
                       author_time=c.author_time, message=c.message,
                       mergetag=c.mergetag, extra=c.extra)

            for _file in all_changes.split('\n'):
                _file = _file.strip()
                obj.setdefault('files', {})
                if not _file:
                    added, removed, fname = 0, 0, None
                else:
                    added, removed, fname = _file.split('\t')
                    added = 0 if added == '-' else int(added)
                    removed = 0 if removed == '-' else int(removed)
                    # FIXME: sql doesn't nest well..
                    changes = {'added': added,
                               'removed': removed}
                    obj['files'][fname] = changes

            # file +/- totals
            obj['added'] = sum(
                [v.get('added', 0) for v in obj['files'].itervalues()])
            obj['removed'] = sum(
                [v.get('removed', 0) for v in obj['files'].itervalues()])

            # extract interesting bits from the message
            obj['acked_by'] = acked_by_re.findall(c.message)
            obj['signed_off_by'] = signed_off_by_re.findall(c.message)
            obj['resolves'] = resolves_re.findall(c.message)
            obj['related'] = related_re.findall(c.message)
            self.objects.add(obj)

        return super(Commit, self).get_objects(**kwargs)
