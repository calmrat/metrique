#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metriquec.cubes.github.issue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the generic metrique cube used
for exctacting issue data from a github repo.

'''

from dateutil.parser import parse as dt_parse
from itertools import chain
import logging

from metrique import pyclient
from metriqueu.utils import dt2ts

logger = logging.getLogger(__name__)

DEFAULT_REPO = 'kejbaly2/metrique'


class Issue(pyclient):
    """
    Object used for extracting issue data from github.com API

    Requires either github auth token or username password!

    :param github_token: github authentication token
    :param github_user: github username
    :param github_pass: github password
    """
    name = 'github_issue'
    _proxy = None

    def __init__(self, github_token=None, github_user=None,
                 github_pass=None, **kwargs):
        super(Issue, self).__init__(**kwargs)
        user = github_user
        _pass = github_pass
        token = github_token
        self.config.github_user = user or self.config.get('github_user')
        self.config.github_pass = _pass or self.config.get('github_pass')
        self.config.github_token = token or self.config.get('github_token')

    def get_objects(self, repo_fullname=DEFAULT_REPO, since=None):
        '''
        Given valid github credentials and a repository name, generate
        a list of github issue objects for all existing issues in the
        repository.

        All issues are returned, including open and closed.

        :param repo_fullname: github repository name (ie, 'user/repo')
        :param since: dateonly return issues updated since date

        An example repo_fullname is 'kejbaly2/metrique'.

        Issue objects contain the following properties:
            * _oid (issue id)
            * assignee
            * body
            * closed_at
            * closed_by
            * created_at
            * labels
            * milestone
            * name
            * number
            * repo url
            * state
            * title
            * updated_at
            * full github url
            * user (reported by)

        '''
        repo_fullname = repo_fullname
        repo = self.proxy.get_repo(repo_fullname)
        if not repo:
            raise ValueError("invalid repo: %s" % repo)

        if isinstance(since, basestring):
            since = dt_parse(since)

        if since:
            _open = repo.get_issues(since=since)
            _closed = repo.get_issues(state='closed', since=since)
        else:
            _open = repo.get_issues()
            _closed = repo.get_issues(state='closed')

        objects = []
        for i in chain(_open, _closed):
            obj = {
                '_oid': i.id,
                'assignee': getattr(i.assignee, 'login', None),
                'body': i.body,
                'closed_at': dt2ts(i.closed_at),
                'closed_by': getattr(i.closed_by, 'login', None),
                'created_at': dt2ts(i.created_at),
                'labels': [l.name for l in i.labels],
                'milestone': getattr(i.milestone, 'title', None),
                'name': repo_fullname,
                'number': i.number,
                'repo': i.repository.url,
                'state': i.state,
                'title': i.title,
                'updated_at': dt2ts(i.updated_at),
                'url': i.url,
                'user': i.user.name,
            }
            objects.append(obj)
            break
        objects = self.normalize(objects)
        return objects

    @property
    def proxy(self):
        '''
        Given a github auth token or username and password,
        connect to the github API using github python module,
        cache it and return it to the caller.
        '''
        try:
            import github
        except ImportError:
            msg = "requires https://github.com/jacquev6/PyGithub"
            raise ImportError(msg)

        if not self._proxy:
            if self.config.github_token:
                self._proxy = github.Github(self.config.github_token)
            else:
                self._proxy = github.Github(self.config.github_user,
                                            self.config.github_pass)
        return self._proxy


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Issue)
