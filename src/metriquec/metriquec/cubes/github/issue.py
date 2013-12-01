#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from dateutil.parser import parse as dt_parse
from itertools import chain

from metrique.core_api import HTTPClient
from metriqueu.utils import dt2ts

DEFAULT_REPO = 'drpoovilleorg/metrique'


class Issue(HTTPClient):
    """
    Object used for extracting issue data from github.com API
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

        for i in chain(_open, _closed):
            obj = {
                '_oid': i.id,
                'assignee': getattr(i, 'assignee.name', None),
                'body': i.body,
                'closed_at': dt2ts(i.closed_at),
                'closed_by': i.closed_by,
                'created_at': dt2ts(i.created_at),
                'labels': [l.name for l in i.labels],
                'milestone': getattr(i, 'milestone.title', None),
                'name': repo_fullname,
                'number': i.number,
                'repo': i.repository.url,
                'state': i.state,
                'title': i.title,
                'updated_at': dt2ts(i.updated_at),
                'url': i.url,
                'user': i.user.name,
            }
            self.objects.append(obj)
        return self.objects

    def extract(self, repo_fullname=None, since=None, save=True, **kwargs):
        '''
        Go to github.com user account page to generate a new token

        repo_fullname::
            eg, drpoovilleorg/metrique
        '''
        objs = self.get_objects(repo_fullname=repo_fullname, since=since)
        if save:
            self.cube_save(objs)
        return objs

    @property
    def proxy(self):
        if not self._proxy:
            import github
            if self.config.github_token:
                self._proxy = github.Github(self.config.github_token)
            else:
                self._proxy = github.Github(self.config.github_user,
                                            self.config.github_pass)
        return self._proxy


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Issue)
