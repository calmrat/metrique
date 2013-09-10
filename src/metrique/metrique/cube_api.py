#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
'''

import os

from metrique.utils import set_default


def drop(self, user=None, cube=None, force=False):
    '''
    Drops current cube from timeline

    :param string cube: cube name to use
    :param string user: owner of cube
    :param bool force: really, do it!
    '''
    if not force:
        raise ValueError(
            "DANGEROUS: set force=True to drop %s.%s" % (user, cube))
    user = set_default(user, self.config.api_username)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(user, cube, 'drop_cube')
    return self._delete(cmd)


def register(self, user=None, cube=None):
    '''
    Drops current cube from timeline

    :param string cube: cube name to use
    :param string user: owner of cube
    '''
    user = set_default(user, self.config.api_username)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(user, cube, 'register')
    return self._post(cmd)


######### INDEX #########

def list_index(self, user=None, cube=None):
    '''
    List indexes for either timeline or warehouse.

    :param string cube: cube name to use
    :param string user: owner of cube
    '''
    return self._get('index', cube=cube)


def ensure_index(self, key_or_list, user=None, cube=None):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    :param string cube: cube name to use
    :param string user: owner of cube
    '''
    return self._get('index', cube=cube, ensure=key_or_list)


def drop_index(self, index_or_name, user=None, cube=None):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    :param string cube: cube name to use
    :param string user: owner of cube
    '''
    return self._get('index', cube=cube, drop=index_or_name)
