#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
'''

import os

from metrique.utils import set_default


def drop(self, owner=None, cube=None, force=False):
    '''
    Drops current cube from timeline

    :param string cube: cube name to use
    :param string owner: owner of cube
    :param bool force: really, do it!
    '''
    if not force:
        raise ValueError(
            "DANGEROUS: set force=True to drop %s.%s" % (owner, cube))
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'drop_cube')
    return self._delete(cmd)


def register(self, owner=None, cube=None):
    '''
    Drops current cube from timeline

    :param string owner: owner of cube
    :param string cube: cube name to use
    '''
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'register')
    return self._post(cmd)


######### INDEX #########

def list_index(self, owner=None, cube=None):
    '''
    List indexes for either timeline or warehouse.

    :param string cube: cube name to use
    :param string owner: owner of cube
    '''
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'index')
    return self._get(cmd)


def ensure_index(self, key_or_list, owner=None, cube=None):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    :param string cube: cube name to use
    :param string owner: owner of cube
    '''
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'index')
    return self._post(cmd, ensure=key_or_list)


def drop_index(self, index_or_name, owner=None, cube=None):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    :param string cube: cube name to use
    :param string owner: owner of cube
    '''
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'index')
    return self._delete(cmd, drop=index_or_name)
