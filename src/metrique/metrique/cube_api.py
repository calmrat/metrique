#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
This module contains all the Cube related api
functionality.

Create/Drop/Update cubes.
Save/Remove cube objects.
Create/Drop cube indexes.
'''

from datetime import datetime
from time import time
import os
import pytz

from metriqueu.utils import batch_gen, set_default

DEFAULT_BATCH = 100000


def drop(self, cube=None, owner=None, force=False):
    '''
    Drops current cube from timeline

    :param string cube: cube name to use
    :param string owner: owner of cube
    :param bool force: really, do it!
    '''
    if not force:
        raise ValueError(
            "DANGEROUS: set force=True to drop %s.%s" % (owner, cube))
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'drop_cube')
    return self._delete(cmd)


def register(self, cube=None, owner=None):
    '''
    Drops current cube from timeline

    :param string owner: owner of cube
    :param string cube: cube name to use
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'register')
    return self._post(cmd)


def update_role(self, username, cube=None, action='push',
                role='__read__', owner=None):
    '''
    Add/Remove cube ACLs

    :param string: cube name to use
    :param string owner: owner of cube
    :param string action: action to take (push, pull)
    :param string role:
        Permission: __read__, __write__, __admin__)
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'update_cube_role')
    return self._post(cmd,
                      cube=cube, owner=owner,
                      username=username,
                      action=action, role=role)


######### INDEX #########

def list_index(self, cube=None, owner=None):
    '''
    List indexes for either timeline or warehouse.

    :param string cube: cube name to use
    :param string owner: owner of cube
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'index')
    return self._get(cmd)


def ensure_index(self, key_or_list, cube=None, owner=None):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    :param string cube: cube name to use
    :param string owner: owner of cube
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'index')
    return self._post(cmd, ensure=key_or_list)


def drop_index(self, index_or_name, cube=None, owner=None):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    :param string cube: cube name to use
    :param string owner: owner of cube
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'index')
    return self._delete(cmd, drop=index_or_name)


######## SAVE/REMOVE ########
def save_objects(self, objects, batch=DEFAULT_BATCH, cube=None, owner=None):
    '''
    :param list objects: list of dictionary-like objects to be stored
    :param string owner: owner of cube
    :param string cube: cube name to use
    :param int batch: maximum slice of objects to post at a time
    :rtype: list - list of object ids saved

    Save a list of objects the given metrique.cube.

    Returns back a list of object ids (_id|_oid) saved.
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)

    olen = len(objects) if objects else None
    if not olen:
        self.logger.debug("... No objects to save")
        return []

    # get 'now' utc timezone aware datetime object
    now = pytz.UTC.localize(datetime.utcnow())

    cmd = os.path.join(owner, cube, 'save_objects')

    t1 = time()
    if olen <= batch:
        saved = self._post(cmd, objects=objects, mtime=now)
    else:
        saved = []
        for _batch in batch_gen(objects, batch):
            _saved = self._post(cmd, objects=_batch, mtime=now)
            saved.extend(_saved)

    self.logger.info("... Saved %s docs in ~%is" % (olen, time() - t1))
    return sorted(list(set([o['_oid'] for o in objects if o])))


def remove_objects(self, ids, backup=False, cube=None, owner=None):
    '''
    Remove objects from cube timeline

    :param list ids: list of object ids to remove
    :param bool backup: return the documents removed to client?
    :param string cube: cube name to use
    '''
    owner = set_default(owner, self.config.api_username, required=True)
    cube = set_default(cube, self.name, required=True)
    cmd = os.path.join(owner, cube, 'remove_objects')
    if not ids:
        return True
    else:
        return self._delete(cmd, ids=ids, backup=backup)
