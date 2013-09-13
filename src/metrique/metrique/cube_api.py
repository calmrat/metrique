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


def list_all(self, startswith=None):
    ''' List all valid cubes for a given metrique instance '''
    return self._get(startswith)


def list_cube_fields(self, cube=None, owner=None,
                     exclude_fields=None, _mtime=False):
    '''
    List all valid fields for a given cube

    :param string cube:
        Name of the cube you want to query
    :param list exclude_fields:
        List (or csv) of fields to exclude from the results
    :param bool mtime:
        Include mtime details
    '''
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
    cmd = os.path.join(owner, cube)
    return self._get(cmd, exclude_fields=exclude_fields, _mtime=_mtime)


def stats(self, cube=None, owner=None):
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
    cmd = os.path.join(owner, cube, 'stats')
    result = self._get(cmd)
    return result


### ADMIN ####

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
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
    cmd = os.path.join(owner, cube, 'drop')
    return self._delete(cmd)


def register(self, cube=None, owner=None):
    '''
    Drops current cube from timeline

    :param string owner: owner of cube
    :param string cube: cube name to use
    '''
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
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
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
    cmd = os.path.join(owner, cube, 'update_role')
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
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
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
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
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
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
    cmd = os.path.join(owner, cube, 'index')
    return self._delete(cmd, drop=index_or_name)


######## SAVE/REMOVE ########
def save_objects(self, objects, batch_size=None, cube=None, owner=None):
    '''
    :param list objects: list of dictionary-like objects to be stored
    :param string owner: owner of cube
    :param string cube: cube name to use
    :param int batch_size: maximum slice of objects to post at a time
    :rtype: list - list of object ids saved

    Save a list of objects the given metrique.cube.

    Returns back a list of object ids (_id|_oid) saved.
    '''
    t1 = time()
    batch_size = set_default(batch_size, self.config.batch_size)
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)

    olen = len(objects) if objects else None
    if not olen:
        self.logger.debug("... No objects to save")
        return []

    # get 'now' utc timezone aware datetime object
    now = pytz.UTC.localize(datetime.utcnow())

    cmd = os.path.join(owner, cube, 'save_objects')

    if olen <= batch_size:
        saved = self._post(cmd, objects=objects, mtime=now)
    else:
        saved = []
        k = 0
        for batch in batch_gen(objects, batch_size):
            _saved = self._post(cmd, objects=batch, mtime=now)
            saved.extend(_saved)
            k += batch_size
            self.logger.info("... %i of %i" % (k, olen))
    slen = len(saved)
    self.logger.info("... Saved %s NEW docs in ~%is" % (slen, time() - t1))
    return saved


def remove_objects(self, ids, backup=False, cube=None, owner=None):
    '''
    Remove objects from cube timeline

    :param list ids: list of object ids to remove
    :param bool backup: return the documents removed to client?
    :param string cube: cube name to use
    '''
    owner = set_default(owner, self.config.api_username)
    cube = set_default(cube, self.name)
    cmd = os.path.join(owner, cube, 'remove_objects')
    if not ids:
        return True
    else:
        return self._delete(cmd, ids=ids, backup=backup)
