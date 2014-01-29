#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.cube_api
~~~~~~~~~~~~~~~~~

This module contains all Cube related api functionality.
'''

from metriqueu.utils import batch_gen


def list_all(self, startswith=None):
    '''
    List all cubes available to the calling client.

    :param startswith: string to use in a simple "startswith" query filter
    :returns list: sorted list of cube names
    '''
    return sorted(self._get(startswith))


def sample_fields(self, cube=None, sample_size=None, query=None, owner=None):
    '''
    List a sample of all valid fields for a given cube.

    Assuming all cube objects have the same exact fields, sampling
    fields should result in a complete list of object fields.

    However, if cube objects have different fields, sampling fields
    might not result in a complete list of object fields, since
    some object variants might not be included in the sample queried.

    :param cube: cube name
    :param sample_size: number of random documents to query
    :param query: `pql` query to filter sample query with
    :param owner: username of cube owner
    :returns list: sorted list of fields
    '''
    cmd = self.get_cmd(owner, cube)
    result = self._get(cmd, sample_size=sample_size,
                       query=query)
    return sorted(result)


def stats(self, cube=None, owner=None, keys=None):
    '''Get server reported statistics and other cube details. Optionally,
    return only the keys specified, not all the stats.

    :param cube: cube name
    :param owner: username of cube owner
    :param keys: return only the specified keys, not all
    '''
    owner = owner or self.config.username
    cmd = self.get_cmd(owner, cube, 'stats')
    result = self._get(cmd)
    if not keys:
        return result
    elif keys and isinstance(keys, basestring):
        return result.get(keys)
    else:
        return [result.get(k) for k in keys]


### ADMIN ####

def drop(self, quiet=False, cube=None, owner=None):
    '''
    Drop (delete) cube.

    :param quiet: ignore exceptions
    :param owner: username of cube owner
    :param cube: cube name
    '''
    cmd = self.get_cmd(owner, cube, 'drop')
    try:
        return self._delete(cmd)
    except Exception:
        if quiet:
            return False
        else:
            raise


def register(self, cube=None, owner=None, quiet=False):
    '''
    Register a new cube.

    :param cube: cube name
    :param owner: username of cube owner
    :param quiet: ignore exceptions
    '''
    cmd = self.get_cmd(owner, cube, 'register')
    try:
        result = self._post(cmd)
    except Exception:
        if quiet:
            result = None
        else:
            raise
    return result


def update_role(self, username, cube=None, action='addToSet',
                role='read', owner=None):
    '''
    Manipulate cube access controls

    :param username: username to manipulate
    :param cube: cube name
    :param action: action to take (addToSet, pull)
    :param role: Permission: read, write, admin)
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'update_role')
    return self._post(cmd, username=username, action=action, role=role)


######### INDEX #########
def list_index(self, cube=None, owner=None):
    '''
    List all cube indexes

    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    result = self._get(cmd)
    return sorted(result)


def ensure_index(self, key_or_list, name=None, background=None,
                 cube=None, owner=None):
    '''
    Build a new index on a cube.

    Examples:
        + ensure_index('field_name')
        + ensure_index([('field_name', 1), ('other_field_name', -1)])

    :param key_or_list: A single field or a list of (key, direction) pairs
    :param name: (optional) Custom name to use for this index
    :param background: MongoDB should create in the background
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    return self._post(cmd, ensure=key_or_list, name=name,
                      background=background)


def drop_index(self, index_or_name, cube=None, owner=None):
    '''
    Drops the specified index on this cube.

    :param index_or_name: index (or name of index) to drop
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    return self._delete(cmd, drop=index_or_name)


######## SAVE/REMOVE ########
def _save_default(self, objects, start_time, owner, cube):
    batch_size = self.config.batch_size
    cmd = self.get_cmd(owner, cube, 'save')
    olen = len(objects) if objects else None
    if (batch_size <= 0) or (olen <= batch_size):
        saved = self._post(cmd, objects=objects, start_time=start_time)
    else:
        saved = []
        k = 0
        for batch in batch_gen(objects, batch_size):
            _saved = self._post(cmd, objects=batch, start_time=start_time)
            saved.extend(_saved)
            k += len(batch)
    return saved


# FIXME: get rid of start_time? we add this by default during normalization
# we don't want server to work unnecessarily hard on this... force
# it to happen client side or metriqued will use the datetime of when
# the request object was
def save(self, objects=None, cube=None, owner=None, start_time=None,
         flush=True):
    '''
    Save a list of objects the given metrique.cube.
    Returns back a list of object ids (_id|_oid) saved.

    :param objects: list of dictionary-like objects to be stored
    :param cube: cube name
    :param owner: username of cube owner
    :param start_time: ISO format datetime to apply as _start
                       per object, serverside
    :param flush: flush objects from memory after save
    :returns None: results are saved in pyclient().result attribute
    '''
    if objects is None:
        objects = self.objects
    if not objects:
        self.logger.info("... No objects to save")
        self.result = []
    else:
        self.logger.info("Saving %s objects" % len(objects))
        # support only list of dicts
        saved = _save_default(self, objects, start_time, owner, cube)
        self.logger.info("... Saved %s NEW docs" % len(saved))
        self.result = saved
        if flush:
            self.flush()
    return


def rename(self, new_name, cube=None, owner=None):
    '''
    Rename a cube.

    :param new_name: new cube name
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'rename')
    result = self._post(cmd, new_name=new_name)
    if result:
        self.name = new_name
    return result


def remove(self, query, date=None, cube=None, owner=None):
    '''
    Remove objects from a cube.

    :param query: `pql` query to filter sample query with
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'remove')
    result = self._delete(cmd, query=query, date=date)
    return result


def export(self, filename, cube=None, owner=None):
    '''
    Export a cube to compressed (gzip) json

    :param filename: path/filename of results export.tgz file
    :param cube: cube name
    :param owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'export')
    return self._save(cmd=cmd, filename=filename)
