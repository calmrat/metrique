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

from copy import deepcopy
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

from metriqueu.utils import batch_gen, set_default, ts2dt, dt2ts, utcnow


def list_all(self, startswith=None):
    ''' List all valid cubes for a given metrique instance '''
    return sorted(self._get(startswith))


def sample_fields(self, cube=None, sample_size=None, query=None, owner=None):
    '''
    List all valid fields for a given cube

    :param list exclude_fields:
        List (or csv) of fields to exclude from the results
    :param bool mtime:
        Include mtime details
    '''
    cmd = self.get_cmd(owner, cube)
    result = self._get(cmd, sample_size=sample_size,
                       query=query)
    return sorted(result)


def stats(self, cube=None, owner=None, keys=None):
    cmd = self.get_cmd(owner, cube, 'stats')
    result = self._get(cmd)
    if not keys:
        return result
    elif keys and isinstance(keys, basestring):
        return result.get(keys)
    else:
        return [result.get(k) for k in keys]


### ADMIN ####

def drop(self, cube=None, force=False, owner=None):
    '''
    Drops current cube from timeline

    :param bool force: really, do it!
    '''
    if not force:
        raise ValueError(
            "DANGEROUS: set false=True to drop %s.%s" % (
                owner, cube))
    cmd = self.get_cmd(owner, cube, 'register')
    return self._delete(cmd)


def register(self, cube=None, owner=None):
    '''
    Register a new user cube
    '''
    cmd = self.get_cmd(owner, cube, 'register')
    return self._post(cmd)


def update_role(self, username, cube=None, action='push',
                role='read', owner=None):
    '''
    Add/Remove cube ACLs

    :param string action: action to take (push, pull)
    :param string role:
        Permission: read, write, admin)
    '''
    cmd = self.get_cmd(owner, cube, 'update_role')
    return self._post(cmd, username=username, action=action, role=role)


######### INDEX #########

def list_index(self, cube=None, owner=None):
    '''
    List indexes for either timeline or warehouse.

    '''
    cmd = self.get_cmd(owner, cube, 'index')
    result = self._get(cmd)
    return sorted(result)


def ensure_index(self, key_or_list, cube=None, owner=None):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    return self._post(cmd, ensure=key_or_list)


def drop_index(self, index_or_name, cube=None, owner=None):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    return self._delete(cmd, drop=index_or_name)


######## SAVE/REMOVE ########

def save(self, objects, cube=None, batch_size=None, owner=None):
    '''
    Save a list of objects the given metrique.cube.
    Returns back a list of object ids (_id|_oid) saved.

    :param list objects: list of dictionary-like objects to be stored
    :param int batch_size: maximum slice of objects to post at a time
    :rtype: list - list of object ids saved
    '''
    batch_size = set_default(batch_size, self.config.batch_size)

    olen = len(objects) if objects else None
    if not olen:
        self.logger.info("... No objects to save")
        return []

    # get 'now' utc timezone aware datetime object
    # FIXME IMPORTANT timestamp should be really taken before extract
    now = utcnow(tz_aware=True)

    cmd = self.get_cmd(owner, cube, 'save')
    if (batch_size <= 0) or (olen <= batch_size):
        saved = self._post(cmd, objects=objects, mtime=now)
    else:
        saved = []
        k = 0
        for batch in batch_gen(objects, batch_size):
            _saved = self._post(cmd, objects=batch, mtime=now)
            saved.extend(_saved)
            k += batch_size
            self.logger.info("... %i of %i" % (k, olen))
    self.logger.info("... Saved %s NEW docs" % len(saved))
    return sorted(saved)


def remove(self, ids, cube=None, backup=False, owner=None):
    '''
    Remove objects from cube timeline

    :param list ids: list of object ids to remove
    :param bool backup: return the documents removed to client?
    '''
    if not ids:
        raise RuntimeError("empty id list")
    else:
        cmd = self.get_cmd(owner, cube, 'remove')
        result = self._delete(cmd, ids=ids, backup=backup)
    return sorted(result)


def _activity_backwards(val, removed, added):
    if isinstance(added, list) and isinstance(removed, list):
        val = [] if val is None else val
        inconsistent = False
        for ad in added:
            if ad in val:
                val.remove(ad)
            else:
                inconsistent = True
        val.extend(removed)
    else:
        inconsistent = val != added
        val = removed
    return val, inconsistent


def _activity_import_doc(cube, time_doc, activities):
    '''
    Import activities for a single document into timeline.
    '''
    batch_updates = [time_doc]
    # We want to consider only activities that happend before time_doc
    # do not move this, because time_doc._start changes
    # time_doc['_start'] is a timestamp, whereas act[0] is a datetime
    td_start = ts2dt(time_doc['_start'])
    activities = filter(lambda act: (act[0] < td_start and
                                     act[1] in time_doc), activities)
    for when, field, removed, added in activities:
        removed = dt2ts(removed) if isinstance(removed, datetime) else removed
        added = dt2ts(added) if isinstance(added, datetime) else added
        last_doc = batch_updates.pop()
        # check if this activity happened at the same time as the last one,
        # if it did then we need to group them together
        if last_doc['_end'] == when:
            new_doc = last_doc
            last_doc = batch_updates.pop()
        else:
            try:
                # set start to creation time if available
                creation_field = cube.get_property('cfield')
                start = last_doc[creation_field]
            except:
                start = when
            new_doc = deepcopy(last_doc)
            new_doc.pop('_id') if '_id' in new_doc else None
            new_doc['_start'] = start
            new_doc['_end'] = when
            last_doc['_start'] = when
        last_val = last_doc[field]
        new_val, inconsistent = _activity_backwards(new_doc[field],
                                                    removed, added)
        new_doc[field] = new_val
        # Check if the object has the correct field value.
        if inconsistent:
            msg = 'Inconsistency: %s %s: %s -> %s, object has %s' % (
                last_doc['_oid'], field, removed, added, last_val)
            logger.debug(msg)
            msg = '        Types: %s -> %s, object has %s.' % (
                type(removed), type(added), type(last_val))
            logger.debug(msg)
            if '_corrupted' not in new_doc:
                new_doc['_corrupted'] = {}
            new_doc['_corrupted'][field] = added
        # Add the objects to the batch
        batch_updates.append(last_doc)
        batch_updates.append(new_doc)
    return batch_updates


def _activity_import(cube, oids):
    # get time docs cursor
    if isinstance(oids, list):
        q = '_oid in %s' % oids
    if isinstance(oids, tuple):
        q = '_oid >= %s and _oid <= %s' % oids
    time_docs = cube.find(q, fields='__all__', date='~',
                          sort=[('_oid', 1), ('_start', 1)], raw=True)

    # generator that yields by ids ascending
    # has format: (id, [(when, field, removed, added)])
    act_generator = cube.activity_get(oids)

    last_doc_id = -1
    aid = -1
    remove_ids = []
    save_objects = []
    for time_doc in time_docs:
        _oid = time_doc['_oid']
        _id = time_doc.pop('_id')
        time_doc.pop('_hash')
        # we want to update only the oldest version of the object
        while aid < _oid:
            aid, acts = act_generator.next()
        if _oid != last_doc_id and aid == _oid:
            last_doc_id = _oid
            updates = _activity_import_doc(cube, time_doc, acts)
            if len(updates) > 1:
                save_objects += updates
                remove_ids.append(_id)
    cube.cube_remove(ids=remove_ids)
    cube.cube_save(save_objects)


def activity_import(self, ids=None, chunk_size=1000):
    '''
    WARNING: Do NOT run extract while activity import is running,
             it might result in data corruption.
    Run the activity import for a given cube, if the cube supports it.

    Essentially, recreate object histories from
    a cubes 'activity history' table row data,
    and dump those pre-calcultated historical
    state object copies into the timeline.

    :param object ids:
        - None: import for all ids
        - list of ids: import for ids in the list
        - csv list of ids:  import for ids in the csv list
        - 2-tuple of ids: import for the ids in the interval
          specified by the tuple
    :param int save_batch_size:
        Determines the size of the batch when sending objects to save to the
        Metrique server
    :param int chunk_size:
        Size of the chunks into which the ids are split, activity import is
        done and saved separately for each batch
    '''
    if ids is None:
        max_oid = self.find('_oid == exists(True)', date='~',
                            sort=[('_oid', -1)], one=True, raw=True)['_oid']
        ids = (0, max_oid)
    if isinstance(ids, tuple):
        for i in range(ids[0], ids[1] + 1, chunk_size):
            _activity_import(self, (i, min(ids[1], i + chunk_size - 1)))
    else:
        if not isinstance(ids, list):
            raise ValueError(
                "Expected ids to be None, tuple or list. Got %s" % type(list))

        for i in range(0, len(ids), chunk_size):
            _activity_import(self, ids[i:i + chunk_size])
