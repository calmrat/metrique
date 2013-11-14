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

import codecs
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime
import os
import simplejson as json
import traceback

from metrique.utils import json_encode
from metriqueu.utils import batch_gen, ts2dt, dt2ts, utcnow


def list_all(self, startswith=None):
    '''
    This is expected to list all cubes available to
    the calling client.

    :param string startswith: simple "startswith" filter string
    :returns list: sorted list of cube names
    '''
    return sorted(self._get(startswith))


def sample_fields(self, cube=None, sample_size=None, query=None, owner=None):
    '''
    List all valid fields for a given cube

    :param int sample_size: number of random documents to query
    :param list exclude_fields:
        List (or csv) of fields to exclude from the results
    :param bool mtime:
        Include mtime details
    :returns list: sorted list of fields
    '''
    cmd = self.get_cmd(owner, cube)
    result = self._get(cmd, sample_size=sample_size,
                       query=query)
    return sorted(result)


def stats(self, cube, owner=None, keys=None):
    '''
    Get back server reported statistics and other data
    about a cube cube; optionally, return only the
    keys specified, not all the stats.
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
    Drops current cube from timeline

    :param string cube: cube name
    :param bool force: really, do it!
    :param string owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'drop')
    try:
        return self._delete(cmd)
    except Exception:
        if quiet:
            return False
        else:
            raise


def register(self, cube=None, owner=None):
    '''
    Register a new user cube

    :param string cube: cube name
    :param string owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'register')
    return self._post(cmd)


def update_role(self, username, cube=None, action='addToSet',
                role='read', owner=None):
    '''
    Add/Remove cube ACLs

    :param string action: action to take (addToSet, pull)
    :param string role:
        Permission: read, write, admin)
    '''
    cmd = self.get_cmd(owner, cube, 'update_role')
    return self._post(cmd, username=username, action=action, role=role)


######### INDEX #########

def list_index(self, cube=None, owner=None):
    '''
    List indexes for either timeline or warehouse.

    :param string cube: cube name
    :param string owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    result = self._get(cmd)
    return sorted(result)


def ensure_index(self, key_or_list, name=None, background=None,
                 cube=None, owner=None):
    '''
    Ensures that an index exists on this cube.

    :param string/list key_or_list:
        Either a single key or a list of (key, direction) pairs.
    :param string name:
        Custom name to use for this index.
        If none is given, a name will be generated.
    :param bool background:
        If this index should be created in the background.
    :param string cube: cube name
    :param string owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    return self._post(cmd, ensure=key_or_list, name=name,
                      background=background)


def drop_index(self, index_or_name, cube=None, owner=None):
    '''
    Drops the specified index on this cube.

    :param string/list index_or_name:
        index (or name of index) to drop
    :param string cube: cube name
    :param string owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'index')
    return self._delete(cmd, drop=index_or_name)


######## SAVE/REMOVE ########

def save(self, objects, cube=None, owner=None, journal=None):
    '''
    Save a list of objects the given metrique.cube.
    Returns back a list of object ids (_id|_oid) saved.

    :param list objects: list of dictionary-like objects to be stored
    :param string cube: cube name
    :param string owner: username of cube owner
    :rtype: list - list of object ids saved
    '''
    batch_size = self.config.batch_size

    olen = len(objects) if objects else None
    if not olen:
        self.logger.info("... No objects to save")
        return []
    else:
        self.logger.info("Saving %s objects" % len(objects))

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
            k += len(batch)
            self.logger.info("... %i of %i posted" % (k, olen))

    journal = journal or self.config.journal
    if saved and self.config.journal:
        objects = [o for o in objects if o['_oid'] in saved]
        cube = '%s__%s' % (self.owner, self.name)
        journaldir = os.path.join(self.config.journaldir, cube)
        if not os.path.exists(journaldir):
            os.makedirs(os.path.expanduser(journaldir))
        _path = '%s.json' % now.date().isoformat()
        path = os.path.join(journaldir, _path)
        exists = os.path.exists(path)
        with codecs.open(path, 'a', 'utf-8') as journal:
            if exists:
                journal.write('\n')
            json.dump(objects, journal, default=json_encode,
                      ensure_ascii=False, indent=2)
    self.logger.info("... Saved %s NEW docs" % len(saved))
    return sorted(saved)


def remove(self, query, cube=None, owner=None):
    '''
    Remove objects from cube timeline

    :param list ids: list of object ids to remove
    :param bool backup: return the documents removed to client?
    :param string cube: cube name
    :param string owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'remove')
    result = self._delete(cmd, query=query)
    return sorted(result)


def export(self, filename, cube=None, owner=None):
    '''
    Export the entire cube to compressed (gzip) json

    :param string cube: cube name
    :param string owner: username of cube owner
    '''
    cmd = self.get_cmd(owner, cube, 'export')
    return self._save(cmd=cmd, filename=filename)


######## ACTIVITY IMPORT ########
def activity_import(self, oids=None, cube=None, owner=None,
                    parallel=True):
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
    '''
    if oids is None:
        oids = self.distinct('_oid', cube=cube, owner=owner)

    max_workers = self.config.max_workers
    batch_size = self.config.batch_size

    saved = []
    if parallel:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_activity_import, self, oids=batch,
                                 cube=cube, owner=owner)
                       for batch in batch_gen(oids, batch_size)]

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                tb = traceback.format_exc()
                self.logger.error('Activity Import Error: %s\n%s' % (e, tb))
                del tb
            else:
                saved.extend(result)
                self.logger.info(
                    '%i of %i extracted' % (len(saved),
                                            len(oids)))
    else:
        for batch in batch_gen(oids, batch_size):
            result = _activity_import(self, oids=batch, cube=cube, owner=owner)
            saved.extend(result)

    failed = set(oids) - set(saved)
    result = {'saved': sorted(saved), 'failed': sorted(failed)}
    return result


def _activity_import(self, oids, cube, owner):
    # get time docs cursor
    time_docs = self.find('_oid in %s' % oids, fields='~', date='~',
                          raw=True, merge_versions=False,
                          cube=cube, owner=owner)
    docs = {}
    for doc in time_docs:
        oid = doc['_oid']
        # we want to update only the oldest version of the object
        if oid not in docs or docs[oid]['_start'] > doc['_start']:
            docs[oid] = doc

    # dict, has format: oid: [(when, field, removed, added)]
    activities = self.activity_get(oids)

    remove_ids = []
    save_objects = []
    self.logger.debug('Processing activity history')

    for time_doc in docs.values():
        _oid = time_doc['_oid']
        _id = time_doc.pop('_id')
        time_doc.pop('_hash')
        acts = activities.setdefault(_oid, [])
        updates = _activity_import_doc(self, time_doc, acts)
        if updates:
            save_objects += updates
            remove_ids.append(_id)

    self.cube_remove('_id in %s' % remove_ids)
    self.cube_save(save_objects)
    return oids


def _activity_import_doc(self, time_doc, activities):
    '''
    Import activities for a single document into timeline.
    '''
    batch_updates = [time_doc]
    # We want to consider only activities that happend before time_doc
    # do not move this, because time_doc._start changes
    # time_doc['_start'] is a timestamp, whereas act[0] is a datetime
    td_start = time_doc['_start'] = ts2dt(time_doc['_start'], tz_aware=True)
    activities = filter(lambda act: (act[0] < td_start and
                                     act[1] in time_doc), activities)
    # make sure that activities are sorted by when descending
    activities.sort(reverse=True)
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
            new_doc = deepcopy(last_doc)
            new_doc.pop('_id') if '_id' in new_doc else None
            new_doc['_start'] = when
            new_doc['_end'] = when
            last_doc['_start'] = when
        last_val = last_doc[field]
        new_val, inconsistent = _activity_backwards(new_doc[field],
                                                    removed, added)
        new_doc[field] = new_val
        # Check if the object has the correct field value.
        if inconsistent:
            incon = {'oid': last_doc['_oid'],
                     'field': field,
                     'removed': removed,
                     'removed_type': str(type(removed)),
                     'added': added,
                     'added_type': str(type(added)),
                     'last_val': last_val,
                     'last_val_type': str(type(last_val)),
                     'when': str(when)}
            if self.config.get('incon_log_type') == 'pretty':
                msg = '{oid} {field}: {removed} -> {added}, has {last_val}; '
                msg += '({removed_type} -> {added_type}, has {last_val_type})'
                msg += ' ... {when}'
                self.logger.error(msg.format(**incon))
            else:
                self.logger.error(json.dumps(incon, ensure_ascii=False))
            if '_corrupted' not in new_doc:
                new_doc['_corrupted'] = {}
            new_doc['_corrupted'][field] = added
        # Add the objects to the batch
        batch_updates.append(last_doc)
        batch_updates.append(new_doc)
    # try to set the _start of the first version to the creation time
    try:
        # set start to creation time if available
        last_doc = batch_updates[-1]
        creation_field = self.get_property('cfield')
        if creation_field:
            creation_ts = ts2dt(last_doc[creation_field])
            if creation_ts < last_doc['_start']:
                last_doc['_start'] = creation_ts
            elif len(batch_updates) == 1:
                # we have only one version, that we did not change
                return []
    except Exception as e:
        self.logger.error('Error updating creation time; %s' % e)
    return batch_updates


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
