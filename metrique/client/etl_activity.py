#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Juraj Niznan" <jniznan@redhat.com>

from copy import deepcopy


def _activity_backwards(val, removed, added):
    if isinstance(added, list) and isinstance(removed, list):
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
    activities = filter(lambda act: (act[0] < time_doc['_start'] and
                                     act[1] in time_doc), activities)
    for when, field, removed, added in activities:
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
            print u'Inconsistency: %s %s: %s -> %s, object has %s' % (
                last_doc['_oid'], field, removed, added, last_val)
            if '_corrupted' not in new_doc:
                new_doc['_corrupted'] = {}
            new_doc['_corrupted'][field] = added
        # Add the objects to the batch
        batch_updates.append(last_doc)
        batch_updates.append(new_doc)
    return batch_updates


def _activity_import(cube, ids):
    if isinstance(ids, list):
        q = '_oid in %s' % ids
    if isinstance(ids, tuple):
        q = '_oid >= %s and _oid <= %s' % ids
    time_docs = cube.find(q, fields='__all__', date='~',
                          sort=[('_oid', 1), ('_start', 1)], raw=True)

    # generator that yields by ids ascending
    # has format: (id, [(when, field, removed, added)])
    act_generator = cube.activity_get(ids)

    last_doc_id = -1
    for time_doc in time_docs:
        _oid = time_doc['_oid']
        # we want to update only the oldest version of the object
        if _oid != last_doc_id:
            last_doc_id = _oid
            _, acts = act_generator.next()
            updates = _activity_import_doc(cube, time_doc, acts)
            if len(updates) > 1:
                cube.save_objects(updates, timeline=True)


def activity_import(self, ids=None, chunk_size=1000):
    '''
    Run the activity import for a given cube, if the
    cube supports it.

    Essentially, recreate object histories from
    a cubes 'activity history' table row data,
    and dump those pre-calcultated historical
    state object copies into the timeline.

    Paremeters
    ----------
    ids : list of cube object ids or str of comma-separated ids
        Specificly run snapshot for this list of object ids
    '''
    if ids is None:
        max_oid = self.find('_oid == exists(True)', date='~',
                            sort=[('_oid', -1)], one=True, raw=True)['_oid']
        ids = (0, max_oid)
    if isinstance(ids, tuple):
        for i in range(ids[0], ids[1] + 1, chunk_size):
            _activity_import(self, (i, min(ids[1], i + chunk_size - 1)))
    else:
        if isinstance(ids, basestring):
            ids = map(int, ids.split(','))
        for i in range(0, len(ids), chunk_size):
            _activity_import(self, ids[i:i + chunk_size])
