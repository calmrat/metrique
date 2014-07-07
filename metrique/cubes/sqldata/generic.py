#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.cubes.sqldata.generic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the cube methods for extracting
data from generic SQL data sources.
'''

from __future__ import unicode_literals, absolute_import

import logging
logger = logging.getLogger('metrique')

from copy import copy, deepcopy
from collections import defaultdict
from dateutil.parser import parse as dt_parse
import os
import pytz
import re

try:
    from joblib import Parallel, delayed
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False
    logger.warn("joblib package not found!")

import warnings

from metrique import pyclient
from metrique.utils import batch_gen, configure, debug_setup, ts2dt
from metrique.utils import dt2ts, is_array


# FIXME: pass in workers=1 and call standard *get* func rather than
# private _*get*?
def get_full_history(cube, oids, flush=False, cube_name=None,
                     config=None, config_file=None, config_key=None,
                     container=None, container_config=None,
                     proxy=None, proxy_config=None, **kwargs):
    m = pyclient(cube=cube, name=cube_name, config=config,
                 config_file=config_file, config_key=config_key,
                 container=container, container_config=container_config,
                 proxy=proxy, proxy_config=proxy_config, **kwargs)
    results = []
    batch_size = m.lconfig.get('batch_size')
    for batch in batch_gen(oids, batch_size):
        _ = m._activity_get_objects(oids=batch, flush=flush)
        results.extend(_)
    return results


def get_objects(cube, oids, flush=False, cube_name=None,
                config=None, config_file=None, config_key=None,
                container=None, container_config=None,
                proxy=None, proxy_config=None, **kwargs):
    m = pyclient(cube=cube, name=cube_name, config=config,
                 config_file=config_file, config_key=config_key,
                 container=container, container_config=container_config,
                 proxy=proxy, proxy_config=proxy_config, **kwargs)
    results = []
    batch_size = m.lconfig.get('batch_size')
    for batch in batch_gen(oids, batch_size):
        _ = m._get_objects(oids=batch, flush=flush)
        results.extend(_)
    return results


class Generic(pyclient):
    '''
    Base, common functionality driver for connecting
    and extracting data from SQL databases.

    It is expected that specific database connectors will
    subclass this basecube to implement db specific connection
    methods, etc.

    :cvar fields: cube fields definitions
    :cvar defaults: cube default property container (cube specific meta-data)

    :param sql_host: teiid hostname
    :param sql_port: teiid port
    :param sql_username: sql username
    :param sql_password: sql password
    :param sql_retries: number of times before we give up on a query
    :param sql_batch_size: how many objects to query at a time
    '''
    dialect = None
    fields = None

    def __init__(self, retries=None, batch_size=None, worker_batch_size=None,
                 **kwargs):
        self.fields = self.fields or {}
        super(Generic, self).__init__(**kwargs)
        options = dict(retries=retries,
                       batch_size=batch_size,
                       worker_batch_size=worker_batch_size)
        defaults = dict(retries=3,
                        batch_size=999,
                        worker_batch_size=5000)
        self.config = configure(options, defaults,
                                config_file=self.config_file,
                                section_key=self.config_key,
                                update=self.config)

        self.retry_on_error = (Exception, )
        self._setup_inconsistency_log()

    def activity_get(self, ids=None):
        '''
        Returns a dictionary of `id: [(when, field, removed, added)]`
        key:value pairs that represent the activity history for
        the particular ids.

        This is used when originating data source has some form of
        a 'change log' table that tracks changes to individual
        object.fields.

        The data returned by this method is used to rebuild historical
        object states.
        '''
        raise NotImplementedError(
            'The activity_get method is not implemented in this cube.')

    def _activity_get_objects(self, oids, flush=False):
        logger.debug('Getting Objects - Activity History')
        self._get_objects(oids=oids, flush=False)
        objects = self.objects.values()
        # dict, has format: oid: [(when, field, removed, added)]
        activities = self.activity_get(oids)
        _objs = []
        for doc in objects:
            _oid = doc['_oid']
            acts = activities.setdefault(_oid, [])  # no activity default
            objs = self._activity_import_doc(doc, acts)
            _objs.extend(objs)

        self.objects.extend(_objs)
        if flush:
            return self.objects.flush(autosnap=False)
        else:
            return self.objects.values()

    def _activity_import_doc(self, time_doc, activities):
        '''
        Import activities for a single document into timeline.
        '''
        batch_updates = [time_doc]
        # We want to consider only activities that happend before time_doc
        # do not move this, because time_doc._start changes
        # time_doc['_start'] is a timestamp, whereas act[0] is a datetime
        # we need to be sure to convert act[0] (when) to timestamp!
        td_start = time_doc['_start']
        activities = filter(lambda act: (act[0] < td_start and
                                         act[1] in time_doc), activities)
        creation_field = self.lconfig.get('cfield')
        # make sure that activities are sorted by when descending
        activities.sort(reverse=True, key=lambda o: o[0])
        new_doc = {}
        for when, field, removed, added in activities:
            last_doc = batch_updates.pop()
            # check if this activity happened at the same time as the last one,
            # if it did then we need to group them together
            if last_doc['_end'] == when:
                new_doc = deepcopy(last_doc)
                last_doc = batch_updates.pop()
            else:
                new_doc = deepcopy(last_doc)
                new_doc['_start'] = when
                new_doc['_end'] = when
                last_doc['_start'] = when
            last_val = last_doc[field]

            new_val, inconsistent = self._activity_backwards(new_doc[field],
                                                             removed, added)
            new_doc[field] = new_val
            # Check if the object has the correct field value.
            if inconsistent:
                self._log_inconsistency(last_doc, last_val, field,
                                        removed, added, when)
                new_doc['_e'] = {} if not new_doc.get('_e') else new_doc['_e']
                # set curreupted field value to the the value that was added
                # and continue processing as if that issue didn't exist
                new_doc['_e'][field] = added
            # Add the objects to the batch
            batch_updates.extend([last_doc, new_doc])
        # try to set the _start of the first version to the creation time
        try:
            # set start to creation time if available
            last_doc = batch_updates[-1]
            if creation_field:
                # again, we expect _start to be epoch float...
                creation_ts = dt2ts(last_doc[creation_field])
                if creation_ts < last_doc['_start']:
                    last_doc['_start'] = creation_ts
                elif len(batch_updates) == 1:
                    # we have only one version, that we did not change
                    return []
                else:
                    pass  # leave as-is
        except Exception as e:
            logger.error('Error updating creation time; %s' % e)
        batch_updates = self._prep_objects(batch_updates)
        return batch_updates

    def _activity_backwards(self, val, removed, added):
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

    def _delta_force(self, force=None, last_update=None, parse_timestamp=None):
        force = force or self.lconfig.get('force') or False
        oids = []
        _c = self.container
        cube_does_not_exist = not (hasattr(_c, '_exists') and _c._exists)
        if is_array(force):
            oids = list(force)
        elif not force:
            if self.lconfig.get('delta_new_ids', True):
                # get all new (unknown) oids
                new_oids = self.get_new_oids()
                oids.extend(new_oids)
            if self.lconfig.get('delta_mtime', False):
                last_update = self._fetch_mtime(last_update, parse_timestamp)
                # get only those oids that have changed since last update
                oids.extend(self.get_changed_oids(last_update,
                                                  parse_timestamp))
        elif force is True or cube_does_not_exist:
            # if force or if the container doesn't exist
            # get a list of all known object ids
            oids = self.sql_get_oids()
        else:
            oids = [force]
        logger.debug("Delta Size: %s" % len(oids))
        return sorted(set(oids))

    def get_changed_oids(self, last_update=None, parse_timestamp=None):
        '''
        Returns a list of object ids of those objects that have changed since
        `mtime`. This method expects that the changed objects can be
        determined based on the `delta_mtime` property of the cube which
        specifies the field name that carries the time of the last change.

        This method is expected to be overriden in the cube if it is not
        possible to use a single field to determine the time of the change and
        if another approach of determining the oids is available. In such
        cubes the `delta_mtime` property is expected to be set to `True`.

        If `delta_mtime` evaluates to False then this method is not expected
        to be used.

        :param mtime: datetime string used as 'change since date'
        '''
        mtime_columns = self.lconfig.get('delta_mtime', [])
        if not (mtime_columns and last_update):
            return []
        if isinstance(mtime_columns, basestring):
            mtime_columns = [mtime_columns]
        where = []
        for _column in mtime_columns:
            _sql = "%s > %s" % (_column, last_update)
            where.append(_sql)
        return self.sql_get_oids(where)

    def _fetch_mtime(self, last_update=None, parse_timestamp=None):
        mtime = None
        if last_update:
            if isinstance(last_update, basestring):
                mtime = dt_parse(last_update)
            else:
                mtime = last_update
        else:
            mtime = self.container.get_last_field(field='_start')

        mtime = ts2dt(mtime)
        logger.debug("Last update mtime: %s" % mtime)

        if mtime:
            if parse_timestamp is None:
                parse_timestamp = self.lconfig.get('parse_timestamp', True)
            if parse_timestamp:
                if not (hasattr(mtime, 'tzinfo') and mtime.tzinfo):
                    # We need the timezone, to readjust relative to the
                    # server's tz
                    mtime = mtime.replace(tzinfo=pytz.utc)
                mtime = mtime.strftime('%Y-%m-%d %H:%M:%S %z')
                dt_format = "yyyy-MM-dd HH:mm:ss z"
                mtime = "parseTimestamp('%s', '%s')" % (mtime, dt_format)
            else:
                mtime = "'%s'" % mtime
        return mtime

    @property
    def fieldmap(self):
        '''
        Dictionary of field_id: field_name, as defined in self.fields property
        '''
        if hasattr(self, '_sql_fieldmap') and self._sql_fieldmap:
            fieldmap = self._sql_fieldmap
        else:
            fieldmap = defaultdict(str)
            fields = copy(self.fields)
            for field, opts in fields.iteritems():
                field_id = opts.get('what')
                if field_id is not None:
                    fieldmap[field_id] = field
            self._sql_fieldmap = fieldmap
        return fieldmap

    def _generate_sql(self, _oids=None, sort=True):
        db = self.lconfig.get('db_schema_name') or self.lconfig.get('db')
        _oid = self.lconfig.get('_oid')
        if is_array(_oid):
            _oid = _oid[0]  # get the db column, not the field alias
        table = self.lconfig.get('table')

        if not all((_oid, table, db)):
            raise ValueError("Must define db, table, _oid in config!")
        selects = []
        stmts = []
        for as_field, opts in self.fields.iteritems():
            select = opts.get('select')
            if not select:
                # not a SQL based field
                continue
            select = '%s as %s' % (select, as_field)
            selects.append(select)
            sql = opts.get('sql') or ''
            sql = re.sub('\s+', ' ', sql)
            if sql:
                stmts.append(sql)

        selects = ', '.join(selects)
        stmts = ' '.join(stmts)
        sql = 'SELECT %s FROM %s.%s %s' % (selects, db, table, stmts)
        if _oids:
            sql += ' WHERE %s.%s in (%s)' % (table, _oid,
                                             ','.join(map(str, _oids)))
        if sort:
            sql += " ORDER BY %s.%s ASC" % (table, _oid)
        sql = re.sub('\s+', ' ', sql)
        return sql

    def get_objects(self, force=None, last_update=None, parse_timestamp=None,
                    flush=False):
        '''
        Extract routine for SQL based cubes.

        :param force:
            for querying for all objects (True) or only those passed in as list
        :param last_update: manual override for 'changed since date'
        :param parse_timestamp: flag to convert timestamp timezones in-line
        '''
        workers = self.lconfig.get('workers')
        # if we're using multiple workers, break the oids
        # according to worker batchsize, then each worker will
        # break the batch into smaller sql batch size batches
        # otherwise, single threaded, use sql batch size
        w_batch_size = self.lconfig.get('worker_batch_size')
        s_batch_size = self.lconfig.get('batch_size')
        # set the 'index' of sql columns so we can extract
        # out the sql rows and know which column : field
        # determine which oids will we query
        oids = self._delta_force(force, last_update, parse_timestamp)

        if HAS_JOBLIB and workers > 1:
            logger.debug(
                'Getting Objects [parallel]- Current Values (%s@%s)' % (
                    workers, w_batch_size))
            runner = Parallel(n_jobs=workers)
            func = delayed(get_objects)
            with warnings.catch_warnings():
                # suppress warning from joblib:
                # UserWarning: Parallel loops cannot be nested ...
                warnings.simplefilter("ignore")
                result = runner(func(
                    cube=self._cube, oids=batch, flush=flush,
                    cube_name=self.name, config=self.config,
                    config_file=self.config_file,
                    config_key=self.config_key,
                    container=type(self.objects),
                    container_config=self.container_config,
                    proxy=type(self.proxy),
                    proxy_config=self.proxy_config)
                    for batch in batch_gen(oids, w_batch_size))
            # merge list of lists (batched) into single list
            result = [i for l in result for i in l]
        else:
            logger.debug(
                'Getting Objects - Current Values (%s@%s)' % (
                    workers, s_batch_size))
            result = []
            _s = 0
            for i, batch in enumerate(batch_gen(oids, s_batch_size)):
                _e = _s + s_batch_size
                logger.debug('batch %s: %s-%s or %s' % (i, _s, _e, len(oids)))
                _ = self._get_objects(oids=batch, flush=flush)
                result.extend(_)
                _s = _e

        if flush:
            return result
        else:
            return self

    def _get_objects(self, oids, flush=False):
        sql = self._generate_sql(oids)
        objects = self._load_sql(sql)
        # set _oid
        objects = self._prep_objects(objects)
        self.objects.extend(objects)
        if flush:
            return self.objects.flush(autosnap=True)
        else:
            return self.objects.values()

    def get_new_oids(self):
        '''
        Returns a list of unique oids that have not been extracted yet.

        Essentially, a diff of distinct oids in the source database
        compared to cube.
        '''
        table = self.lconfig.get('table')
        _oid = self.lconfig.get('_oid')
        if is_array(_oid):
            _oid = _oid[0]  # get the db column, not the field alias
        last_id = self.container.get_last_field(field='_oid')
        ids = []
        if last_id:
            try:  # try to convert to integer... if not, assume unicode value
                last_id = float(last_id)
                where = "%s.%s > %s" % (table, _oid, last_id)
            except (TypeError, ValueError):
                where = "%s.%s > '%s'" % (table, _oid, last_id)
            ids = self.sql_get_oids(where)
        return ids

    def get_full_history(self, force=None, last_update=None,
                         parse_timestamp=None, flush=False):
        '''
        Fields change depending on when you run activity_import,
        such as "last_updated" type fields which don't have activity
        being tracked, which means we'll always end up with different
        hash values, so we need to always remove all existing object
        states and import fresh
        '''
        workers = self.lconfig.get('workers')
        w_batch_size = self.lconfig.get('worker_batch_size')
        s_batch_size = self.lconfig.get('batch_size')
        # determine which oids will we query
        oids = self._delta_force(force, last_update, parse_timestamp)
        if HAS_JOBLIB and workers > 1:
            logger.debug(
                'Getting Full History (%s@%s)' % (
                    workers, w_batch_size))
            runner = Parallel(n_jobs=workers)
            func = delayed(get_full_history)
            with warnings.catch_warnings():
                # suppress warning from joblib:
                # UserWarning: Parallel loops cannot be nested ...
                warnings.simplefilter("ignore")
                result = runner(func(
                    cube=self._cube, oids=batch, flush=flush,
                    cube_name=self.name, config=self.config,
                    config_file=self.config_file,
                    config_key=self.config_key,
                    container=type(self.objects),
                    container_config=self.container_config,
                    proxy=type(self.proxy),
                    proxy_config=self.proxy_config)
                    for batch in batch_gen(oids, w_batch_size))
            # merge list of lists (batched) into single list
            result = [i for l in result for i in l]
        else:
            logger.debug(
                'Getting Full History (%s@%s)' % (
                    workers, s_batch_size))
            result = []
            _s = 0
            for i, batch in enumerate(batch_gen(oids, s_batch_size)):
                _e = _s + s_batch_size
                logger.debug('batch %s: %s-%s or %s' % (i, _s, _e, len(oids)))
                _ = self._activity_get_objects(oids=batch, flush=flush)
                result.extend(_)
                _s = _e

        if flush:
            return result
        else:
            return self

    def _left_join(self, select_as, select_prop, join_prop, join_table,
                   on_col, on_db=None, on_table=None, join_db=None, **kwargs):
        on_table = on_table or self.lconfig.get('table')
        on_db = (on_db or self.lconfig.get('db_schema_name') or
                 self.lconfig.get('db'))
        join_db = (join_db or self.lconfig.get('db_schema_name') or
                   self.lconfig.get('db'))
        return dict(select='%s.%s' % (select_as, select_prop),
                    sql='LEFT JOIN %s.%s %s ON %s.%s = %s.%s.%s' % (
                        join_db, join_table, select_as, select_as, join_prop,
                        on_db, on_table, on_col),
                    **kwargs)

    def _log_inconsistency(self, last_doc, last_val, field, removed, added,
                           when):
        incon = {'oid': last_doc['_oid'],
                 'field': field,
                 'removed': removed,
                 'removed_type': str(type(removed)),
                 'added': added,
                 'added_type': str(type(added)),
                 'last_val': last_val,
                 'last_val_type': str(type(last_val)),
                 'when': str(when)}
        m = u'{oid} {field}: {removed}-> {added} has {last_val}; '
        m += u'({removed_type}-> {added_type} has {last_val_type})'
        m += u' ... on {when}'
        msg = m.format(**incon)
        self.log_inconsistency(msg)

    def _load_sql(self, sql, retries=None):
        retries = retries or self.lconfig.get('retries')
        return self.proxy._load_sql(sql, retries=retries)

    def _prep_objects(self, objects):
        _oid = self.lconfig.get('_oid')
        if is_array(_oid):
            _oid = _oid[1]  # get the field name, not the actual db column
        for i, obj in enumerate(objects):
            obj['_oid'] = obj[_oid]  # map _oid
            objects[i] = obj
        return objects

    def _setup_inconsistency_log(self):
        _log_file = self.lconfig.get('log_file').split('.log')[0]
        basename = _log_file + '.inconsistencies'
        log_file = basename + '.log'
        log_dir = self.lconfig.get('log_dir')
        log_file = os.path.join(log_dir, log_file)

        log_format = "%(message)s"
        level = logging.ERROR
        logger = debug_setup(logger='incon', level=level, log2stdout=False,
                             log_format=log_format, log2file=True,
                             log_dir=log_dir, log_file=log_file)
        self.log_inconsistency = logger.error

    def sql_get_oids(self, where=None):
        '''
        Query source database for a distinct list of oids.
        '''
        table = self.lconfig.get('table')
        db = self.lconfig.get('db_schema_name') or self.lconfig.get('db')
        _oid = self.lconfig.get('_oid')
        if is_array(_oid):
            _oid = _oid[0]  # get the db column, not the field alias
        sql = 'SELECT DISTINCT %s.%s FROM %s.%s' % (table, _oid, db, table)
        if where:
            where = [where] if isinstance(where, basestring) else list(where)
            sql += ' WHERE %s' % ' OR '.join(where)
        result = sorted([r[_oid] for r in self._load_sql(sql)])
        return result
