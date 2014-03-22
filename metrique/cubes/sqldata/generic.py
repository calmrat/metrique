#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
metrique.cubes.sqldata.generic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the cube methods for extracting
data from generic SQL data sources.
'''

import logging
logger = logging.getLogger(__name__)

from copy import deepcopy, copy
from collections import defaultdict
from dateutil.parser import parse as dt_parse
from functools import partial

try:
    from joblib import Parallel, delayed
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False
    logger.warn("joblib package not found!")

import pytz
import re
import simplejson as json
import time
import traceback

from metrique import pyclient
from metrique.utils import batch_gen, ts2dt, dt2ts, utcnow

# FIXME: why not utf-8?
DEFAULT_ENCODING = 'latin-1'


def get_full_history(cube, oids, flush=False, cube_name=None, autosnap=False,
                     **kwargs):
    m = pyclient(cube=cube, name=cube_name, **kwargs)
    return m._activity_get_objects(oids=oids, flush=flush, autosnap=autosnap)


def get_objects(cube, oids, field_order, flush=False, cube_name=None,
                autosnap=True, **kwargs):
    m = pyclient(cube=cube, name=cube_name, **kwargs)
    return m._get_objects(oids=oids, field_order=field_order,
                          flush=flush, autosnap=autosnap)


class Generic(pyclient):
    '''
    Base, common functionality driver for connecting
    and extracting data from SQL databases.

    It is expected that specific database connectors will
    subclass this basecube to implement db specific connection
    methods, etc.

    .sql_proxy must be defined, in order to know how
    to get a connection object to the target sql db.

    :param sql_host: teiid hostname
    :param sql_port: teiid port
    '''
    def __init__(self, sql_host=None, sql_port=None, **kwargs):
        super(Generic, self).__init__(**kwargs)
        if sql_host:
            self.config['sql_host'] = sql_host
        if sql_port:
            self.config['sql_port'] = sql_port
        self.retry_on_error = None

        self._setup_inconsistency_log()
        self._auto_reconnect_attempted = False

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

    def _activity_get_objects(self, oids, flush=False, autosnap=False):
        logger.debug('Getting Objects - Activity History')
        self.get_objects(force=oids, flush=False)
        # dict, has format: oid: [(when, field, removed, added)]
        activities = self.activity_get(oids)
        objects = self.objects.values()
        for doc in objects:
            _oid = doc['_oid']
            acts = activities.setdefault(_oid, [])
            objs = self._activity_import_doc(doc, acts)
            self.objects.extend(objs)
        logger.debug('... activity get - done')
        if flush:
            return self.flush(autosnap=autosnap)
        else:
            return self.objects.values()

    def _activity_import_doc(self, time_doc, activities):
        '''
        Import activities for a single document into timeline.
        '''
        batch_updates = [time_doc]
        # compare tz aware/naive depending if acts 'when' is tz_aware or not
        tz_aware = True if activities and activities[0][0].tzinfo else False
        # We want to consider only activities that happend before time_doc
        # do not move this, because time_doc._start changes
        # time_doc['_start'] is a timestamp, whereas act[0] is a datetime
        td_start = ts2dt(time_doc['_start'], tz_aware=tz_aware)
        activities = filter(lambda act: (act[0] < td_start and
                                         act[1] in time_doc), activities)
        incon_log_type = self.config.get('incon_log_type')
        creation_field = self.get_property('cfield')
        # make sure that activities are sorted by when descending
        activities.sort(reverse=True, key=lambda o: o[0])
        new_doc = {}
        for when, field, removed, added in activities:
            when = dt2ts(when)
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

            # FIXME: pass in field and call _type() within _activity_backwards?
            # for added/removed?
            new_val, inconsistent = self._activity_backwards(new_doc[field],
                                                             removed, added)
            new_doc[field] = new_val

            # Check if the object has the correct field value.
            if inconsistent:
                self._log_inconsistency(last_doc, last_val, field,
                                        removed, added, when, incon_log_type)
                new_doc.setdefault('_corrupted', {})
                # set curreupted field value to the the value that was added
                # and continue processing as if that issue didn't exist
                new_doc['_corrupted'][field] = added
            # Add the objects to the batch
            batch_updates.extend([last_doc, new_doc])
        # try to set the _start of the first version to the creation time
        try:
            # set start to creation time if available
            last_doc = batch_updates[-1]
            if creation_field:
                creation_ts = dt2ts(last_doc[creation_field])
                if creation_ts < last_doc['_start']:
                    last_doc['_start'] = creation_ts
                elif len(batch_updates) == 1:
                    # we have only one version, that we did not change
                    return []
        except Exception as e:
            logger.error('Error updating creation time; %s' % e)
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

    def _build_rows(self, rows):
        logger.debug('Building dict_rows from sql_rows(%i)' % len(rows))
        _rows = defaultdict(list)
        for row in rows:
            _rows[row['_oid']].append(row)
        return _rows

    def _build_objects(self, rows):
        '''
        Given a set of rows/columns, build metrique object dictionaries
        Normalize null values to be type(None).
        '''
        objects = []
        logger.debug('Building objects from rows(%i)' % len(rows))
        for col_rows in rows.itervalues():
            if len(col_rows) > 1:
                obj = self._normalize_object(col_rows)
                objects.append(obj)
            else:
                objects.append(col_rows[0])
        logger.debug('... done')
        return objects

    def _convert(self, value, field):
        convert = self.get_property('convert', field)
        container = self.get_property('container', field)
        if value and convert and container:
            _convert = partial(convert, self)
            value = map(_convert, value)
        elif convert:
            value = convert(self, value)
        else:
            value = value
        return value

    def _get_objects(self, oids, field_order, flush=False, autosnap=True):
        retries = self.config.sql_retries
        sql = self._gen_sql(oids, field_order)
        while retries >= 0:
            try:
                rows = self._fetchall(sql, field_order)
                logger.info('Fetch OK')
            except self.retry_on_error:
                tb = traceback.format_exc()
                logger.error('Fetch Failed: %s' % tb)
                del tb
                if retries == 0:
                    raise
                else:
                    retries -= 1
            else:
                rows = self._build_rows(rows)
                self.objects = self._build_objects(rows)
                break
        if flush:
            return self.flush(autosnap=autosnap)
        else:
            return self.objects.values()

    def _extract_row_ids(self, rows):
        if rows:
            return sorted([x[0] for x in rows])
        else:
            return []

    def _delta_force(self, force, last_update, parse_timestamp):
        if force is None:
            force = self.get_property('force', default=False)

        oids = []
        if isinstance(force, (list, tuple, set)):
            oids = force
        elif force is True:
            # get a list of all known object ids
            table = self.get_property('table')
            db = self.get_property('db')
            _id = self.get_property('column')
            sql = 'SELECT DISTINCT %s.%s FROM %s.%s' % (table, _id, db, table)
            rows = self.sql_proxy.fetchall(sql)
            oids = self._extract_row_ids(rows)
        else:
            if self.get_property('delta_new_ids', default=True):
                # get all new (unknown) oids
                oids.extend(self.get_new_oids())
            if self.get_property('delta_mtime', default=False):
                # get only those oids that have changed since last update
                mtime = self._fetch_mtime(last_update, parse_timestamp)
                if mtime:
                    oids.extend(self.get_changed_oids(mtime))
        return sorted(set(oids))

    def fetchall(self, sql, cached=True):
        '''
        Shortcut for getting a cursor, cleaning the sql a bit,
        adding the LIMIT clause, executing the sql, fetching
        all the results

        If certain failures occur, this method will authomatically
        attempt to reconnect and rerun.

        :param sql: sql string to execute
        :param cached: flag for using a chaced proxy or not
        '''
        logger.debug('Fetching rows...')
        proxy = self.get_sql_proxy(cached=cached)
        k = proxy.cursor()
        sql = re.sub('\s+', ' ', sql).strip().encode('utf-8')
        logger.debug('SQL:\n %s' % sql.decode('utf-8'))
        rows = None
        try:
            k.execute(sql)
            rows = k.fetchall()
        except Exception as e:
            if re.search('Transaction is not active', str(e)):
                if not self._auto_reconnect_attempted:
                    logger.warn('Transaction failure; reconnecting')
                    self.fetchall(sql, cached=False)
            logger.error('%s\n%s\n%s' % ('*' * 100, e, sql))
            raise
        else:
            if self._auto_reconnect_attempted:
                # in the case we've attempted to reconnect and
                # the transaction succeeded, reset this flag
                self._auto_reconnect_attempted = False
        finally:
            k.close()
            del k
        logger.debug('... fetched (%i)' % len(rows))
        return rows

    def get_sql_proxy(self, **kwargs):
        '''
        Database specific drivers must implemented this method.

        It is expected that by calling this method, the instance
        will set ._proxy with a auhenticated connection, which is
        also returned to the caller.
        '''
        raise NotImplementedError(
            "Driver has not provided a get_sql_proxy method!")

    def get_full_history(self, force=None, last_update=None,
                         parse_timestamp=None, flush=False, autosnap=False):
        '''
        Fields change depending on when you run activity_import,
        such as "last_updated" type fields which don't have activity
        being tracked, which means we'll always end up with different
        hash values, so we need to always remove all existing object
        states and import fresh
        '''
        logger.debug('Extracting Objects - Full History')

        oids = self._delta_force(force, last_update, parse_timestamp)
        logger.debug("Updating %s objects" % len(oids))

        batch_size = self.config.sql_batch_size
        max_workers = self.config.max_workers
        kwargs = self.config
        kwargs.pop('cube', None)  # ends up in config; ignore it
        if HAS_JOBLIB:
            runner = Parallel(n_jobs=max_workers)
            func = delayed(get_full_history)
            result = runner(func(
                cube=self._cube, oids=batch, flush=flush,
                cube_name=self.name, autosnap=autosnap, **kwargs)
                for batch in batch_gen(oids, batch_size))
            # merge list of lists (batched) into single list
            result = [i for l in result for i in l]
        else:
            result = []
            for batch in batch_gen(oids, batch_size):
                _ = get_objects(
                    cube=self._cube, oids=batch, flush=flush,
                    cube_name=self.name, autosnap=autosnap, **kwargs)
                result.extend(_)

        if flush:
            return result
        else:
            [self.objects.add(obj) for obj in result]
            return self

    def _unwrap_aggregated(self, rows):
        # unwrap aggregated values
        # FIXME: This unicode stuff is fragile and likely to fail
        encoding = self.get_property('encoding', default=DEFAULT_ENCODING)
        for k, row in enumerate(rows):
            _row = []
            for column in row:
                if type(column) is buffer:
                    # unwrap/convert the aggregated string 'buffer'
                    # objects to string
                    column = unicode(str(column), encoding)
                    column = column.replace('"', '').strip()
                    if not column:
                        column = None
                    else:
                        column = column.split('\n')
                else:
                    if isinstance(column, basestring):
                        column = unicode(column.decode(encoding))
                _row.append(column)
            rows[k] = _row
        return rows

    def _fetchall(self, sql, field_order):
        rows = self.sql_proxy.fetchall(sql)
        if not rows:
            return []
        logger.debug('Preparing row data...')
        rows = self._unwrap_aggregated(rows)
        k = len(rows)
        t0 = time.time()
        objects = [self._prep_object(row, field_order) for row in rows]
        t1 = time.time()
        logger.debug('... Rows prepared %i docs (%i/sec)' % (
            k, float(k) / (t1 - t0)))
        return objects

    def _fetch_mtime(self, last_update, parse_timestamp):
        mtime = None
        if last_update:
            if isinstance(last_update, basestring):
                mtime = dt_parse(last_update)
            else:
                mtime = last_update
        else:
            mtime = self.get_last_field('_start')
        # convert timestamp to datetime object
        mtime = ts2dt(mtime)
        logger.info("Last update mtime: %s" % mtime)

        if mtime:
            if parse_timestamp is None:
                parse_timestamp = self.get_property('parse_timestamp',
                                                    default=True)
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
        fieldmap = defaultdict(str)
        for field in self.fields:
            field_id = self.get_property('what', field)
            if field_id is not None:
                fieldmap[field_id] = field
        return fieldmap

    def _gen_sql(self, id_delta, field_order):
        '''
        '''
        logger.debug('Generating SQL...')
        db = self.get_property('db')
        table = self.get_property('table')
        selects = self._get_sql_selects(field_order)

        base_from = '%s.%s' % (db, table)
        froms = 'FROM ' + ', '.join([base_from])

        left_joins = self._get_sql_left_joins(field_order)

        delta_filter = []
        where = ''

        delta_filter.extend(
            self._get_id_delta_sql(table, id_delta))

        if delta_filter:
            where = 'WHERE ' + ' OR '.join(delta_filter)

        sql = 'SELECT %s %s %s %s' % (
            selects, froms, left_joins, where)

        sql += self._sql_sort(table)
        sql = self._sql_distinct(sql)
        logger.debug('... done')
        return sql

    def _get_id_delta_sql(self, table, id_delta):
        '''
        '''
        _id = self.get_property('column')
        if id_delta:
            id_delta = sorted(set(id_delta))
            id_delta = ','.join(map(str, id_delta))
            return ["(%s.%s IN (%s))" % (table, _id, id_delta)]
        else:
            return []

    def get_changed_oids(self, mtime):
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
        mtime_columns = self.get_property('delta_mtime', default=list())
        if not (mtime_columns and mtime):
            return []
        if isinstance(mtime_columns, basestring):
            mtime_columns = [mtime_columns]

        filters = []
        for _column in mtime_columns:
            _sql = "%s > %s" % (_column, mtime)
            filters.append(_sql)

        db = self.get_property('db')
        table = self.get_property('table')
        _id = self.get_property('column')

        sql = """SELECT DISTINCT %s.%s FROM %s.%s
            WHERE %s""" % (table, _id, db, table,
                           ' OR '.join(filters))
        rows = self.sql_proxy.fetchall(sql) or []
        return [x[0] for x in rows]

    def get_metrics(self, names=None):
        '''
        Run metric SQL defined in the cube.
        '''
        if isinstance(names, basestring):
            names = [names]

        start = utcnow()

        obj = {
            '_start': start,
            '_end': None,
        }

        objects = []
        for metric, definition in self.metrics.items():
            if names and metric not in names:
                continue
            sql = definition['sql']
            fields = definition['fields']
            _oid = definition['_oid']
            rows = self.sql_proxy.fetchall(sql)
            for row in rows:
                d = copy(obj)

                # derive _oid from metric name and row contents
                d['_oid'] = _oid(metric, row)

                for i, element in enumerate(row):
                    d[fields[i]] = element

                # append to the local metric result list
                objects.append(d)
        objects = self.normalize(objects)
        return objects

    def get_objects(self, force=None, last_update=None, parse_timestamp=None,
                    flush=False, autosnap=True):
        '''
        Extract routine for SQL based cubes.

        :param force:
            for querying for all objects (True) or only those passed in as list
        :param last_update: manual override for 'changed since date'
        :param parse_timestamp: flag to convert timestamp timezones in-line
        '''
        logger.debug('Fetching Objects - Current Values')

        # determine which oids will we query
        oids = self._delta_force(force, last_update, parse_timestamp)

        # set the 'index' of sql columns so we can extract
        # out the sql rows and know which column : field
        field_order = tuple(self.fields)

        batch_size = self.config.sql_batch_size
        kwargs = self.config
        kwargs.pop('cube', None)  # ends up in config; ignore it
        if HAS_JOBLIB:
            max_workers = self.config.max_workers
            runner = Parallel(n_jobs=max_workers)
            func = delayed(get_objects)
            result = runner(func(
                cube=self._cube, oids=batch, flush=flush,
                field_order=field_order, cube_name=self.name,
                autosnap=autosnap, **kwargs)
                for batch in batch_gen(oids, batch_size))
            # merge list of lists (batched) into single list
            result = [i for l in result for i in l]
        else:
            result = []
            for batch in batch_gen(oids, batch_size):
                _ = get_objects(
                    cube=self._cube, oids=batch, flush=flush,
                    field_order=field_order, cube_name=self.name,
                    autosnap=autosnap, **kwargs)
                result.extend(_)

        logger.debug('... current values objects get - done')
        if flush:
            return result
        else:
            [self.objects.add(obj) for obj in result]
            return self

    def get_new_oids(self):
        '''
        Returns a list of unique oids that have not been extracted yet.

        Essentially, a diff of distinct oids in the source database
        compared to cube.
        '''
        table = self.get_property('table')
        db = self.get_property('db')
        _id = self.get_property('column')
        last_id = self.get_last_field('_oid')
        if last_id:
            # if we delta_new_ids is on, but there is no 'last_id',
            # then we need to do a FULL run...
            try:  # try to convert to integer... if not, assume unicode value
                last_id = int(last_id)
            except (TypeError, ValueError):
                pass
            if type(last_id) in [int, float]:
                where = "%s.%s > %s" % (table, _id, last_id)
            else:
                where = "%s.%s > '%s'" % (table, _id, last_id)
            sql = 'SELECT DISTINCT %s.%s FROM %s.%s WHERE %s' % (
                table, _id, db, table, where)
            rows = self.sql_proxy.fetchall(sql)
            ids = self._extract_row_ids(rows)
        else:
            ids = []
        return ids

    def _get_sql_clause(self, clause, default=None):
        '''
        '''
        clauses = []
        for f in self.fields.iterkeys():
            try:
                _clause = self.fields[f]['sql'].get(clause)
            except KeyError:
                if default:
                    _clause = default
                else:
                    raise
            if type(_clause) is list:
                clauses.extend(_clause)
            else:
                clauses.append(_clause)
        clauses = list(set(clauses))
        try:
            del clauses[clauses.index(None)]
        except ValueError:
            pass
        return clauses

    def _get_sql_selects(self, field_order):
        table = self.get_property('table')
        _id = self.get_property('column')

        base_select = '%s.%s' % (table, _id)
        selects = [base_select]
        for f in field_order:
            try:
                assert isinstance(self.fields[f]['sql'], dict)
            except KeyError:
                select = '%s.%s' % (table, f)
            except AssertionError:
                select = '%s.%s' % (table, self.fields[f]['sql'])
            else:
                s = self.fields[f]['sql'].get('select')
                if re.match('!', s):
                    # if we start with a bang, append the line directly
                    select = re.sub('^!', '', s)
                elif self.get_property('container', f):
                    select = '%s_grouped.%s' % (f, s)
                else:
                    select = '%s.%s' % (f, s)
            selects.append(select)
        return ', '.join(selects)

    def _get_sql_left_joins(self, field_order):
        left_joins = []
        for f in field_order:
            try:
                assert isinstance(self.fields[f]['sql'], dict)
            except (KeyError, AssertionError):
                pass
            else:
                lj = self.fields[f]['sql'].get('left_join', [])
                for i in lj:
                    container = self.get_property('container', f)
                    is_str = isinstance(i, basestring)
                    if not (container or is_str):
                        _field = f
                        _select = i[0]
                        _select_name = '%s.%s' % (f, i[1])
                        _on_equals = i[2]

                        _lj = 'LEFT JOIN %s %s ON %s = %s' % (
                            _select, _field, _select_name, _on_equals)
                    elif container and not is_str:
                        raise ValueError(
                            "[%s] Write textagg(for) "
                            "join statements by hand!" % f)
                    else:
                        _lj = i
                    left_joins.append(_lj)
        return ' '.join(left_joins)

    def _log_inconsistency(self, last_doc, last_val, field, removed, added,
                           when, log_type):
        incon = {'oid': last_doc['_oid'],
                 'field': field,
                 'removed': removed,
                 'removed_type': str(type(removed)),
                 'added': added,
                 'added_type': str(type(added)),
                 'last_val': last_val,
                 'last_val_type': str(type(last_val)),
                 'when': str(ts2dt(when))}
        if log_type == 'json':
            self.log_inconsistency(json.dumps(incon, ensure_ascii=False))
        else:
            m = u'{oid} {field}: {removed}-> {added} has {last_val}; '
            m += u'({removed_type}-> {added_type} has {last_val_type})'
            m += u' ... on {when}'
            self.log_inconsistency(m.format(**incon))

    def _normalize_object(self, rows):
        o = rows.pop(0)
        for field, tokens in self.__row_iter(rows):
            if o[field] == tokens:
                continue

            if type(o[field]) is list:
                if type(tokens) is list:
                    o[field].extend(tokens)
                elif tokens not in o[field]:
                    o[field].append(tokens)
                else:
                    # skip non-unique duplicate values
                    continue
            else:
                o[field] = [o[field], tokens]

    def _normalize_container(self, value, field):
        container = self.get_property('container', field)
        value_is_list = type(value) is list
        if container and not value_is_list:
            # and normalize to be a singleton list
            value = [value] if value else None
        elif not container and value_is_list:
            raise ValueError(
                "Expected single value (%s), got list (%s)" % (
                    field, value))
        else:
            value = value
        return value

    @property
    def sql_proxy(self):
        raise NotImplementedError("sql_proxy is not defined")

    def _prep_object(self, row, field_order):
        '''
        0th item is always the object '_oid'
        Otherwise, fields is expected to map 1:1 with row columns
        '''
        row = list(row)
        obj = {'_oid': row.pop(0)}
        for k, column in enumerate(row, 0):
            field = field_order[k]
            column = self._normalize_container(column, field)
            column = self._convert(column, field)
            column = self._type(column, field)
            obj[field] = column
        return obj

    def __row_iter(self, rows):
        for row in rows:
            for field, tokens in row.iteritems():
                # _oid field doesn't require normalization
                if field != '_oid':
                    yield field, tokens

    def _setup_inconsistency_log(self):
        _logfile = self.config.logfile.split('.log')[0]
        basename = _logfile + '.inconsistencies'
        logfile = basename + '.log'

        logger_name = 'incon'
        logger = logging.getLogger(logger_name)
        hdlr = logging.FileHandler(logfile)
        log_format = logging.Formatter("%(message)s")
        hdlr.setFormatter(log_format)
        logger.addHandler(hdlr)
        logger.setLevel(logging.ERROR)
        logger.propagate = 0
        self.log_inconsistency = logger.error

    def sql_get_oids(self):
        '''
        Query source database for a distinct list of oids.
        '''
        table = self.get_property('table')
        _id = self.get_property('column')
        db = self.get_property('db')
        sql = 'SELECT DISTINCT %s.%s FROM %s.%s' % (table, _id, db, table)
        return sorted([r[0] for r in self.sql_proxy.fetchall(sql)])

    def _sql_distinct(self, sql):
        # whether to query for distinct rows only or not; default, no
        if self.get_property('distinct', default=False):
            return re.sub('^SELECT', 'SELECT DISTINCT', sql)
        else:
            return sql

    def _sql_sort(self, table):
        _id = self.get_property('column')
        if self.get_property('sort', default=False):
            return " ORDER BY %s.%s ASC" % (table, _id)
        else:
            return ""

    def _type_container(self, value, _type):
        ' apply type to all values in the list '
        if value is None:  # don't convert null values
            return value
        assert isinstance(value, (list, tuple))
        for i, item in enumerate(value):
            item = self._type_single(item, _type)
            value[i] = item
        value = sorted(value)
        return value

    def _type_single(self, value, _type):
        ' apply type to the single value '
        # FIXME: convert '' -> None?
        if None in [_type, value]:  # don't convert null values
            pass
        elif isinstance(value, _type):  # or values already of correct type
            pass
        else:
            value = _type(value)
            if isinstance(value, basestring):
                # FIXME: docode.locale()
                value = unicode(value)
        return value

    def _type(self, value, field):
        container = self.get_property('container', field)
        _type = self.get_property('type', field)
        if container:
            value = self._type_container(value, _type)
        else:
            value = self._type_single(value, _type)
        return value
