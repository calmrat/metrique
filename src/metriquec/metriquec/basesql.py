#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Base cube for extracting data from SQL databases
'''

from dateutil.parser import parse as dt_parse
from functools import partial
import pytz
import re
import time
import traceback

from metriquec.basecube import BaseCube

from metrique.utils import batch_gen, ts2dt

DEFAULT_ROW_LIMIT = 100000
DEFAULT_RETRIES = 10


class BaseSql(BaseCube):
    '''
    Base, common functionality driver for connecting
    and extracting data from SQL databases.

    **This class MUST be subclassed**.

    .proxy must be defined, in order to know how
    to get a connection object to the target sql db.

    FIXME ... MORE DOCS TO COME
    '''
    def __init__(self, host, port, db, row_limit=DEFAULT_ROW_LIMIT, **kwargs):
        self.host = host
        self.port = port
        self.db = db
        self.row_limit = row_limit
        self.retry_on_error = Exception
        super(BaseSql, self).__init__(**kwargs)

    @property
    def proxy(self):
        raise NotImplementedError("BaseSql has not defined a proxy")

    def _fetchall(self, sql, start, field_order):
        '''
        '''

        self.logger.debug('Fetching rows...')
        rows = self.proxy.fetchall(sql, self.row_limit, start)
        self.logger.debug('... fetched (%i)' % len(rows))
        if not rows:
            return []

        for k, row in enumerate(rows):
            _row = []
            for column in row:
                if type(column) is buffer:
                    # unwrap/convert the aggregated string 'buffer'
                    # objects to string
                    column = str(column).replace('"', '').strip()
                    if not column:
                        column = None
                    else:
                        column = column.split('\n')
                _row.append(column)
            rows[k] = _row

        k = len(rows)

        self.logger.debug('Preparing row data...')
        t0 = time.time()
        objects = [self._prep_object(row, field_order) for row in rows]
        t1 = time.time()
        self.logger.debug('... Rows prepared %i docs (%i/sec)' % (
            k, float(k) / (t1 - t0)))
        return objects

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
            column = self._type(column, field)
            column = self._convert(column, field)

            if not column:
                obj.update({field: None})
            else:
                obj.update({field: column})
        return obj

    def _type(self, value, field):
        container = self.get_property('container', field)
        _type = self.get_property('type', field)
        if None in [value, _type] or isinstance(value, _type):
            # skip converting null values
            # and skip converting if _type is null
            return value
        elif container:
            # appy type to all values in the list
            items = []
            for item in value:
                if isinstance(item, basestring):
                    item = item.decode('utf8')
                items.append(_type(item))
            value = items
        else:
            # apply type to the single value
            if isinstance(value, basestring):
                value = value.decode('utf8')
            value = _type(value)
        return value

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

    def _delta_init(self, id_delta, force, delta_batch_size):
        if isinstance(id_delta, basestring):
            id_delta = id_delta.split(',')
        elif isinstance(id_delta, int):
            id_delta = [id_delta]
        if delta_batch_size is None:
            delta_batch_size = self.config.sql_delta_batch_size
        return id_delta, delta_batch_size

    def _fetch_mtime(self, last_update, exclude_fields):
        mtime = None
        if last_update:
            if isinstance(last_update, basestring):
                mtime = dt_parse(last_update)
            else:
                mtime = last_update
        else:
            # list_cube_fields returns back a dict from the server that
            # contains a global _mtime that represents the last time
            # any field was updated.
            c_fields = self.list_cube_fields(exclude_fields=exclude_fields,
                                             _mtime=True)
            mtime = c_fields.get('_mtime')

        # convert timestamp to datetime object
        mtime = ts2dt(mtime)

        self.logger.info("Last update mtime: %s" % mtime)
        return mtime

    def _extract_id_delta(self, id_delta, delta_batch_size,
                          force, field_order, retries):
        objects = []
        if not retries:
            retries = self.config.sql_delta_batch_retries
        # Sometimes we have hiccups. Try, Try and Try again
        # to succeed, then fail.
        # retires == -1 means unlimited retries
        # FIXME: run these in a thread and kill them after
        # a given 'timeout' period passes.
        done = []
        while retries != 0:
            failed = []
            local_done = 0
            for batch in batch_gen(id_delta,
                                   delta_batch_size):
                try:
                    objects.extend(self._extract(force, batch,
                                                 field_order))
                except self.retry_on_error:
                    failed.extend(batch)
                    tb = traceback.format_exc()
                    self.logger.warn(
                        'ERROR: %s\nBATCH Failed (%i). '
                        'Tries remaining: %i' % (
                            tb, len(failed), retries))
                    retries -= 1
                else:
                    done.extend(batch)
                    local_done += len(batch)
                    self.logger.info(
                        'BATCH SUCCESS. %i of %i' % (
                            local_done, len(id_delta)))
            else:
                if failed:
                    id_delta = failed
                else:
                    break
        else:
            rt = self.config.sql_delta_batch_retries
            raise RuntimeError(
                "Query Failed after %s retries." % rt)
        return objects, failed

    def _extract_loop(self, sql, start=0):
        _stop = False
        _rows = []
        while not _stop:
            rows = self.proxy.fetchall(sql, self.row_limit, start)
            _rows.extend(rows)

            k = len(rows)
            if k < self.row_limit:
                _stop = True
            else:
                start += k
                # theoretically, k == self.row_limit
                assert k == self.row_limit
        return _rows

    def _extract_row_ids(self, rows):
        if rows:
            return sorted([x[0] for x in rows])
        else:
            return []

    def extract(self, exclude_fields=None, force=False, id_delta=None,
                last_update=None, update=False, delta_batch_size=None,
                retries=DEFAULT_RETRIES, row_limit=None, parse_timestamp=None,
                dry_run=False, **kwargs):
        '''
        Extract routine for SQL based cubes.

        ... docs coming soon ...

        Accept, but ignore unknown kwargs.
        '''
        if 'debug' in kwargs:
            self.config.debug = self.logger, kwargs['debug']

        if not id_delta:
            id_delta = []
        if row_limit:
            self.row_limit = row_limit
        if parse_timestamp is None:
            parse_timestamp = self.get_property('parse_timestamp', None, True)

        exclude_fields = self.parse_fields(exclude_fields)
        id_delta, delta_batch_size = self._delta_init(
            id_delta, force, delta_batch_size)

        if force and not id_delta and delta_batch_size != 0:
            # get a list of all object ids and batch extract them
            table = self.get_property('table')
            _id = self.get_property('column')
            sql = 'SELECT DISTINCT %s.%s FROM %s.%s' % (
                table, _id, self.db, table)
            rows = self._extract_loop(sql)
            id_delta = self._extract_row_ids(rows)
        elif force and id_delta:
            # force:True and id_delta:True == ONLY filter on id_delta
            pass
        elif not force:
            # include objects updated since last mtime too
            mtime = self._fetch_mtime(last_update, exclude_fields)
            id_delta.extend(self._get_mtime_id_delta(mtime, parse_timestamp))

        # this is to set the 'index' of sql columns so we can extract
        # out the sql rows and know which column : field
        field_order = list(set(self.fields) - set(exclude_fields))

        if id_delta and delta_batch_size != 0:
            objects, failed = self._extract_id_delta(
                id_delta, delta_batch_size, force,
                field_order, retries)
        else:
            objects = self._extract(force, id_delta, field_order)
            failed = []

        # FIXME: Queue these up for the next extract call!?
        if failed:
            self.logger.error('FAILED: %s' % failed)

        if dry_run:
            return objects
        else:
            return self.save_objects(objects, update=update)

    def _build_rows(self, rows):
        _rows = {}
        if not rows:
            return _rows
        self.logger.debug('Building dict_rows from sql_rows(%i)' % len(rows))
        for row in rows:
            _rows.setdefault(row['_oid'], []).append(row)
        return _rows

    def _build_objects(self, rows):
        '''
        Given a set of rows/columns, build metrique object dictionaries
        Normalize null values to be type(None).
        '''
        objects = []
        if not rows:
            return objects
        self.logger.debug('Building objects from rows(%i)' % len(rows))
        for col_rows in rows.itervalues():
            if len(col_rows) > 1:
                obj = self._normalize_object(col_rows)
                objects.append(obj)
            else:
                objects.append(col_rows[0])
        return objects

    def __row_iter(self, rows):
        for row in rows:
            for field, tokens in row.iteritems():
                # _oid field doesn't require normalization
                if field != '_oid':
                    yield field, tokens

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

            #try:
            #    del o[field][o[field].index(None)]
            #    # if we have more than one value, drop any
            #    # redundant None (null) values, if any
            #except (ValueError):
            #    pass

    def _extract(self, force, id_delta, field_order):
        '''
        '''
        sql = self._gen_sql(force, id_delta, field_order)

        start = 0
        _stop = False
        _rows = []
        while not _stop:
            rows = self._fetchall(sql, start, field_order)
            _rows.extend(rows)

            k = len(rows)
            if k < self.row_limit:
                _stop = True
            else:
                start += k
                # theoretically, k == self.row_limit
                assert k == self.row_limit

        __rows = self._build_rows(_rows)
        objects = self._build_objects(__rows)
        return objects

    def _get_sql_clause(self, clause, default=None):
        '''
        '''
        clauses = []
        for f in self.fields.keys():
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

    def _get_id_delta_sql(self, table, id_delta):
        '''
        '''
        _id = self.get_property('column')
        if id_delta:
            id_delta = sorted(set(id_delta))
            if type(id_delta) is list:
                id_delta = ','.join(map(str, id_delta))
            return ["(%s.%s IN (%s))" % (table, _id, id_delta)]
        else:
            return []

    def _get_last_id_sql(self):
        '''
        '''
        table = self.get_property('table')
        _id = self.get_property('column')
        last_id = self.get_last_oid()
        if last_id and self.get_property('delta_new_ids', True):
            # if we delta_new_ids is on, but there is no 'last_id',
            # then we need to do a FULL run...
            try:  # try to convert to integer... if not, assume unicode value
                last_id = int(last_id)
            except (TypeError, ValueError):
                pass
            if type(last_id) in [int, float]:
                last_id_sql = "%s.%s > %s" % (table, _id, last_id)
            else:
                last_id_sql = "%s.%s > '%s'" % (table, _id, last_id)
            return [last_id_sql]
        else:
            return []

    def _get_mtime_id_delta(self, mtime, parse_timestamp):
        '''
        '''
        mtime_columns = self.get_property('delta_mtime', None, [])
        if not (mtime_columns and mtime):
            return []
        if isinstance(mtime_columns, basestring):
            mtime_columns = [mtime_columns]

        if not (hasattr(mtime, 'tzinfo') and mtime.tzinfo):
            # We need the timezone, to readjust relative to the server's tz
            mtime = mtime.replace(tzinfo=pytz.utc)
        mtime = mtime.strftime(
            '%Y-%m-%d %H:%M:%S %z')
        dt_format = "yyyy-MM-dd HH:mm:ss z"

        filters = []
        if parse_timestamp:
            mtime = "parseTimestamp('%s', '%s')" % (mtime, dt_format)
        else:
            mtime = "'%s'" % mtime

        for _column in mtime_columns:
            _sql = "%s > %s" % (_column, mtime)
            filters.append(_sql)

        db = self.get_property('db')
        table = self.get_property('table')
        _id = self.get_property('column')

        sql = """SELECT DISTINCT %s.%s FROM %s.%s
               WHERE %s""" % (table, _id, db, table,
                              ' OR '.join(filters))
        rows = self.proxy.fetchall(sql)
        if rows:
            ids = [x[0] for x in rows]
            return ids
        else:
            return []

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

    def _gen_sql(self, force, id_delta, field_order):
        '''
        '''
        self.logger.debug('Generating SQL...')
        db = self.get_property('db')
        table = self.get_property('table')

        selects = self._get_sql_selects(field_order)

        base_from = '%s.%s' % (db, table)
        froms = 'FROM ' + ', '.join([base_from])

        left_joins = self._get_sql_left_joins(field_order)

        delta_filter = []
        where = ''
        if force and id_delta:
            delta_filter.extend(
                self._get_id_delta_sql(table, id_delta))

        elif not force:
            # apply delta sql clause's if we're not forcing a full run
            if id_delta:
                delta_filter.extend(
                    self._get_id_delta_sql(table, id_delta))
            if self.get_property('delta', None, True):
                delta_filter.extend(self._get_last_id_sql())

        if delta_filter:
            where = 'WHERE ' + ' OR '.join(delta_filter)

        sql = 'SELECT %s %s %s %s' % (
            selects, froms, left_joins, where)

        sql += self._sql_sort(table)
        sql = self._sql_distinct(sql)

        return sql

    def _sql_distinct(self, sql):
        # whether to query for distinct rows only or not; default, no
        if self.get_property('distinct', None, False):
            return re.sub('^SELECT', 'SELECT DISTINCT', sql)
        else:
            return sql

    def _sql_sort(self, table):
        _id = self.get_property('column')
        if self.get_property('sort', None, False):
            return " ORDER BY %s.%s ASC" % (table, _id)
        else:
            return ""
