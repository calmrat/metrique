#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from dateutil.parser import parse as dt_parse
from functools import partial
import re
import time

from metrique.client.cubes.basecube import BaseCube

from metrique.tools import batch_gen


DEFAULT_ROW_LIMIT = 100000
MAX_WORKERS = 1


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

        super(BaseSql, self).__init__(**kwargs)

    @property
    def proxy(self):
        raise NotImplementedError("BaseSql has not defined a proxy")

    def _fetchall(self, sql, start, field_order):
        '''
        '''

        logger.debug('Fetching rows...')
        rows = self.proxy.fetchall(sql, self.row_limit, start)
        logger.debug('... fetched (%i)' % len(rows))
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

        logger.debug('Preparing row data...')
        t0 = time.time()
        objects = [self._prep_object(row, field_order) for row in rows]
        t1 = time.time()
        logger.debug('... Rows prepared %i docs (%i/sec)' % (
            k, float(k) / (t1 - t0)))
        return objects

    def _prep_object(self, row, field_order):
        '''
        0th item is always the object '_id'
        Otherwise, fields is expected to map 1:1 with row columns
        '''
        row = list(row)
        obj = {'_id': row.pop(0)}
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
        if value and container and _type:
            value = map(_type, value)
        elif value and _type and type(value) is not _type:
            value = _type(value)
        else:
            value = value
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
        if id_delta and force:
            raise RuntimeError(
                "force and id_delta can't be used simultaneously")
        if isinstance(id_delta, basestring):
            id_delta = id_delta.split(',')
        elif isinstance(id_delta, int):
            id_delta = [id_delta]
        if not delta_batch_size:
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
            if isinstance(mtime, basestring):
                mtime = dt_parse(mtime)
            tzaware = (mtime and
                       hasattr(mtime, 'tzinfo') and
                       mtime.tzinfo)
            if c_fields and not tzaware:
                raise TypeError(
                    'last_update dates must be timezone '
                    'aware. Got: type(%s), %s' % (
                        type(mtime), mtime))
        logger.debug("(last update) mtime: %s" % mtime)
        return mtime

    def extract(self, exclude_fields=None, force=False, id_delta=None,
                last_update=None, workers=MAX_WORKERS, update=False,
                delta_batch_size=None, **kwargs):
        '''
        '''
        if not id_delta:
            id_delta = []
        exclude_fields = self.parse_fields(exclude_fields)
        id_delta, delta_batch_size = self._delta_init(
            id_delta, force, delta_batch_size)

        mtime = self._fetch_mtime(last_update, exclude_fields)
        id_delta.extend(self._get_mtime_id_delta(mtime))

        # this is to set the 'index' of sql columns so we can extract
        # out the sql rows and know which column : field
        field_order = list(set(self.fields) - set(exclude_fields))

        if id_delta:
            objects = []
            tries_left = self.config.sql_delta_batch_retries
            # Sometimes we have hiccups. Try, Try and Try again
            # to succeed, then fail.
            # FIXME: run these in a thread and kill them after
            # a given 'timeout' period passes.
            done = []
            while tries_left > 0:
                failed = []
                local_done = 0
                for batch in batch_gen(id_delta,
                                       delta_batch_size):
                    try:
                        objects.extend(self._extract(force, batch,
                                                     field_order))
                    except Exception as e:
                        failed.extend(batch)
                        logger.warn(
                            '%s\nBATCH Failed (%i). Tries remaining: %i' % (
                                e, len(failed), tries_left))
                        tries_left -= 1
                    else:
                        done.extend(batch)
                        local_done += len(batch)
                        logger.debug(
                            'BATCH SUCCESS. %i of %i' % (
                                local_done, len(id_delta)))
                else:
                    if failed:
                        id_delta = failed
                    else:
                        break
        else:
            objects = self._extract(force, id_delta, field_order)

        return self.save_objects(objects, update=update)

    def _build_rows(self, rows):
        logger.debug('Building dict_rows from sql_rows(%i)' % len(rows))
        _rows = {}
        for row in rows:
            _rows.setdefault(row['_id'], []).append(row)
        return _rows

    def _build_objects(self, rows):
        '''
        Given a set of rows/columns, build metrique object dictionaries
        Normalize null values to be type(None).
        '''
        logger.debug('Building objects from rows(%i)' % len(rows))
        objects = []
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
                # _id field doesn't require normalization
                if field != '_id':
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

    def _get_id_delta_sql(self, table, column, id_delta):
        '''
        '''
        if id_delta:
            id_delta = sorted(set(id_delta))
            if type(id_delta) is list:
                id_delta = ','.join(map(str, id_delta))
            return ["(%s.%s IN (%s))" % (table, column, id_delta)]
        else:
            return []

    def _get_last_id_sql(self):
        '''
        '''
        table = self.get_property('table')
        _id = self.get_property('column')
        last_id = self.get_last_id()
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

    def _get_mtime_id_delta(self, mtime):
        '''
        '''
        mtime_columns = self.get_property('delta_mtime', None, [])
        if not (mtime_columns and mtime):
            return []
        if isinstance(mtime_columns, basestring):
            mtime_columns = [mtime_columns]
        mtime = mtime.strftime(
            '%Y-%m-%d %H:%M:%S %z')
        dt_format = "yyyy-MM-dd HH:mm:ss z"
        filters = []
        for _column in mtime_columns:
            _sql = "%s > parseTimestamp('%s', '%s')" % (
                _column, mtime, dt_format)
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
        logger.debug('Generating SQL...')
        db = self.get_property('db')
        table = self.get_property('table')
        _id = self.get_property('column')

        selects = self._get_sql_selects(field_order)

        base_from = '%s.%s' % (db, table)
        froms = 'FROM ' + ', '.join([base_from])

        left_joins = self._get_sql_left_joins(field_order)

        delta_filter = []
        if id_delta:
            delta_filter.extend(self._get_id_delta_sql(table, _id, id_delta))
        if not force and self.get_property('delta', None, True):
            delta_filter.extend(self._get_last_id_sql())

        if delta_filter:
            where = 'WHERE ' + ' OR '.join(delta_filter)
        else:
            where = ''

        sql = 'SELECT %s %s %s %s' % (
            selects, froms, left_joins, where)

        if self.get_property('sort', None, False):
            sql += " ORDER BY %s.%s ASC" % (table, _id)

        # whether to query for distinct rows only or not; default, no
        if self.get_property('distinct', None, False):
            sql = re.sub('^SELECT', 'SELECT DISTINCT', sql)

        return sql
