#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dateutil.parser import parse as dt_parse
from functools import partial
import pytz
import re
import time
import traceback

from metrique.core_api import HTTPClient
from metriqueu.utils import batch_gen, ts2dt, strip_split


class Generic(HTTPClient):
    '''
    Base, common functionality driver for connecting
    and extracting data from SQL databases.

    **This class MUST be subclassed**.

    .proxy must be defined, in order to know how
    to get a connection object to the target sql db.

    FIXME ... MORE DOCS TO COME
    '''
    def __init__(self, sql_host=None, sql_port=None, **kwargs):
        super(Generic, self).__init__(**kwargs)
        if sql_host:
            self.config['sql_host'] = sql_host
        if sql_port:
            self.config['sql_port'] = sql_port
        self.retry_on_error = None

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
        self.logger.debug('... done')
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

    def _extract(self, id_delta, field_order):
        retries = self.config.retries
        sql = self._gen_sql(id_delta, field_order)
        while 1:
            try:
                rows = self._fetchall(sql, field_order)
            except self.retry_on_error:
                tb = traceback.format_exc()
                self.logger.error('Fetch Failed: %s' % tb)
                del tb
                if retries == 0:
                    raise
                else:
                    retries -= 1
            else:
                break
        rows = self._build_rows(rows)
        objects = self._build_objects(rows)
        # save the objects
        self.cube_save(objects)
        # log the objects saved
        return [o['_oid'] for o in objects]

    def _extract_threaded(self, id_delta, field_order):
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as ex:
            futures = [ex.submit(self._extract, batch, field_order)
                       for batch in batch_gen(id_delta,
                                              self.config.sql_batch_size)]
        saved = []
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                tb = traceback.format_exc()
                self.logger.error('Extract Error: %s\n%s' % (e, tb))
                del tb
            else:
                saved.extend(result)
                self.logger.info(
                    '%i of %i extracted' % (len(saved),
                                            len(id_delta)))
        failed = set(id_delta) - set(saved)
        result = {'saved': sorted(saved), 'failed': sorted(failed)}
        self.logger.debug(result)
        return result

    def _extract_row_ids(self, rows):
        if rows:
            return sorted([x[0] for x in rows])
        else:
            return []

    def extract(self, exclude_fields=None, force=None,
                last_update=None, parse_timestamp=None, **kwargs):
        '''
        Extract routine for SQL based cubes.

        ... docs coming soon ...

        :param list/str exclude_fields:
            A list or csv string of the field names to exclude.
        :param force:
            If None (use: default False), then it will try to extract
            only the objects that have changed since the last extract.
            If True, then it will try to extract all the objects.
            If it is a list of oids, then it will try to extract only those
            objects with oids from the list.

        Accept, but ignore unknown kwargs.
        '''
        exclude_fields = strip_split(exclude_fields)

        oids = []

        if force is None:
            force = self.get_property('force', None, False)

        if force is True:
            # get a list of all known object ids
            table = self.get_property('table')
            db = self.get_property('db')
            _id = self.get_property('column')
            sql = 'SELECT DISTINCT %s.%s FROM %s.%s' % (table, _id, db,
                                                        table)
            rows = self.proxy.fetchall(sql)
            oids = self._extract_row_ids(rows)

        # [cward] FIXME: is 'delta' flag necessary? just look for
        # the individual delta flags, no?
        if force is False and self.get_property('delta', None, True):
            # include objects updated since last mtime too
            # apply delta sql clause's if we're not forcing a full run
            if self.get_property('delta_mtime', None, False):
                mtime = self._fetch_mtime(last_update, parse_timestamp)
                if mtime:
                    oids.extend(self.get_changed_oids(mtime))
            if self.get_property('delta_new_ids', None, True):
                oids.extend(self.get_new_oids())

        if isinstance(force, list):
            oids = force

        oids = sorted(set(oids))

        # this is to set the 'index' of sql columns so we can extract
        # out the sql rows and know which column : field
        field_order = list(set(self.fields) - set(exclude_fields))

        if self.config.sql_batch_size <= 0:
            return self._extract(oids, field_order)
        else:
            return self._extract_threaded(oids, field_order)

    def _fetchall(self, sql, field_order):
        rows = self.proxy.fetchall(sql)
        if not rows:
            return []

        # unwrap aggregated values
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

        self.logger.debug('Preparing row data...')
        k = len(rows)
        t0 = time.time()
        objects = [self._prep_object(row, field_order) for row in rows]
        t1 = time.time()
        self.logger.debug('... Rows prepared %i docs (%i/sec)' % (
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
        self.logger.info("Last update mtime: %s" % mtime)

        if mtime:
            if parse_timestamp is None:
                parse_timestamp = self.get_property('parse_timestamp',
                                                    None, True)
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
        Dictionary of field_id: field_name
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
        self.logger.debug('Generating SQL...')
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
        self.logger.debug('... done')
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

    def get_new_oids(self):
        '''
        Returns a list of new oids that have not been extracted yet.
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
            rows = self.proxy.fetchall(sql)
            ids = self._extract_row_ids(rows)
        else:
            ids = []
        return ids

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
        '''
        mtime_columns = self.get_property('delta_mtime', None, [])
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
        rows = self.proxy.fetchall(sql) or []
        return [x[0] for x in rows]

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
    def proxy(self):
        raise NotImplementedError("proxy is not defined")

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

            if not column:
                obj.update({field: None})
            else:
                obj.update({field: column})
        return obj

    def __row_iter(self, rows):
        for row in rows:
            for field, tokens in row.iteritems():
                # _oid field doesn't require normalization
                if field != '_oid':
                    yield field, tokens

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

    def _type(self, value, field):
        container = self.get_property('container', field)
        _type = self.get_property('type', field)
        if None in [_type, value]:
            # don't convert null values
            return value
        elif container:
            sort = self.get_property('sort', field, 1)
            # apply type to all values in the list
            items = []
            for item in value:
                if item is None or isinstance(item, _type):
                    # skip converting null values
                    # and skip converting if _type is null
                    pass
                else:
                    item = _type(item)

                # normalize strings to utf8
                if isinstance(item, basestring):
                    item = item.decode('utf8')

                items.append(item)
            if sort == 1:
                value = sorted(items)
            elif sort == -1:
                value = sorted(items, reverse=True)
            else:
                value = items
        else:
            # apply type to the single value
            if isinstance(value, basestring):
                value = value.decode('utf8')
            elif value is None or isinstance(value, _type):
                pass
            else:
                value = _type(value)
        return value
