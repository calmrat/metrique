#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

import re
import time

from metrique.client.cubes.basecube import BaseCube

from metrique.tools.constants import INT_TYPE, FLOAT_TYPE

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
            container = self.get_property('container', field)
            column_is_list = type(column) is list
            if container and not column_is_list:
                # and normalize to be a singleton list
                column = [column] if column else None
            elif not container and column_is_list:
                raise ValueError(
                    "Expected single value (%s), got list (%s)" % (
                        field, column))
            #else:    pass  # column = column

            _type = self.get_property('type', field)
            if column and container and _type:
                column = map(_type, column)
            elif column and _type and type(column) is not _type:
                column = _type(column)
            else:
                pass

            convert = self.get_property('convert', field)
            if convert and container:
                column = map(convert, self, column)
            elif convert:
                column = convert(self, column)
            #else: pass

            if not column:
                obj.update({field: None})
            else:
                obj.update({field: column})
        return obj

    def extract(self, exclude_fields=None, force=False, id_delta=None,
                last_update=None, workers=MAX_WORKERS, **kwargs):
        '''
        '''
        if id_delta and force:
            raise RuntimeError(
                "force and id_delta can't be used simultaneously")

        exclude_fields = self.parse_fields(exclude_fields)
        field_order = list(set(self.fields) - set(exclude_fields))

        mtime = None
        if last_update:
            mtime = last_update
        else:
            # list_cube_fields returns back a dict from the server that
            # contains the most recent mtime for the given field, if any.
            # keys are fields; values are mtimes
            # from all fields, get the oldest and use it as 'last update' of
            # any cube object.
            c_fields = self.list_cube_fields(exclude_fields=exclude_fields)
            mtimes = sorted(
                [v for f, v in c_fields.items()])
            mtime = mtimes[0] if mtimes else None
            tzaware = (mtime and
                       hasattr(mtime, 'tzinfo') and
                       mtime.tzinfo)
            if c_fields and not tzaware:
                raise TypeError(
                    'last_update dates must be timezone '
                    'aware. Got: %s' % mtime)
        logger.debug("mtime: %s" % mtime)

        objects = self._extract(force, id_delta, mtime, field_order)

        return self.save_objects(objects)

    def _build_rows(self, rows):
        _rows = {}
        for row in rows:
            _rows.setdefault(row['_id'], []).append(row)
        return _rows

    def _build_objects(self, rows):
        '''
        Given a set of rows/columns, build metrique object dictionaries
        Normalize null values to be type(None).
        '''
        objects = []
        for k, v in rows.iteritems():
            if len(v) > 1:
                o = v.pop(0)
                for e in v:
                    for _k, _v in e.iteritems():
                        if _k == '_id':
                            # these are always the same...
                            continue
                        elif o[_k] != _v:
                            if type(o[_k]) is list:
                                if type(_v) is list:
                                    o[_k].extend(_v)
                                elif _v not in o[_k]:
                                    o[_k].append(_v)
                                else:
                                    # skip non-unique duplicate values
                                    continue
                            else:
                                o[_k] = [o[_k], _v]

                            try:
                                del o[_k][o[_k].index(None)]
                                # if we have more than one value, drop any
                                # redundant None (null) values, if any
                            except (ValueError):
                                pass
                        else:
                            pass
                objects.append(o)
            else:
                objects.append(v[0])
        #else:
        #    # walk through all the objects one more time... converting
        #    # those expected to be container
        #    for i, o in enumerate(objects):
        #        for f, v in o.iteritems():
        #            container = self.get_property('container', f)
        #            v_is_list = type(v) is list
        #            if container and not v_is_list:
        #                objects[i][f] = [v]
        #            elif not container and v_is_list:
        #                raise ValueError(
        #                    "Expected single value (%s), got list (%s)" % (
        #                        f, v))
        return objects

    def _extract(self, force, id_delta, mtime, field_order):
        '''
        '''
        sql = self._gen_sql(force, id_delta, mtime, field_order)

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

        return self._build_objects(__rows)

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
        # the following deltas are mutually exclusive
        return ["(%s.%s IN (%s))" % (table, column, id_delta)]

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
            if type(last_id) in [INT_TYPE, FLOAT_TYPE]:
                last_id_sql = "%s.%s > %s" % (table, _id, last_id)
            else:
                last_id_sql = "%s.%s > '%s'" % (table, _id, last_id)
            return [last_id_sql]
        else:
            return []

    def _get_mtime_sql(self, mtime):
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
        return filters

    def _get_delta_sql(self, mtime=None):
        '''
        '''
        delta_filter = []
        # last_id delta
        delta_filter.extend(self._get_last_id_sql())
        # mtime based delta
        delta_filter.extend(self._get_mtime_sql(mtime))
        return delta_filter

    def _gen_sql(self, force, id_delta, mtime, field_order):
        '''
        '''
        db = self.get_property('db')
        table = self.get_property('table')
        _id = self.get_property('column')

        base_select = '%s.%s' % (table, _id)
        selects = [base_select]
        #groupbys = copy(selects)

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
        selects = ', '.join(selects)

        base_from = '%s.%s' % (db, table)
        froms = 'FROM ' + ', '.join([base_from])

        left_joins = []
        for f in field_order:
            try:
                assert isinstance(self.fields[f]['sql'], dict)
            except (KeyError, AssertionError):
                pass
            else:
                lj = self.fields[f]['sql'].get('left_join', [])
                for i in lj:
                    if isinstance(i, basestring):
                        # use the string in its raw form
                        left_joins.append(i)
                        continue


                    if self.get_property('container', f):
                        __select = self.fields[f]['sql'].get('select')
                        _select = '''
(SELECT %s, TEXTAGG(FOR(%s.%s)) AS %s
%s
JOIN %s %s ON %s.%s = %s
GROUP BY %s)
                        ''' % (base_select, f, __select, __select,
                               froms,
                               i[0], f, f, i[1], base_select,
                               base_select)
                        _select_name = base_select
                        _on_equals = '%s_grouped.%s' % (f, i[2])
                        _field = '%s_grouped' % f
                    else:
                        _field = f
                        _select = i[0]
                        _select_name = '%s.%s' % (f, i[1])
                        _on_equals = i[2]

                    _lj = 'LEFT JOIN %s %s ON %s = %s' % (
                        _select, _field, _select_name, _on_equals)

                    left_joins.append(_lj)
        left_joins = ' '.join(left_joins)

        joins = []
        for f in field_order:
            try:
                assert isinstance(self.fields[f]['sql'], dict)
            except (KeyError, AssertionError):
                pass
            else:
                lj = self.fields[f]['sql'].get('join', [])
                for i in lj:
                    if isinstance(i, basestring):
                        joins.append(i)
                    else:
                        joins.append(
                            'JOIN %s %s ON %s.%s = %s' % (
                                i[0], f, f, i[1], i[2]))
        joins = ' '.join(joins)

        # the following deltas are mutually exclusive
        if id_delta:
            delta_filter = self._get_id_delta_sql(table, _id, id_delta)
        elif not force and self.get_property('delta', None, True):
            delta_filter = self._get_delta_sql(mtime)
        else:
            delta_filter = []

        if delta_filter:
            where = 'WHERE ' + ' OR '.join(delta_filter)
        else:
            where = ''

        #sql = 'SELECT %s %s %s %s %s GROUP BY %s' % (
        #    selects, froms, left_joins, joins, where, groupbys)
        sql = 'SELECT %s %s %s %s %s' % (
            selects, froms, left_joins, joins, where)

        if self.get_property('sort', None, False):
            sql += " ORDER BY %s.%s ASC" % (table, _id)

        # whether to query for distinct rows only or not; default, no
        if self.get_property('distinct', None, False):
            sql = re.sub('^SELECT', 'SELECT DISTINCT', sql)

        return sql
