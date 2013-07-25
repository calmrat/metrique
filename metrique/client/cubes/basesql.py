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

    def _fetchall(self, sql, start, exclude_fields, field_order):
        '''
        '''

        logger.debug('Fetching rows...')
        rows = self.proxy.fetchall(sql, self.row_limit, start)
        logger.debug('... fetched (%i)' % len(rows))
        if not rows:
            return []

        k = len(rows)

        logger.debug('Preparing row data...')
        t0 = time.time()
        objects = [self._prep_row(row,
                                  exclude_fields,
                                  field_order) for row in rows]
        t1 = time.time()
        logger.debug('... Rows prepared %i docs (%i/sec)' % (
            k, float(k) / (t1 - t0)))
        return objects

    def _prep_row(self, row, exclude_fields, field_order):
        '''
        0th item is always the object '_id'
        Otherwise, fields is expected to map 1:1 with row columns
        '''
        row = list(row)
        obj = {'_id': row.pop(0)}
        for k, e in enumerate(row, 0):
            field = field_order[k]
            convert = self.get_property('convert', field, None)
            if convert:
                e = convert(self, e)
            if e is '':
                e = None  # normalize to None for empty strings
            obj.update({field: e})
        return obj

    def extract(self, exclude_fields=None, force=False, id_delta=None,
                last_update=None, workers=MAX_WORKERS, **kwargs):
        '''
        '''
        objects = self._extract(exclude_fields, force, id_delta, last_update)
        return self.save_objects(objects)

    def _extract(self, exclude_fields=None, force=False,
                 id_delta=None, last_update=None):
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
            c_fields = self.list_cube_fields()
            if c_fields:
                mtimes = sorted([v for v in c_fields.values()])
                mtime = mtimes[0]
                tzaware = (mtime and
                           hasattr(mtime, 'tzinfo') and
                           mtime.tzinfo)
                if not tzaware:
                    raise TypeError(
                        'last_update dates must be timezone '
                        'aware. Got: %s' % mtime)
        logger.debug("mtime: %s" % mtime)

        sql = self._gen_sql(force, id_delta, mtime, field_order)

        start = 0
        _stop = False
        _rows = []
        while not _stop:
            rows = self._fetchall(sql, start, exclude_fields, field_order)
            _rows.extend(rows)

            k = len(rows)
            if k < self.row_limit:
                _stop = True
            else:
                start += k
                if k != self.row_limit:  # theoretically, k == self.row_limit
                    logger.warn(
                        "rows count seems incorrect! "
                        "row_limit: %s, row returned: %s" % (
                            self.row_limit, k))

        objects = []

        __rows = {}
        for row in _rows:
            __rows.setdefault(row['_id'], []).append(row)

        for k, v in __rows.iteritems():
            if len(v) > 1:
                o = v.pop(0)
                for e in v:
                    for _k, _v in e.iteritems():
                        if _k == '_id':
                            # these are always the same...
                            continue
                        elif o[_k] != _v:
                            if type(o[_k]) is list:
                                if _v not in o[_k]:
                                    o[_k].append(_v)
                                else:
                                    continue
                            else:
                                o[_k] = [o[_k], _v]

                            try:
                                del o[_k][o[_k].index(None)]
                                # if we have more than one value, drop any
                                # redundant None (null) values, if any
                            except (ValueError):
                                pass

                objects.append(o)
            else:
                objects.append(v[0])
        else:
            # walk through all the objects one more time... converting
            # those expected to be container
            for i, o in enumerate(objects):
                for f, v in o.iteritems():
                    container = self.get_property('container', f)
                    v_is_list = type(v) is list
                    if container and not v_is_list:
                        objects[i][f] = [v]
                    elif not container and v_is_list:
                        raise ValueError(
                            "Expected single value (%s), got list (%s)" % (
                                f, v))
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

        selects = ['%s.%s' % (table, _id)]

        for f in field_order:
            try:
                assert isinstance(self.fields[f]['sql'], dict)
            except KeyError:
                selects.append('%s.%s' % (table, f))
            except AssertionError:
                selects.append('%s.%s' % (table,
                                          self.fields[f]['sql']))
            else:
                s = self.fields[f]['sql'].get('select')
                if re.match('!', s):
                    # if we start with a bang, append the line directly
                    s = re.sub('^!', '', s)
                    selects.append(s)
                else:
                    selects.append('%s.%s' % (f, s))
        selects = ', '.join(selects)

        froms = ['%s.%s' % (db, table)]
        froms = 'FROM ' + ', '.join(froms)

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
                        left_joins.append(i)
                    else:
                        left_joins.append(
                            'LEFT JOIN %s %s ON %s.%s = %s' % (
                                i[0], f, f, i[1], i[2]))
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

        sql = 'SELECT %s %s %s %s %s' % (selects, froms, left_joins, joins, where)

        #groupbys = ', '.join(self._get_sql_clause('groupby'))
        #if groupbys:
        #    sql += ' GROUP BY %s ' % ', '.join(groupbys)

        if self.get_property('sort', None, False):
            sql += " ORDER BY %s.%s ASC" % (table, _id)

        # whether to query for distinct rows only or not; default, no
        if self.get_property('distinct', None, False):
            sql = re.sub('^SELECT', 'SELECT DISTINCT', sql)

        return sql
