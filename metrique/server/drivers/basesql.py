#!/usr/bin/env pyehon
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from logging import getLogger
logger = getLogger(__name__)

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, date
from datetime import time as dt_time
import re
import time

from metrique.server.drivers.basedriver import BaseDriver
from metrique.server.drivers.drivermap import get_cube
from metrique.server.etl import get_last_id
from metrique.server.etl import save_doc, last_known_warehouse_mtime

from metrique.tools.constants import UTC
from metrique.tools.constants import INT_TYPE, FLOAT_TYPE
from metrique.tools.type_cast import type_cast

DEFAULT_ROW_LIMIT = 100000
MAX_WORKERS = 1


class BaseSql(BaseDriver):
    '''
    '''
    def __init__(self, host, db, row_limit=None,
                 *args, **kwargs):
        super(BaseSql, self).__init__(*args, **kwargs)
        self.db = db
        self.host = host
        if not row_limit:
            row_limit = DEFAULT_ROW_LIMIT
        self.row_limit = row_limit

    @property
    def proxy(self):
        raise NotImplementedError("BaseSql has not defined a proxy")

    def _sql_fetchall(self, sql, start, field, row_limit):
        '''
        '''
        logger.debug('Fetching rows')

        # return the raw as token if no convert is defined by driver (self)
        convert = self.get_field_property('convert', field, None)

        # if driver.field specifies a type for this field, use it
        # otherwise, it'll be casted into a unicode string
        token_type = self.get_field_property('type', field)
        logger.debug('... Field Token Type: %s - %s' % (field, token_type))

        rows = list(self.proxy.fetchall(sql, row_limit, start))
        k = len(rows)

        logger.debug('... fetched (%i)' % len(rows))
        if not rows:
            return []

        logger.debug('Preparing row data...')
        t0 = time.time()
        _rows = []

        for row in rows:
            _rows.append(self._get_row(row, field,
                         convert, token_type))

        t1 = time.time()
        logger.info('... Rows prepared %i docs (%i/sec)' % (
            k, float(k) / (t1 - t0)))
        return _rows

    def _get_row(self, row, field, convert, token_type):
        # id 'column' is expected first
        id = row[0]
        # and raw token 'lookup' second
        raw = row[1]
        if type(raw) is date:
            # force convert dates into datetimes... otherwise mongo barfs
            raw = datetime.combine(raw, dt_time()).replace(tzinfo=UTC)
        # convert based on driver defined conversion method
        # and cast to appropriate data type
        if convert:
            tokens = convert(self, raw)
        else:
            tokens = raw
        tokens = type_cast(tokens, token_type)

        return {'id': id, 'field': field, 'tokens': tokens}

    def grouper(self, rows):
        ''' Group tokens by id/field '''
        k = len(rows)
        logger.debug('... ... ... Grouping started of %s rows!' % k)
        grouped = {}
        t0 = time.time()
        for row in rows:
            id = row['id']
            field = row['field']
            tokens = row['tokens']
            grouped.setdefault(id, {})
            grouped[id].setdefault(field, [])
            if not tokens:  # if tokens is empty, don't update the list
                continue
            grouped[id][field].append(tokens)
        t1 = time.time()
        logger.info('... ... ... Grouped %i docs (%i/sec)' % (
            k, float(k) / (t1 - t0)))
        return grouped

    def extract_func(self, **kwargs):
        if self.metrique_config.async:
            with ProcessPoolExecutor(MAX_WORKERS) as executor:
                future = executor.submit(_extract_func, self.name, **kwargs)
            return future.result()
        else:
            _extract_func(self.name, **kwargs)


def _extract_func(cube, **kwargs):
    '''
    SQL import method
    '''
    c = get_cube(cube)
    field = kwargs.get('field')
    if not field:
        raise ValueError("Field argument required")
    force = int(kwargs.get('force', 0))
    id_delta = kwargs.get('id_delta', None)

    if id_delta:
        if force:
            raise RuntimeError("force and id_delta can't be used simultaneously")
        else:
            touch = False
    else:
        touch = True

    db = c.get_field_property('db', field)
    table = c.get_field_property('table', field)
    db_table = '%s.%s' % (db, table)
    column = c.get_field_property('column', field)
    table_column = '%s.%s' % (table, column)

    # max number of rows to return per call (ie, LIMIT)
    row_limit = c.get_field_property('row_limit', field, c.row_limit)
    try:
        row_limit = int(row_limit)
    except (TypeError, ValueError):
        raise ValueError("row_limit must be a number")

    _sql = c.get_field_property('sql', field)
    sql_where = []
    sql = 'SELECT %s, %s FROM ' % (table_column, _sql[0])
    _from = [db_table]
    if _sql[1]:
        _from.extend(_sql[1])
    sql += ', '.join(_from)
    sql += ' '
    if _sql[2]:
        sql += ' '.join(_sql[2])
    sql += ' '
    if _sql[3]:
        sql_where.append('(%s)' % ' OR '.join(_sql[3]))

    delta_filter = []
    delta_filter_sql = None

    # force full update
    if force:
        _delta = False
    else:
        _delta = c.get_field_property('delta', field, True)

    if _delta:
        # delta is enabled
        # the following deltas are mutually exclusive
        if id_delta:
            delta_sql = "(%s IN (%s))" % (table_column, id_delta)
            delta_filter.append(delta_sql)
        elif c.get_field_property('delta_new_ids', field):
            # if we delta_new_ids is on, but there is no 'last_id',
            # then we need to do a FULL run...
            last_id = get_last_id(c.name, field)
            if last_id:
                # FIXME: any reason to ensure we know what the _id is typecasted as?
                try:
                        last_id = int(last_id)
                except (TypeError, ValueError):
                        pass

                if type(last_id) in [INT_TYPE, FLOAT_TYPE]:
                    last_id_sql = "%s > %s" % (table_column, last_id)
                else:
                    last_id_sql = "%s > '%s'" % (table_column, last_id)
                delta_filter.append(last_id_sql)

            mtime_columns = c.get_field_property('delta_mtime', field)
            if mtime_columns:
                if isinstance(mtime_columns, basestring):
                    mtime_columns = [mtime_columns]
                last_update_dt = last_known_warehouse_mtime(c.name, field)
                last_update_dt = last_update_dt.strftime('%Y-%m-%d %H:%M:%S %z')
                dt_format = "yyyy-MM-dd HH:mm:ss z"
                for _column in mtime_columns:
                    _sql = "%s > parseTimestamp('%s', '%s')" % (
                        _column, last_update_dt, dt_format)
                    delta_filter.append(_sql)

    if delta_filter:
        delta_filter_sql = ' OR '.join(delta_filter)
        sql_where.append('(%s)' % delta_filter_sql)

    if sql_where:
        sql += ' WHERE %s ' % ' AND '.join(sql_where)

    if not c.get_field_property('no_sort', field, False):
        sql += " ORDER BY %s ASC" % table_column

    # whether to query for distinct rows only or not; default, no
    if c.get_field_property('distinct', field, False):
        sql = re.sub('^SELECT', 'SELECT DISTINCT', sql)

    start = 0
    saved = 0
    _stop = False
    rows = []
    failed = []

    # FIXME: prefetch the next set of rows while importing to mongo
    logger.debug('... ... Starting SQL fetchall routine!')

    container = c.get_field_property('container', field)

    if touch:
        now = datetime.now(UTC)
        spec_mtime = {'cube': cube}
        update_mtime = {'$set': {field: {'mtime': now}}}

    while not _stop:
        rows = c._sql_fetchall(sql, start, field, row_limit)
        k = len(rows)
        if k > 0:
            logger.debug('... ... Starting Processer')
            grouped = c.grouper(rows)
            logger.debug('... ... Saving docs now!')
            t0 = time.time()
            _id_k = 0
            for _id in grouped.iterkeys():
                _id_k += 1
                for field in grouped[_id].iterkeys():
                    tokens = grouped[_id][field]
                    if not tokens:
                        tokens = None
                    elif container and type(tokens) is not list:
                        tokens = [tokens]
                    elif not container and type(tokens) is list:
                        if len(tokens) > 1:
                            raise TypeError(
                                "Tokens contains too many values (%s); "
                                "(set container=True?)" % (tokens))
                        else:
                            tokens = tokens[0]

                    try:
                        saved += save_doc(c.name, field, tokens, _id)
                    except Exception as e:
                        logger.error(
                            'Error saving (%s) %s: %s' % (tokens, _id, e))
                        saved = 0
                    if not saved:
                        failed.append(_id)
            t1 = time.time()
            logger.info('... ... Saved %i docs (%i/sec)' % (
                k, k / (t1 - t0)))
        else:
            logger.debug('... ... No rows; nothing to process')

        if k < row_limit:
            _stop = True
        else:
            start += k
            if k != row_limit:  # theoretically, k == row_limit
                logger.warn(
                    "rows count seems incorrect! row_limit: %s, row returned: %s" % (
                        row_limit, k))

    result = {'saved': saved}
    if failed:
        result.update({'failed_ids': failed})
    else:
        if touch:
            # update the mtimestamp for when this field was last touched
            # to the moment we started updating
            c.c_etl_activity.update(spec_mtime, update_mtime, upsert=True)
    return result
