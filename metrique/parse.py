#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.parse
~~~~~~~~~~~~~~

This module contains various parser related functions
for consistently generating date range query syntax
and field inclusion/exclusion mappers, along with a custom
Metrique Query Parser which supports consisten querying
against mulitple datastorage backends (eg, postgresql,
sqlite, mongodb) using a single syntax.
'''

from __future__ import unicode_literals, absolute_import

import logging
logger = logging.getLogger('metrique')

import ast
import re

try:
    from sqlalchemy.sql import and_, or_, not_, operators
    from sqlalchemy import select, Table
except ImportError as e:
    logger.warn('sqlalchemy not installed! (%s)' % e)
    HAS_SQLALCHEMY = False


from metrique.utils import ts2dt, dt2ts


def parse_fields(fields, as_dict=False):
    '''
    Given a list of fields (or several other variants of the same),
    return back a consistent, normalized form of the same.

    To forms are currently supported:
        dictionary form (mongodb): dict 'key' is the field name
                                   and dict 'value' is either 1 (include)
                                   or 0 (exclude).
        list form (other): list values are field names to be included

    If fields passed is one of the following values, it will be assumed
    the user wants to include all fields and thus, we return an empty
    dict or list to indicate this, accordingly:
     * all fields: ['~', None, False, True, {}, []]


    '''
    _fields = {}
    if fields in ['~', None, False, True, {}, []]:
        # all these signify 'all fields'
        _fields = {}
    elif isinstance(fields, dict):
        _fields.update(
            {unicode(k).strip(): int(v) for k, v in fields.iteritems()})
    elif isinstance(fields, basestring):
        _fields.update({unicode(s).strip(): 1 for s in fields.split(',')})
    elif isinstance(fields, (list, tuple)):
        _fields.update({unicode(s).strip(): 1 for s in fields})
    else:
        raise ValueError("invalid fields value")
    if as_dict:
        return _fields
    else:
        return sorted(_fields.keys())


def date_range(date, func='date'):
    '''
    return back start and end dates given date string

    :param date: metrique date (range) to apply to pql query

    The tilde '~' symbol is used as a date range separated.

    A tilde by itself will mean 'all dates ranges possible'
    and will therefore search all objects irrelevant of it's
    _end date timestamp.

    A date on the left with a tilde but no date on the right
    will generate a query where the date range starts
    at the date provide and ends 'today'.
    ie, from date -> now.

    A date on the right with a tilde but no date on the left
    will generate a query where the date range starts from
    the first date available in the past (oldest) and ends
    on the date provided.
    ie, from beginning of known time -> date.

    A date on both the left and right will be a simple date
    range query where the date range starts from the date
    on the left and ends on the date on the right.
    ie, from date to date.
    '''
    if isinstance(date, basestring):
        date = date.strip()
    if not date:
        return '_end == None'
    if date == '~':
        return ''

    before = lambda d: '_start <= %s("%s")' % (func, ts2dt(d) if d else None)
    after = lambda d: '(_end >= %s("%s") or _end == None)' % \
        (func, ts2dt(d) if d else None)
    split = date.split('~')
    # replace all occurances of 'T' with ' '
    # this is used for when datetime is passed in
    # like YYYY-MM-DDTHH:MM:SS instead of
    #      YYYY-MM-DD HH:MM:SS as expected
    # and drop all occurances of 'timezone' like substring
    # FIXME: need to adjust (to UTC) for the timezone info we're dropping!
    split = [re.sub('\+\d\d:\d\d', '', d.replace('T', ' ')) for d in split]
    if len(split) == 1:  # 'dt'
        return '%s and %s' % (before(split[0]), after(split[0]))
    elif split[0] in ['', None]:  # '~dt'
        return before(split[1])
    elif split[1] in ['', None]:  # 'dt~'
        return after(split[0])
    else:  # 'dt~dt'
        return '%s and %s' % (before(split[1]), after(split[0]))


class MQLInterpreter(object):
    '''
    Simple interpreter that interprets MQL using SQLAlchemy constructs.

    FIXME: Add more docs
    '''
    def __init__(self, table):
        '''
        :param sqlalchemy.Table table:
            the table definition
        '''
        if not isinstance(table, Table):
            raise ValueError('table must be instance of sqlalchemy.Table;'
                             ' got %s' % type(table))
        self.table = table
        self.scalars = []
        self.arrays = []

        for field in table.c:
            try:
                ptype = field.type.python_type
            except NotImplementedError:
                logger.warn(
                    "%s (%s) has no python_type defined" % (field, field.type))
                ptype = None

            if ptype is list:
                self.arrays.append(field.name)
            else:
                self.scalars.append(field.name)

    def parse(self, s):
        tree = ast.parse(s, mode='eval').body
        return self.p(tree)

    def p(self, node):
        try:
            p = getattr(self, 'p_' + node.__class__.__name__)
        except:
            raise ValueError('Cannot parse: %s' % node)
        return p(node)

    def p_BoolOp(self, node):
        return self.p(node.op)(*map(self.p, node.values))

    def p_UnaryOp(self, node):
        return self.p(node.op)(self.p(node.operand))

    def p_And(self, node):
        return and_

    def p_Or(self, node):
        return or_

    def p_Not(self, node):
        return not_

    op_dict = {
        'Eq': lambda (left, right):  left == right,
        'NotEq': lambda (left, right):  left != right,
        'Gt': lambda (left, right):  left > right,
        'GtE': lambda (left, right):  left >= right,
        'Lt': lambda (left, right):  left < right,
        'LtE': lambda (left, right):  left <= right,
        'In': lambda (left, right):  left.in_(right),
        'NotIn': lambda (left, right):  not_(left.in_(right)),
    }

    arr_op_dict = {
        'Eq': lambda (left, right):  left.any(right, operator=operators.eq),
        'NotEq': lambda (left, right):  left.all(right,
                                                 operator=operators.ne),
        'In': lambda (left, right):  or_(*[
            left.any(v, operator=operators.eq) for v in right]),
        'NotIn': lambda (left, right):  and_(*[
            left.all(v, operator=operators.ne) for v in right]),
    }

    def p_Compare(self, node):
        if len(node.comparators) != 1:
            raise ValueError('Wrong number of comparators: %s' % node.ops)
        left = self.p(node.left)
        right = self.p(node.comparators[0])
        op = node.ops[0].__class__.__name__
        # Eq, NotEq, Gt, GtE, Lt, LtE, In, NotIn
        if node.left.id in self.arrays:
            return self.arr_op_dict[op]((left, right))
        elif isinstance(right, tuple) and right[0] in ['regex', 'iregex']:
            oper = "~" if right[0] == 'regex' else "~*"
            regex = right[1]
            if op == 'Eq':
                return left.op(oper)(regex)
            if op == 'NotEq':
                return left.op("!" + oper)(regex)
            raise ValueError('Unsupported operation for regex: %s' % op)
        else:
            return self.op_dict[op]((left, right))
        raise ValueError('Unsupported operation: %s' % op)

    def p_Num(self, node):
        return node.n

    def p_Str(self, node):
        return node.s

    def p_List(self, node):
        return map(self.p, node.elts)

    def p_Tuple(self, node):
        return map(self.p, node.elts)

    def p_Name(self, node):
        if node.id in ['None', 'True', 'False']:
            return eval(node.id)
        if node.id in self.scalars + self.arrays:
            return self.table.c[node.id]
        raise ValueError('Unknown field: %s' % node.id)

    def p_array_name(self, node):
        if node.id in self.arrays:
            return self.table.c[node.id]
        raise ValueError('Expected array field: %s' % node.id)

    def p_Call(self, node):
        if node.func.id == 'empty':
            if len(node.args) != 1:
                raise ValueError('empty expects 1 argument.')
            name = self.p_array_name(node.args[0])
            return name == '{}'
        elif node.func.id == 'date':
            if len(node.args) != 1:
                raise ValueError('date expects 1 argument.')
            else:
                # convert all datetimes to float epoch
                node.args[0].s = dt2ts(node.args[0].s)
                return self.p(node.args[0])
        elif node.func.id in ['regex', 'iregex']:
            return (node.func.id, self.p(node.args[0]))
        else:
            raise ValueError('Unknown function: %s' % node.func.id)


def parse(table, query=None, date=None, fields=None,
          distinct=False, limit=None, alias=None):
    date = date_range(date)
    if query and date:
        query = '%s and %s' % (query, date)
    elif date:
        query = date
    elif query:
        pass
    else:  # date is null, query is not
        query = None

    fields = parse_fields(fields=fields) or None
    fields = fields if fields else [table]

    msg = 'parse(query=%s, fields=%s)' % (query, fields)
    #msg = re.sub(' in \[[^\]]+\]', ' in [...]', msg)
    logger.debug(msg)
    kwargs = {}
    if query:
        interpreter = MQLInterpreter(table)
        query = interpreter.parse(query)
        kwargs['whereclause'] = query
    if distinct:
        kwargs['distinct'] = distinct
    query = select(fields, from_obj=table, **kwargs)
    if limit >= 1:
        query = query.limit(limit)
    if alias:
        query = query.alias(alias)
    return query
