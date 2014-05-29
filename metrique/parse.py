#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger('metrique')

import ast
import re

try:
    from sqlalchemy.sql import and_, or_, not_, operators
    from sqlalchemy.sql.expression import func
    from sqlalchemy import select, Table
except ImportError:
    logger.warn('sqlalchemy not installed!')
    HAS_SQLALCHEMY = False


from metrique.utils import ts2dt, is_true


def parse_fields(fields, as_dict=False):
    _fields = {}
    if fields in [None, False]:
        _fields = {}
    elif fields in ['~', True]:
        _fields = {}
    elif isinstance(fields, dict):
        _fields.update(fields)
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


def date_range(date):
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
    #if not date:
    #    return '_end == None'
    if not date:
        return '_end == None'
    if date == '~':
        return ''

    _b4 = '_start <= date("%s")'
    before = lambda d: _b4 % ts2dt(d) if d else None
    _after = '(_end >= date("%s") or _end == None)'
    after = lambda d: _after % ts2dt(d) if d else None
    split = date.split('~')
    # FIXME: should we adjust for the timezone info we're dropping?
    # replace all occurances of 'T' with ' '
    # this is used for when datetime is passed in
    # like YYYY-MM-DDTHH:MM:SS instead of
    #      YYYY-MM-DD HH:MM:SS as expected
    # and drop all occurances of 'timezone' like substring
    split = [re.sub('\+\d\d:\d\d', '', d.replace('T', ' ')) for d in split]
    if len(split) == 1:
        # 'dt'
        return '%s and %s' % (before(split[0]), after(split[0]))
    elif split[0] in ['', None]:
        # '~dt'
        return before(split[1])
    elif split[1] in ['', None]:
        # 'dt~'
        return after(split[0])
    else:
        # 'dt~dt'
        return '%s and %s' % (before(split[1]), after(split[0]))


class SQLAlchemyMQLParser(object):
    '''
    Simple sytax parser that converts to SQL
    '''
    def __init__(self, table):
        '''
        :param sqlalchemy.Table table:
            the table definition
        '''
        is_true(isinstance(table, Table),
                'table must be instance of sqlalchemy.Table;'
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

    def parse(self, query=None, date=None, fields=None,
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
        fields = fields if fields is not None else [self.table]

        kwargs = {}
        if query:
            tree = ast.parse(query, mode='eval').body
            query = self.p(tree)
            kwargs['whereclause'] = query
        if distinct:
            kwargs['distinct'] = distinct
        query = select(fields, from_obj=self.table, **kwargs)
        if limit >= 1:
            query = query.limit(limit)
        if alias:
            query = query.alias(alias)
        return query

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
        elif isinstance(right, tuple) and right[0] == 'regex':
            regex = right[1]
            if op == 'Eq':
                return left.op("~")(regex)
            if op == 'NotEq':
                return left.op("!~")(regex)
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
            return func.date(self.p(node.args[0]))
        elif node.func.id == 'regex':
            return ('regex', self.p(node.args[0]))
        raise ValueError('Unknown function: %s' % node.func.id)
