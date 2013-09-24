#!/usr/bin/env python
# Author:  Jan Grec <jgrec@redhat.com>

from metriquec.sql.basesql import BaseSql

callable_test = lambda row_limit: BaseSql(row_limit)

basesql = BaseSql()


def try_except(func, exceptions, **kwargs):
    try:
        func(**kwargs)
    except exceptions:
        pass


def test_proxy():
    ' By default, proxy is not implemented '
    try_except(lambda: basesql.proxy, NotImplementedError)


def test_cursor():
    '''
        By default, cursor (get from proxy) is unreachable (proxy not
        implemented
    '''
    try_except(lambda: basesql.cursor, NotImplementedError)


def test_fetchall():
    ' By default, trying to get proxy will raise not implemented '
    try_except(lambda: basesql.fetchall("", 5), NotImplementedError)


def test_validate_row_limit():
    ' check that row is an int or exception'
    try_except(lambda: basesql._validate_row_limit('a'), TypeError)

    assert isinstance(basesql._validate_row_limit(50.0), int)
