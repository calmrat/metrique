#!/usr/bin/env python
# Author:  Jan Grec <jgrec@redhat.com>

from metriquec.sql.basesql import BaseSql

callable_test = lambda row_limit: BaseSql(row_limit)


def init_basesql():
    return BaseSql()


def try_except(func, exceptions, **kwargs):
    try:
        func(**kwargs)
    except exceptions:
        pass


def test_proxy():
    ' By default, proxy is not implemented '
    basesql = init_basesql()
    try_except(lambda: basesql.get_proxy(), NotImplementedError)


def test_validate_row_limit():
    ' check that row is an int or exception'
    basesql = init_basesql()
    try_except(lambda: basesql._validate_row_limit('a'), TypeError)

    assert isinstance(basesql._validate_row_limit(50.0), int)
