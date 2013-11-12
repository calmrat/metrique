#!/usr/bin/env python
# Author:  Jan Grec <jgrec@redhat.com>

from metriquec.sql.basesql import BaseSql

callable_test = lambda: BaseSql()


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
