#!/usr/bin/env python
# Author:  Jan Grec <jgrec@redhat.com>

from unittest import TestCase, main

from metrique.server.utils.sql.basesql import BaseSql

callable_test = lambda row_limit: BaseSql(row_limit)


class TestBaseSql(TestCase):

    def setUp(self):
        self.basesql = BaseSql()

    def test_proxy(self):
        # By default, proxy is not implemented
        self.assertRaises(NotImplementedError, lambda: self.basesql.proxy)

    def test_cursor(self):
        # By default, cursor (get from proxy) is unreachable (proxy not
        # implemented)
        self.assertRaises(NotImplementedError, lambda: self.basesql.cursor)

    def test_configure(self):
        # By default, configure is not implemented
        self.assertRaises(NotImplementedError, lambda: self.basesql.configure())


    def test_fetchall(self):
        # By default, trying to get proxy will raise not implemented
        self.assertRaises(NotImplementedError, lambda: self.basesql.fetchall("", 5))

if __name__ == '__main__':
    main()
