#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.utils
~~~~~~~~~~~~~~~~~

This module contains utility functions shared between
metrique sub-modules
'''

from __future__ import unicode_literals

import logging
logger = logging.getLogger('metrique')


import cPickle
import sqlite3


class SQLite3(object):
    ''' basic, naive dictionary like mapper "cube" container object '''
    def __init__(self, path):
        self._db_path = path
        self.proxy = sqlite3.connect(path)
        self.proxy.isolation_level = "DEFERRED"
        self.proxy.row_factory = self._dict_factory
        _ = 'create table if not exists cube (_id text primary key, data blob)'
        self.proxy.execute(_)

    def keys(self):
        select = "select _id from cube"
        return [o['_id'] for o in self.proxy.execute(select)]

    def __setitem__(self, key, value):
        key = unicode(key)
        obj = self._dumps(key=value)
        insert = "insert or replace into cube values (?, ?)"
        self.proxy.execute(insert, (key, obj))

    def __getitem__(self, key):
        select = "select data from cube where _id = '%s'" % key
        objs = [o for o in self.proxy.execute(select)]
        if objs:
            assert len(objs) == 1
            obj = objs[0]
            return self._loads(obj['data'])
        else:
            return None

    def sync(self):
        self.proxy.commit()

    def items(self):
        select = "SELECT * FROM cube"
        objs = [{r['_id']: self._loads(r['data'])}
                for r in self.proxy.execute(select)]
        return objs

    def values(self):
        select = "SELECT data FROM cube"
        objs = [self._loads(o['data']) for o in self.proxy.execute(select)]
        return objs

    @staticmethod
    def _dict_factory(cursor, row):
        desc = enumerate(cursor.description)
        return {col[0]: row[idx] for idx, col in desc}

    def _dumps(self, **kwargs):
        obj = cPickle.dumps(kwargs, protocol=2)
        obj = sqlite3.Binary(obj)
        return obj

    def _loads(self, value):
        value = str(value)
        return cPickle.loads(value)['key']

    def __len__(self):
        select = "SELECT count(*) as k FROM cube"
        rows = [r for r in self.proxy.execute(select)]
        assert len(rows) == 1
        return int(rows[0]['k'])
