#!/usr/bin/env python
# Author:  Jan Grec <jgrec@redhat.com>

import os
from unittest import TestCase, main

from metrique.server.mongodb.basemongodb import BaseMongoDB

from jsonconf import JSONConf

config_dir = '~/.metrique/'
config_dir = os.path.expanduser(config_dir)
config = JSONConfig('mongodb', config_dir)

host = config['host']
username = config['admin_username']
password = config['admin_password']
admin_db = config['admin_db']


class Test_MongoDB_Admin(TestCase):

    def setUp(self):
        """Sets up admindb connection into test collection."""
        db = 'test'
        self.admindb = BaseMongoDB(host, username, password, db, admin_db)
        self.test = self.admindb[db]

    def test_insert(self):
        """Tests database insert function."""
        self.test.insert({'_id': 'test_insert', 'value': 'test'}, safe=True)
        self.assertTrue(self.test.find_one({'_id': 'test_insert'}))
        self.test.remove({'_id': 'test_insert'})

    def test_save(self):
        """Tests database save function."""
        self.test.save({'_id': 'test_save', 'value': 'test'}, safe=True)
        self.assertTrue(self.test.find_one({'_id': 'test_save'}))
        self.test.remove({'_id': 'test_save'})

    def test_find(self):
        """
        Tests database find function.

        At first, tries to find any data in test database. Then runs
        through returned cursor, removing each record. At the end tries
        to find data again, raising ValueError if finding is successful.
        """
        cursor = self.test.find()
        self.assertTrue(cursor)
        for record in cursor:
            self.test.remove({'_id': record['_id']})
        cursor = self.test.find()
        self.assertFalse(cursor.count(), "Find has found data in empty database")

    def test_update(self):
        """
        Tests database update function.

        Creates two records, updates one at a time, and then
        updates both at the same time using multi=True.
        """
        self.test.save({'_id': 1, 'value': 'test'})
        self.test.save({'_id': 2, 'value': 'test'})

        ud1 = {'$set': {'value': 'new_value_1', 'ad_value': 'test'}}
        ud2 = {'$set': {'value': 'new_value_2', 'ad_value': 'test'}}
        self.test.update({'_id': 1}, ud1)
        self.test.update({'_id': 2}, ud2)

        new_record_1 = self.test.find_one({'_id': 1})
        self.assertEqual(new_record_1['value'], u'new_value_1')
        new_record_2 = self.test.find_one({'_id': 2})
        self.assertEqual(new_record_2['value'], u'new_value_2')

        nud = {'$set': {'value': 'the_newest_value', 'ad_value': 'changed'}}
        self.test.update({'_id': {'$gt': 0}}, nud, multi=True)

        for record in self.test.find():
            self.assertEqual(record['value'], u'the_newest_value')
            self.assertEqual(record['ad_value'], u'changed')

        self.test.remove({'_id': 1})
        self.test.remove({'_id': 2})

    def test_ensure_index(self):
        """
        Tests database ensure_index function.

        Creates three records with ascending integer '_id' and descending
        character 'value'. Then creates an unnamed ascending index on 'value'
        field.
        """
        self.test.drop_indexes()
        self.test.save({'_id': 1, 'value': 'c'})
        self.test.save({'_id': 2, 'value': 'b'})
        self.test.save({'_id': 3, 'value': 'a'})
        key = str("value")
        self.test.ensure_index(key, 5, name="testingdex")
        cursor = self.test.find({'value': {'$gt': 'b'}})
        for record in cursor:
            self.assertTrue(record['_id'], 1)
        self.test.drop_indexes()
        self.test.remove({'_id': 1})
        self.test.remove({'_id': 2})
        self.test.remove({'_id': 3})


if __name__ == '__main__':
    main()
