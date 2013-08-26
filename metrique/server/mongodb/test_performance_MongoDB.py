#!/usr/bin/env python
# Author:  Jan Grec <jgrec@redhat.com>

from datetime import datetime
import os
import time
from unittest import TestCase, TestLoader, TextTestRunner

from metrique.server.Utils.MongoDB.BaseMongoDB import BaseMongoDB

from jsonconf import JSONConf

config_dir = '~/.metrique/'
config_dir = os.path.expanduser(config_dir)
config = JSONConfig('mongodb', config_dir)

host = config['host']
username = config['admin_username']
password = config['admin_password']
admin_db = config['admin_db']
tests_db = 'tests'
junk_c = 'junk'
results_c = 'results'

TEST_RANGE = 10000   # How many operation iterations will be done during one test
JUNK_RANGE = TEST_RANGE + 1  # Pre-created junk collection records count
                             # MUST BE GREATER THAN TEST_RANGE
SAFE = False

# WARNING
# These tests creates new collection ('tests') in metrique database and in this
# collection creates this dbs:
#   - junk (storing testing data - removed after each test)
#   - results (storing results after each test - doesn't get removed)


class Test_MongoDB_Performance(TestCase):

    def setUp(self):
        """Opens connection and creates 'junk' records."""
        self.admindb = BaseMongoDB(host, username, password,
                                   tests_db, admin_db)
        self.junk = self.admindb[junk_c]
        self.results = self.admindb[results_c]
        self.junk.remove(safe=SAFE)
        for record in range(0, JUNK_RANGE):
            self.junk.insert({'_id': record, 'value': 'test_value'}, safe=SAFE)

    def tearDown(self):
        """Removes all 'junk' records."""
        self.junk.remove(safe=SAFE)
        self.admindb.close()
        del(self.junk)
        del(self.results)
        del(self.admindb)

    def save_result(self, case, time):
        """Stores results of performance tests in mongo database."""
        result = {'date': datetime.utcnow(), 'test': 'MongoDB-performance',
                  'case': case, 'result': time, 'range': TEST_RANGE}
        self.results.insert(result)

    def test_remove_ascending(self):
        """Remove test - ascending."""
        id_set = range(0, TEST_RANGE)
        S = time.time()
        for sid in id_set:
            self.junk.remove({'_id': sid}, safe=SAFE)
        D = time.time() - S
        self.save_result('remove (asc)', D)

    def test_find_ascending(self):
        """Find test - ascending."""
        id_set = range(0, TEST_RANGE)
        S = time.time()
        for sid in id_set:
            self.junk.find({'_id': sid})
        D = time.time() - S
        self.save_result('find (asc)', D)

    def test_findone_ascending(self):
        """Find_one test - ascending."""
        id_set = range(0, TEST_RANGE)
        S = time.time()
        for sid in id_set:
            self.junk.find_one({'_id': sid})
        D = time.time() - S
        self.save_result('find_one (asc)', D)

    def test_save(self):
        """Save test."""
        id_set = range(0, TEST_RANGE)
        S = time.time()
        for sid in id_set:
            record = {'_id': sid, 'value': 'test_junks'}
            self.junk.save(record, safe=SAFE)
        D = time.time() - S
        self.save_result('save', D)

    def test_updatemulti(self):
        """Update test (multi=True)."""
        record = {'$set': {'value': 'test_junks'}}
        S = time.time()
        self.junk.update({'_id': {'$lt': TEST_RANGE}}, record, multi=True, safe=SAFE)
        D = time.time() - S
        self.save_result('update-multi', D)

    def test_update_ascending(self):
        """Update test - ascending."""
        id_set = range(0, TEST_RANGE)
        record = {'$set': {'value': 'test_junks'}}
        S = time.time()
        for sid in id_set:
            self.junk.update({'_id': sid}, record, safe=SAFE)
        D = time.time() - S
        self.save_result('update (asc)', D)

    def test_insert(self):
        """Insert test."""
        id_set = range(JUNK_RANGE, JUNK_RANGE + TEST_RANGE)
        S = time.time()
        for sid in id_set:
            record = {'_id': sid, 'value': 'test_dumps'}
            self.junk.insert(record, safe=SAFE)
        D = time.time() - S
        self.save_result('insert', D)


def print_results():
    admindb = BaseMongoDB(host, username, password,
                          tests_db, admin_db)
    results = admindb[results_c]

    res_table = {}

    cases = results.distinct('case')
    for case in cases:

        cur = results.find({"case": case}).sort("date", -1)
        try:
            actual_rec = cur.next()
            actual = actual_rec[u'result']
            actual_average = actual / actual_rec[u'range']
        except StopIteration:
            actual = 0.0
            actual_average = 0.0
            previous = 0.0
            previous_average = 0.0
        try:
            previous_rec = cur.next()
            previous = previous_rec[u'result']
            previous_average = previous / previous_rec[u'range']
        except StopIteration:
            previous = 0.0
            previous_average = 0.0

        cur = results.find({"case": case}).sort("result", 1)
        try:
            best_rec = cur.next()
            best = best_rec[u'result']
            best_average = best / best_rec[u'range']
        except StopIteration:
            best = 0.0
            best_average = 0.0

        cur = results.find({"case": case}).sort("result", -1)
        try:
            worst_rec = cur.next()
            worst = worst_rec[u'result']
            worst_average = worst / worst_rec[u'range']
        except:
            worst = 0.0
            worst_average = 0.0

        res_table[case] = {'actual': actual, 'actual_average': actual_average,
                           'previous': previous, 'previous_average': previous_average,
                           'best': best, 'best_average': best_average,
                           'worst': worst, 'worst_average': worst_average}

    admindb.close()

    print "name" + (14 * ' ') + "actual  act(av)   prev    prev(av)  best    best(av)  worst   worst(av)"
    print (89 * '-')
    for case in cases:
        spaces = 18 - len(str(case))
        table = "%3.4f  %3.6f  %3.4f  %3.6f  %3.4f  %3.6f  %3.4f  %3.6f" % (res_table[case]['actual'],
                                                                            res_table[case]['actual_average'],
                                                                            res_table[case]['previous'],
                                                                            res_table[case]['previous_average'],
                                                                            res_table[case]['best'],
                                                                            res_table[case]['best_average'],
                                                                            res_table[case]['worst'],
                                                                            res_table[case]['worst_average'])
        print str(case) + (spaces * ' ') + table

if __name__ == '__main__':
    suite = TestLoader().loadTestsFromTestCase(Test_MongoDB_Performance)
    TextTestRunner(verbosity=2).run(suite)
    print_results()
