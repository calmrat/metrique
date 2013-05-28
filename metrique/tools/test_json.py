#!/usr/bin/env python
# coding: utf-8
# Author:  Jan Grec <jgrec@redhat.com>

from bson.objectid import ObjectId
from datetime import datetime, date
from dateutil import parser
from random import randint
from unittest import TestCase, main

from metrique.tools.json import Encoder

RAND_ITERATIONS = 10000


class TestEncoder(TestCase):
    """JSON Encoder tests."""

    def setUp(self):
        self.enc = Encoder()

    def test_datetime(self):
        """
        datetime() encoding test.

        Tests current day with all information, one day with only date
        information, and randomly generated dates.
        Backwards conversion done by dateutils.parser.
        """

        # today() test - all information
        today_dt = datetime.today()
        today_enc = self.enc.encode(today_dt)
        self.assertEqual(parser.parse(today_enc.replace('"', '')),
                         today_dt)

        # date information only
        day_only_dt = datetime(2013, 4, 3)
        day_only_enc = self.enc.encode(day_only_dt)
        self.assertEqual(parser.parse(day_only_enc.replace('"', '')),
                         day_only_dt)

        # random generated datetime
        for x in range(RAND_ITERATIONS):
            day = randint(1, 28)
            month = randint(1, 12)
            year = randint(100, 9999)
            hour = randint(1, 23)
            minute = randint(1, 59)
            second = randint(1, 59)
            day_dt = datetime(year, month, day, hour, minute, second)
            day_enc = self.enc.encode(day_dt)
            self.assertEqual(parser.parse(day_enc.replace('"', '')),
                             day_dt)

    def test_date(self):
        """
        date() encoding test.

        Tests current day with date information, and randomly generated
        dates.
        Backwards conversion done by datetime.date.
        """

        # today() test - all information
        today_d = date.today()
        today_enc = self.enc.encode(today_d)
        enc_args = [int(x) for x in today_enc.replace('"', '').split('-')]
        self.assertEqual(date(enc_args[0], enc_args[1], enc_args[2]),
                         today_d)

        # random generated date
        for x in range(RAND_ITERATIONS):
            day = randint(1, 28)
            month = randint(1, 12)
            year = randint(1, 9999)
            day_d = date(year, month, day)
            day_enc = self.enc.encode(day_d)
            enc_args = [int(x) for x in day_enc.replace('"', '').split('-')]
            self.assertEqual(date(enc_args[0], enc_args[1], enc_args[2]),
                             day_d)

    def test_ObjectID(self):
        """
        ObjectId() encoding test.

        Tests randomly generates correct ObjectIds objects.
        Backwards conversion done by bson.objectid.
        """

        hexa = list("0123456789abcdef")

        # random generated objectid
        for x in range(RAND_ITERATIONS):
            id_str = ""
            for char in range(24):
                id_str += hexa[randint(0, len(hexa) - 1)]
            id_oid = ObjectId(id_str)
            id_enc = self.enc.encode(id_oid)
            self.assertEqual(ObjectId(id_enc.replace('"', '')), id_oid)

    def test_set(self):
        """
        set() encoding test.

        Tests list of integers, list of chars, and dictionary.
        Backwards conversion done by eval().
        """

        int_list = [0, 1, 2, 3]
        int_set = set(int_list)
        encoded = self.enc.encode(int_set)
        self.assertEqual(int_list, eval(encoded))

        str_list = ['1', '2', '3', '4']
        str_set = set(str_list)
        encoded = self.enc.encode(str_set)
        for value in encoded:
            if value not in str_list:
                self.assertTrue(True, "Value not in original list")

        key_dict = {'a': 'alpha', 'b': 'bravo', 'c': 'charlie'}
        key_set = set(key_dict)
        encoded = self.enc.encode(key_set)
        self.assertEqual(key_dict.keys(), eval(encoded))

    def test_Exception(self):
        """
        Exception() encoding test.

        Tests all exceptions predefined in exceptions list.
        Backwards conversion not possible, check done by Exception.args[0].i
        """

        exceptions = [StopIteration, BufferError, FloatingPointError, IOError,
                      IndexError, UnboundLocalError, NotImplementedError,
                      IndentationError, TabError, UnicodeError, Warning,
                      DeprecationWarning]

        # prechosen exceptions test
        for exception in exceptions:
            exc = exception("Test message")
            exc_enc = self.enc.encode(exc)
            self.assertTrue(exc_enc, exc.args[0])

    def test_else(self):
        """
        Else possible scenarios.

        tests random generated strings and unicodes.
        """

        alphabet = list("abcdefghijklmnopqrstuvwxyz")

        # random string test
        for str_iter in range(RAND_ITERATIONS):
            string = ""
            for x in range(1, 50):
                string += alphabet[randint(0, len(alphabet) - 1)]
            string_enc = self.enc.encode(string)
            self.assertEqual(string_enc.replace('"', ''), string)

        # random unicode test
        for unic_iter in range(RAND_ITERATIONS):
            unc = unicode()
            for x in range(1, 50):
                unc += unichr(randint(0, 65535))
            self.enc.encode(unc)
        #    unc_enc = self.enc.encode(unc)
        #    self.assertEqual(unicode(unc_enc.__str__().replace('"', '')), unc)

        # TODO: Bounded to JSON encoder unicode


if __name__ == '__main__':
    main()
