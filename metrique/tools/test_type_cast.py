#!/usr/bin/env python
# Author:  Jan Grec <jgrec@redhat.com>

from datetime import datetime
from time import time
from type_cast import type_cast
from unittest import TestCase, main

from metrique.tools.constants import UTC

tester_type_cast = lambda raw, cast_type: type_cast(raw, cast_type)


class TestArgs(TestCase):

    def setUp(self):
        pass

    def test_invalid_cast_type(self):
        """Invalid cast_types and raw_types tests"""

        # Undefined cast type
        self.assertRaises(TypeError, tester_type_cast,
                          raw="123456", cast_type="UNDEFINED")

        # Dictionaries not supported
        self.assertRaises(TypeError, tester_type_cast,
                          raw={'key': 'value'}, cast_type=None)


class TestFloat(TestCase):
    """Class testing typecasting to float"""

    def setUp(self):
        pass

    def test_none_to_float(self):
        self.assertEqual(None, type_cast(None, float))

    def test_float_to_float(self):
        self.assertEqual(10.0, type_cast(10, float))
        self.assertEqual(-10.0, type_cast(-10, float))
        self.assertEqual(3.14159, type_cast(3.14159, float))
        self.assertEqual(-3.14159, type_cast(-3.14159, float))
        self.assertEqual([1.5, 2.5, 3.5], type_cast([1.5, 2.5, 3.5], float))
        self.assertEqual([1.5, 2.5, 3.5], type_cast((1.5, 2.5, 3.5), float))

    def test_integer_to_float(self):
        self.assertEqual(10.0, type_cast(10, float))
        self.assertEqual(-10.0, type_cast(-10, float))
        self.assertEqual([1.5, 2.5, 3.5], type_cast([1.5, 2.5, 3.5], float))
        self.assertEqual([1.0, 2.0, 3.0], type_cast((1, 2, 3), float))

    def test_boolean_to_float(self):
        self.assertEqual(1.0, type_cast(True, float))
        self.assertEqual(0.0, type_cast(False, float))

    def test_datetime_to_float(self):
        self.assertRaises(TypeError, tester_type_cast,
                          raw=datetime(2013, 2, 19), cast_type=float)
        self.assertRaises(TypeError, tester_type_cast,
                          raw=datetime(2013, 2, 19, 22, 4), cast_type=float)
        self.assertRaises(TypeError, tester_type_cast,
                          raw="2013-02-19", cast_type=float)
        self.assertRaises(TypeError, tester_type_cast,
                          raw="2013-02-19 22:04", cast_type=float)
        self.assertRaises(TypeError, tester_type_cast,
                          raw=unicode("2013-02-19"), cast_type=float)
        self.assertRaises(TypeError, tester_type_cast,
                          raw=unicode("2013-02-19 22:04"), cast_type=float)

    def test_unicode_to_float(self):
        self.assertEqual(10.0, type_cast(unicode(10), float))
        self.assertEqual(-10.0, type_cast(unicode(-10), float))
        self.assertEqual(10.9876, type_cast(unicode(10.9876), float))
        self.assertEqual(-10.9876, type_cast(unicode(-10.9876), float))

        self.assertRaises(TypeError, tester_type_cast,
                          raw=unicode("holahoj"), cast_type=float)

    def test_string_to_float(self):
        self.assertEqual(10.0, type_cast("10", float))
        self.assertEqual(-10.0, type_cast("-10", float))
        self.assertEqual(10.1234, type_cast("10.1234", float))
        self.assertEqual(-10.1234, type_cast("-10.1234", float))

        self.assertRaises(TypeError, tester_type_cast,
                          raw="holahoj", cast_type=float)


class TestDatetime(TestCase):
    """Class testing typecasting to datetime"""

    def setUp(self):
        self.ts = time()

    def test_none_to_date(self):
        self.assertEqual(None, type_cast(None, datetime))

    # NOTE: 300000000000 would create a date larger than 9999-12-31
    # which python's datetime doesn't accept.
    def test_float_to_date(self):
        self.assertEqual(datetime.fromtimestamp(self.ts, UTC),
                         type_cast(self.ts, datetime))
        self.assertRaises(TypeError, tester_type_cast,
                          raw=300000000000.0, cast_type=datetime)

    def test_integer_to_date(self):
        self.assertEqual(datetime.fromtimestamp(int(self.ts), UTC),
                         type_cast(int(self.ts), datetime))
        self.assertRaises(TypeError, tester_type_cast,
                          raw=300000000000, cast_type=datetime)

    def test_long_to_date(self):
        self.assertEqual(datetime.fromtimestamp(long(self.ts), UTC),
                         type_cast(long(self.ts), datetime))
        self.assertRaises(TypeError, tester_type_cast,
                          raw=long(300000000000), cast_type=datetime)

    def test_boolean_to_date(self):
        self.assertRaises(TypeError, tester_type_cast,
                          raw=True, cast_type=datetime)
        self.assertRaises(TypeError, tester_type_cast,
                          raw=False, cast_type=datetime)

    def test_datetime_to_date(self):
        dt_utc = datetime.fromtimestamp(self.ts, UTC)
        casted_utc = type_cast(dt_utc, datetime)
        self.assertEqual(dt_utc, casted_utc)
        self.assertEqual(dt_utc.date(), casted_utc.date())
        self.assertEqual(dt_utc.time(), casted_utc.time())

    def test_unicode_to_date(self):
        today = datetime(2013, 2, 19, 22, 4)
        casted = type_cast(unicode("20130219"), datetime)
        self.assertEqual(today.date(), casted.date())

        casted = type_cast(unicode("201302192204"), datetime)
        self.assertEqual(today.date(), casted.date())
        self.assertEqual(today.time(), casted.time())

        # with a T separating date and time
        casted = type_cast(unicode("20130219T2204"), datetime)
        self.assertEqual(today.date(), casted.date())
        self.assertEqual(today.time(), casted.time())

        self.assertRaises(TypeError, tester_type_cast,
                          raw=unicode("201301"), cast_type=datetime)

    def test_string_to_date(self):
        today = datetime(2013, 2, 19, 22, 4)
        casted = type_cast("20130219", datetime)
        self.assertEqual(today.date(), casted.date())

        casted = type_cast("201302192204", datetime)
        self.assertEqual(today.date(), casted.date())
        self.assertEqual(today.time(), casted.time())

        # with a T separating date and time
        casted = type_cast("20130219T2204", datetime)
        self.assertEqual(today.date(), casted.date())
        self.assertEqual(today.time(), casted.time())

        self.assertRaises(TypeError, tester_type_cast,
                          raw=str("201301"), cast_type=datetime)

        self.assertRaises(TypeError, tester_type_cast,
                          raw=str("-20130219"), cats_type=datetime)


class TestUnicode(TestCase):

    def test_none_to_unicode(self):
        self.assertEqual(None, type_cast(None))

    def test_float_to_unicode(self):
        self.assertEqual(unicode("3.14159"), type_cast(float(3.14159)))
        self.assertEqual(unicode("22.0"), type_cast(float(22)))

    def test_integer_to_unicode(self):
        self.assertEqual(unicode("20"), type_cast(20))
        self.assertEqual(unicode("-22"), type_cast(-22))
        self.assertEqual(unicode("0"), type_cast(0))

    def test_boolean_to_unicode(self):
        self.assertEqual(unicode("True"), type_cast(True))
        self.assertEqual(unicode("False"), type_cast(False))

    def test_datetime_to_unicode(self):
        today = datetime(2013, 2, 19, 22, 4)
        casted = type_cast(today)
        self.assertEqual(casted, unicode("2013-02-19 22:04:00"))

        today = datetime(2013, 2, 19)
        casted = type_cast(today)
        self.assertEqual(casted, unicode("2013-02-19 00:00:00"))

    def test_unicode_to_unicode(self):
        self.assertEqual(unicode("blahblah.com"), type_cast(unicode("blahblah.com")))

    def test_string_to_unicode(self):
        self.assertEqual(unicode("blahblah.com"), type_cast("blahblah.com"))

if __name__ == '__main__':
    main()
