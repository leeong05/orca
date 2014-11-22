"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from types import GeneratorType
from datetime import datetime
import unittest

import pandas as pd

from orca.utils import dateutil


class DateutilTestCase(unittest.TestCase):

    dates_int = range(20140101, 20140110)
    dates_str = [str(date) for date in dates_int]
    dates_pddt = pd.to_datetime(dates_str)
    dates_dt = [datetime.strptime(date, '%Y%m%d') for date in dates_str]

    def test_to_datetime_int_input(self):
        res = dateutil.to_pddatetime(DateutilTestCase.dates_int)
        self.assertTrue((DateutilTestCase.dates_pddt == res).all())

    def test_to_datetime_str_input(self):
        res = dateutil.to_pddatetime(DateutilTestCase.dates_str)
        self.assertTrue((DateutilTestCase.dates_pddt == res).all())

    def test_to_datetime_dt_input(self):
        res = dateutil.to_pddatetime(DateutilTestCase.dates_dt)
        self.assertTrue((DateutilTestCase.dates_pddt == res).all())

    def test_to_datestr_int_input(self):
        res = dateutil.to_datestr(DateutilTestCase.dates_int)
        self.assertListEqual(DateutilTestCase.dates_str, res)

    def test_to_datestr_dt_input(self):
        res = dateutil.to_datestr(DateutilTestCase.dates_dt)
        self.assertListEqual(DateutilTestCase.dates_str, res)

    def test_to_datestr_pddt_input(self):
        res = dateutil.to_datestr(DateutilTestCase.dates_pddt)
        self.assertListEqual(DateutilTestCase.dates_str, res)

    def test_to_dateint_str_input(self):
        res = dateutil.to_dateint(DateutilTestCase.dates_int)
        self.assertListEqual(DateutilTestCase.dates_int, res)

    def test_to_dateint_dt_input(self):
        res = dateutil.to_dateint(DateutilTestCase.dates_dt)
        self.assertListEqual(DateutilTestCase.dates_int, res)

    def test_to_dateint_pddt_input(self):
        res = dateutil.to_dateint(DateutilTestCase.dates_pddt)
        self.assertListEqual(DateutilTestCase.dates_int, res)

    def test_is_sorted_positive_ascending(self):
        l = [1, 2, 3]
        self.assertTrue(dateutil.is_sorted(l))

    def test_is_sorted_positive_descending(self):
        l = [1, 2, 3][::-1]
        self.assertTrue(dateutil.is_sorted(l, False))

    def test_is_sorted_negative_ascending(self):
        l = [1, 3, 2]
        self.assertFalse(dateutil.is_sorted(l))

    def test_is_sorted_negative_descending(self):
        l = [1, 3, 2][::-1]
        self.assertFalse(dateutil.is_sorted(l, False))

    def test_find_ge_normal1(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertEqual(dateutil.find_ge(l, 12), (5, 13))

    def test_find_ge_normal2(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertEqual(dateutil.find_ge(l, 13), (5, 13))

    def test_find_ge_exception(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertRaises(ValueError, dateutil.find_ge, l, 20)

    def test_find_le_normal1(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertEqual(dateutil.find_le(l, 12), (4, 11))

    def test_find_le_normal2(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertEqual(dateutil.find_ge(l, 13), (5, 13))

    def test_find_le_exception(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertRaises(ValueError, dateutil.find_le, l, 1)

    def test_compliment_datestring_8l_0(self):
        self.assertEqual(dateutil.compliment_datestring('20141301'), '20141301')

    def test_compliment_datestring_8l_1(self):
        self.assertRaises(ValueError, dateutil.compliment_datestring, '20141301', date_check=True)

    def test_compliment_datestring_6l_0(self):
        self.assertEqual(dateutil.compliment_datestring('201401'), '20140101')

    def test_compliment_datestring_6l_1(self):
        self.assertEqual(dateutil.compliment_datestring('201402', direction=1), '20140231')

    def test_compliment_datestring_6l_2(self):
        self.assertEqual(dateutil.compliment_datestring('201402', direction=1, date_check=True), '20140228')

    def test_compliment_datestring_6l_3(self):
        self.assertEqual(dateutil.compliment_datestring('201413'), '20141301')

    def test_compliment_datestring_6l_4(self):
        self.assertEqual(dateutil.compliment_datestring('201413', direction=1), '20141331')

    def test_compliment_datestring_6l_5(self):
        self.assertRaises(ValueError, dateutil.compliment_datestring, '201413', date_check=True)

    def test_compliment_datestring_4l_0(self):
        self.assertEqual(dateutil.compliment_datestring('2014'), '20140101')

    def test_compliment_datestring_4l_1(self):
        self.assertEqual(dateutil.compliment_datestring('2014', direction=1), '20141231')

    def test_compliment_datestring_xl(self):
        self.assertRaises(ValueError, dateutil.compliment_datestring, '20140')

    def test_cut_window_1(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertEqual(dateutil.cut_window(l, 9, 16, 2), [5, 7, 11, 13])

    def test_cut_window_2(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertEqual(dateutil.cut_window(l, 9, None, 2), [5, 7, 11, 13, 17, 19])

    def test_cut_window_3(self):
        l = [2, 3, 5, 7, 11, 13, 17, 19]
        self.assertRaises(ValueError, dateutil.cut_window, l, 9, None, 5)

    def test_generate_timestamps_1(self):
        a = dateutil.generate_timestamps('093000', '113000', 30*60)
        self.assertIsInstance(a, GeneratorType)

    def test_generate_timestamps_2(self):
        a = dateutil.generate_timestamps('093000', '113000', 30*60)
        self.assertEqual(list(a), ['093000', '100000', '103000', '110000'])

    def test_generate_timestamps_3(self):
        a = dateutil.generate_timestamps('093000', '113000', 30*60)
        a.next()
        self.assertEqual(list(a), ['100000', '103000', '110000'])
