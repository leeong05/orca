"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from datetime import datetime
import unittest

import pandas as pd

from orca.utils import date


class DateTestCase(unittest.TestCase):

    dates_int = range(20140101, 20140110)
    dates_str = [str(date) for date in dates_int]
    dates_pddt = pd.to_datetime(dates_str)
    dates_dt = [datetime.strptime(date, '%Y%m%d') for date in dates_str]

    def test_to_datetime_int_input(self):
        res = date.to_datetime(DateTestCase.dates_int)
        self.assertTrue((DateTestCase.dates_pddt == res).all())

    def test_to_datetime_str_input(self):
        res = date.to_datetime(DateTestCase.dates_str)
        self.assertTrue((DateTestCase.dates_pddt == res).all())

    def test_to_datetime_dt_input(self):
        res = date.to_datetime(DateTestCase.dates_dt)
        self.assertTrue((DateTestCase.dates_pddt == res).all())

    def test_to_datestr_int_input(self):
        res = date.to_datestr(DateTestCase.dates_int)
        self.assertListEqual(DateTestCase.dates_str, res)

    def test_to_datestr_dt_input(self):
        res = date.to_datestr(DateTestCase.dates_dt)
        self.assertListEqual(DateTestCase.dates_str, res)

    def test_to_datestr_pddt_input(self):
        res = date.to_datestr(DateTestCase.dates_pddt)
        self.assertListEqual(DateTestCase.dates_str, res)

    def test_to_dateint_str_input(self):
        res = date.to_dateint(DateTestCase.dates_int)
        self.assertListEqual(DateTestCase.dates_int, res)

    def test_to_dateint_dt_input(self):
        res = date.to_dateint(DateTestCase.dates_dt)
        self.assertListEqual(DateTestCase.dates_int, res)

    def test_to_dateint_pddt_input(self):
        res = date.to_dateint(DateTestCase.dates_pddt)
        self.assertListEqual(DateTestCase.dates_int, res)
