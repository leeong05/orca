"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import logging
import unittest

import pandas as pd

from orca import (
        DB,
        DATES,
        SIDS
        )
from orca.mongo.base import (
        FetcherBase,
        KDayFetcher,
        KMinFetcher
        )
from orca.utils import dateutil


class FetcherBaseDummy(FetcherBase):

    def fetch(self, dname, startdate, enddate=None, backdays=0, **kwargs):
        raise NotImplementedError

    def fetch_window(self, dname, window, **kwargs):
        raise NotImplementedError

    def fetch_history(self, dname, date, backdays, **kwargs):
        raise NotImplementedError

    def fetch_daily(self, dname, date, offset=0, **kwargs):
        raise NotImplementedError


class FetcherBaseDummyTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = FetcherBaseDummy()

    def tearDown(self):
        self.fetcher = None

    def test_is_abstract_class(self):
        self.assertRaises(TypeError, FetcherBase)

    def test_logger_name(self):
        self.assertEqual(self.fetcher.logger.name, FetcherBase.LOGGER_NAME)

    def test_debug_mode_default_on(self):
        self.assertEqual(self.fetcher.logger.level, logging.DEBUG)

    def test_debug_mode_reset(self):
        self.fetcher.set_debug_mode(False)
        self.assertEqual(self.fetcher.logger.level, logging.INFO)
        # change status back
        self.fetcher.set_debug_mode(True)

    def test_fetch(self):
        self.assertRaises(NotImplementedError, self.fetcher.fetch, '', '')

    def test_fetch_window(self):
        self.assertRaises(NotImplementedError, self.fetcher.fetch_window, '', [])

    def test_fetch_history(self):
        self.assertRaises(NotImplementedError, self.fetcher.fetch_history, '', '', 0)

    def test_fetch_daily(self):
        self.assertRaises(NotImplementedError, self.fetcher.fetch_daily, '', '')


class KDayFetcherDummy(KDayFetcher):

    pass

KDayFetcherDummy.collection = DB.quote


class KDayFetcherDummyTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = KDayFetcherDummy()
        self.dates_str = dateutil.get_startfrom(DATES, '20140101', 50)
        self.dates_pddt = pd.to_datetime(self.dates_str)

    def tearDown(self):
        self.fetcher = None

    def test_fetch_window_datetime_index_true(self):
        df = self.fetcher.fetch_window('close', self.dates_str, datetime_index=True)
        self.assertTrue((df.index == self.dates_pddt).all())

    def test_fetch_window_reindex(self):
        df1 = self.fetcher.fetch_window('close', self.dates_str, reindex=False)
        df2 = self.fetcher.fetch_window('close', self.dates_str, reindex=True)
        self.assertTrue((len(df1.columns) < len(SIDS)) and (list(df2.columns) == SIDS))

    def test_fetch_backdays(self):
        df = self.fetcher.fetch('close', self.dates_str[5], self.dates_str[-1], backdays=5)
        self.assertListEqual(self.dates_str, list(df.index))

    def test_fetch_history_delay(self):
        df = self.fetcher.fetch_history('close', self.dates_str[-1], 45, delay=5)
        self.assertListEqual(self.dates_str[:-5], list(df.index))

    def test_fetch_daily_offset(self):
        ser = self.fetcher.fetch_daily('close', self.dates_str[-1], offset=49)
        self.assertEqual(ser.name, self.dates_str[0])


class KMinFetcherDummy(KMinFetcher):

    pass

KMinFetcherDummy.collection = DB.ts_5min


class KMinFetcherDummyTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = KMinFetcherDummy()
        self.dates_str = dateutil.get_startfrom(DATES, '20140101', 50)
        self.times = list(dateutil.generate_timestamps('100000', '113000', 30*60))
        self.dates_pddt = pd.to_datetime(self.dates_str)

    def tearDown(self):
        self.fetcher = None

    def test_fetch_window_datetime_index_true(self):
        pl = self.fetcher.fetch_window('close', self.times, self.dates_str, datetime_index=True)
        print pl
        self.assertTrue((pl.major_axis == self.dates_pddt).all())

    def test_fetch_window_reindex(self):
        pl1 = self.fetcher.fetch_window('close', self.times, self.dates_str, reindex=False)
        pl2 = self.fetcher.fetch_window('close', self.times, self.dates_str, reindex=True)
        self.assertTrue((len(pl1.minor_axis) < len(SIDS)) and (list(pl2.minor_axis) == SIDS))

    def test_fetch_window_times_notlist(self):
        df = self.fetcher.fetch_window('close', self.times[0], self.dates_str)
        self.assertIsInstance(df, pd.DataFrame)

    def test_fetch_window_times_list(self):
        pl = self.fetcher.fetch_window('close', [self.times[0]], self.dates_str)
        self.assertIsInstance(pl, pd.Panel)

    def test_fetch_backdays(self):
        pl = self.fetcher.fetch('close', self.times, self.dates_str[5], self.dates_str[-1], backdays=5)
        self.assertListEqual(self.dates_str, list(pl.major_axis))

    def test_fetch_history_delay(self):
        pl = self.fetcher.fetch_history('close', self.times, self.dates_str[-1], 45, delay=5)
        self.assertListEqual(self.dates_str[:-5], list(pl.major_axis))

    def test_fetch_daily_offset(self):
        ser = self.fetcher.fetch_daily('close', self.times[0], self.dates_str[-1], offset=49)
        self.assertEqual(ser.name, self.dates_str[0])
