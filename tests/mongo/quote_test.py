"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from orca.mongo import QuoteFetcher
from orca.utils.testing import (
        series_equal,
        frames_equal)
from orca import DATES


class QuoteFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = QuoteFetcher()
        self.dates = DATES[2000:2050]

    def tearDown(self):
        self.fetcher = None

    def test_fetch_window_classmethod(self):
        df1 = self.fetcher.fetch_window('close', self.dates)
        df2 = QuoteFetcher.fetch_window('close', self.dates)
        self.assertTrue(frames_equal(df1, df2))

    def test_returns_N_eq1_1(self):
        df1 = self.fetcher.fetch('returns', self.dates[5], self.dates[-1], backdays=5)
        df2 = self.fetcher.fetch('returnsN', 1, self.dates[5], self.dates[-1], backdays=5)
        self.assertTrue(frames_equal(df1, df2))

    def test_returns_N_eq1_2(self):
        df1 = self.fetcher.fetch_window('returns', self.dates)
        df2 = self.fetcher.fetch_window('returnsN', 1, self.dates)
        self.assertTrue(frames_equal(df1, df2))

    def test_returns_N_eq1_3(self):
        df1 = self.fetcher.fetch_history('returns', self.dates[-1], 45, delay=5)
        df2 = self.fetcher.fetch_history('returnsN', 1, self.dates[-1], 45, delay=5)
        self.assertTrue(frames_equal(df1, df2))

    def test_returns_N_eq1_4(self):
        s1 = self.fetcher.fetch_daily('returns', self.dates[-1], offset=49)
        s2 = self.fetcher.fetch_daily('returnsN', 1, self.dates[-1], offset=49)
        self.assertTrue(series_equal(s1, s2))

    def test_returns_N_gt1(self):
        df = self.fetcher.fetch_window('returns', self.dates[:2])
        s1 = (df.iloc[0].fillna(0) + 1) * (df.iloc[1] + 1) - 1
        s1.name = self.dates[1]
        s2 = self.fetcher.fetch_daily('returnsN', 2, self.dates[1])
        self.assertTrue(series_equal(s1, s2))
