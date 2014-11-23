"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from orca import DATES
from orca.mongo.industry import IndustryFetcher
from orca.utils import dateutil
from orca.utils.testing import frames_equal


class IndustryFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = IndustryFetcher()
        self.dates = dateutil.get_startfrom(DATES, '20140101', 20)

    def tearDown(self):
        self.fetcher = None

    def test_fetch_window1(self):
        df1_1 = self.fetcher.fetch_window('level1', self.dates)
        df1_2 = self.fetcher.fetch_window('sector', self.dates)
        df2_1 = self.fetcher.fetch_window('level2', self.dates)
        df2_2 = self.fetcher.fetch_window('industry', self.dates)
        df3_1 = self.fetcher.fetch_window('level3', self.dates)
        df3_2 = self.fetcher.fetch_window('subindustry', self.dates)
        self.assertTrue(frames_equal(df1_1, df1_2)
                      & frames_equal(df2_1, df2_2)
                      & frames_equal(df3_1, df3_2))

    def test_fetch_window2(self):
        df1 = self.fetcher.fetch_window('level1', self.dates)
        df2 = self.fetcher.fetch_window('level1', self.dates, standard='ZX')
        self.assertFalse(frames_equal(df1, df2))

    def test_fetch_info1(self):
        d0 = self.fetcher.fetch_info('name', level=0)
        d1 = self.fetcher.fetch_info('name', level=1)
        d2 = self.fetcher.fetch_info('name', level=2)
        d3 = self.fetcher.fetch_info('name', level=3)
        self.assertEqual(len(d0), len(d1)+len(d2)+len(d3))

    def test_fetch_info2(self):
        d0 = self.fetcher.fetch_info('index', level=0)
        d1 = self.fetcher.fetch_info('index', level=1)
        d2 = self.fetcher.fetch_info('index', level=2)
        d3 = self.fetcher.fetch_info('index', level=3)
        self.assertEqual(len(d0), len(d1)+len(d2)+len(d3))
