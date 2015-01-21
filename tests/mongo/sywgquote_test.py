"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from orca import DATES
from orca.mongo.sywgquote import SYWGQuoteFetcher
from orca.utils import dateutil


class SYWGQuoteFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = SYWGQuoteFetcher()
        self.dates = dateutil.get_startfrom(DATES, '20140101', 20)

    def tearDown(self):
        self.fetcher = None

    def test_fetch_window1(self):
        close = self.fetcher.fetch_window('close', self.dates)
        sector = self.fetcher.fetcher.fetch_info('name', level=1, date=self.dates[-1]).keys()
        self.assertSetEqual(set(close.columns), set(sector))

    def test_fetch_window2(self):
        close = self.fetcher.fetch_window('close', self.dates, use_industry=False)
        indice = self.fetcher.fetcher.fetch_info('index', level=1, date=self.dates[-1]).values()
        self.assertSetEqual(set(close.columns), set(indice))

    def test_fetch_window3(self):
        close = self.fetcher.fetch_window('close', self.dates, level=2)
        industry = self.fetcher.fetcher.fetch_info('name', level=2, date=self.dates[-1]).keys()
        self.assertSetEqual(set(close.columns), set(industry))

    def test_fetch_window4(self):
        close = self.fetcher.fetch_window('close', self.dates, level=2, use_industry=False)
        indice = self.fetcher.fetcher.fetch_info('index', level=2, date=self.dates[-1]).values()
        self.assertSetEqual(set(close.columns), set(indice))
