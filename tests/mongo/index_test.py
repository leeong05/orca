"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

import pandas as pd

from orca import DATES
from orca.mongo.index import IndexQuoteFetcher
from orca.utils import dateutil


class IndexQuoteFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = IndexQuoteFetcher()
        self.dates = dateutil.get_startfrom(DATES, '20140101', 20)

    def tearDown(self):
        self.fetcher = None

    def test_fetch_window1(self):
        close = self.fetcher.fetch_window('close', self.dates, index='HS300')
        self.assertIsInstance(close, pd.Series)

    def test_fetch_window2(self):
        close = self.fetcher.fetch_window(['close'], self.dates, index='HS300')
        self.assertIsInstance(close, pd.DataFrame)
