"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

import pandas as pd

from orca import DATES
from orca.mongo.adjquote import AdjQuoteFetcher
from orca.mongo.quote import QuoteFetcher
from orca.utils.dateutil import get_startfrom
from orca.utils.testing import (
        frames_equal,
        series_equal,
        )


class AdjQuoteFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.adjfetcher = AdjQuoteFetcher()
        self.fetcher = QuoteFetcher()
        self.dates = get_startfrom(DATES, '20140101', 50)

    def tearDown(self):
        self.adjfetcher = None
        self.fetcher = None

    def test_noadjust(self):
        ret1 = self.adjfetcher.fetch_window('adj_returns', self.dates)
        ret2 = self.fetcher.fetch_window('returns', self.dates)
        amt1 = self.adjfetcher.fetch_window('adj_amount', self.dates)
        amt2 = self.fetcher.fetch_window('amount', self.dates)
        self.assertTrue(frames_equal(ret1, ret2) & frames_equal(amt1, amt2))

    def test_price1(self):
        cls1 = self.adjfetcher.fetch_window('adj_close', self.dates)
        cls2 = self.fetcher.fetch_window('close', self.dates)
        self.assertTrue((cls1.index == cls2.index).all())

    def test_price2(self):
        cls1 = self.adjfetcher.fetch_window('adj_close', self.dates)
        cls2 = self.fetcher.fetch_window('close', self.dates)
        self.assertTrue(series_equal(cls1.iloc[-1], cls2.iloc[-1]))

    def test_price3(self):
        self.adjfetcher.mode = AdjQuoteFetcher.FORWARD
        cls1 = self.adjfetcher.fetch_window('adj_close', self.dates)
        cls2 = self.fetcher.fetch_window('close', self.dates)
        self.adjfetcher.mode = AdjQuoteFetcher.BACKWARD
        self.assertTrue(series_equal(cls1.iloc[0], cls2.iloc[0]))

    def test_price4(self):
        cls1 = self.adjfetcher.fetch_window('adj_close', self.dates)
        cls2 = self.fetcher.fetch_window('close', self.dates)
        self.assertTrue(frames_equal(cls1.notnull(), cls2.notnull()))

    def test_price5(self):
        cls1 = self.adjfetcher.fetch_window('adj_close', self.dates).fillna(0)
        cls2 = self.fetcher.fetch_window('close', self.dates).fillna(0)
        print pd.concat([cls1['000002'], cls2['000002'], cls1['000002'] <= cls2['000002']], axis=1)
        print self.adjfetcher.cax.fetch_window('adjfactor', self.dates)['000002']
        self.assertTrue((cls1 <= cls2+0.01).all().all())

    def test_volume1(self):
        vol1 = self.adjfetcher.fetch_window('adj_volume', self.dates)
        vol2 = self.fetcher.fetch_window('volume', self.dates)
        self.assertTrue((vol1.index == vol2.index).all())

    def test_volume2(self):
        vol1 = self.adjfetcher.fetch_window('adj_volume', self.dates)
        vol2 = self.fetcher.fetch_window('volume', self.dates)
        self.assertTrue(series_equal(vol1.iloc[-1], vol2.iloc[-1]))

    def test_volume3(self):
        self.adjfetcher.mode = AdjQuoteFetcher.FORWARD
        vol1 = self.adjfetcher.fetch_window('adj_volume', self.dates)
        vol2 = self.fetcher.fetch_window('volume', self.dates)
        self.adjfetcher.mode = AdjQuoteFetcher.BACKWARD
        self.assertTrue(series_equal(vol1.iloc[0], vol2.iloc[0]))

    def test_volume4(self):
        vol1 = self.adjfetcher.fetch_window('adj_volume', self.dates)
        vol2 = self.fetcher.fetch_window('volume', self.dates)
        self.assertTrue(frames_equal(vol1.notnull(), vol2.notnull()))

    def test_volume5(self):
        vol1 = self.adjfetcher.fetch_window('adj_volume', self.dates).fillna(0)
        vol2 = self.fetcher.fetch_window('volume', self.dates).fillna(0)
        self.assertTrue((vol1+1 >= vol2).all().all())

    def test_history1(self):
        cls = self.adjfetcher.fetch_history('adj_close', self.dates[-1], len(self.dates)-1)
        self.assertListEqual(list(cls.index), self.dates[:-1])

    def test_history2(self):
        cls = self.adjfetcher.fetch_history('adj_close', self.dates[-1], len(self.dates), delay=0)
        self.assertListEqual(list(cls.index), self.dates)

    def test_history3(self):
        cls1 = self.adjfetcher.fetch_history('adj_close', self.dates[-1], len(self.dates)-1)
        cls2 = self.adjfetcher.fetch_window('adj_close', self.dates[:-1], self.dates[-1])
        self.assertTrue(frames_equal(cls1, cls2))

    def test_adjust(self):
        self.assertRaises(ValueError, self.adjfetcher.fetch_window, 'adj_close', self.dates, self.dates[2])
