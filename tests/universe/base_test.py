"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import logging
import unittest

import pandas as pd

from orca import DATES
from orca.universe import (
        FilterBase,
        SimpleDataFilter)
from orca.universe.rules import avg_gt
from orca.mongo.base import FetcherBase
from orca.mongo.quote import QuoteFetcher
from orca.utils.testing import frames_equal
from orca.utils import dateutil


class FilterBaseDummy(FilterBase):

    def filter(self, startdate, enddate=None, parent=None):
        raise NotImplementedError


class FilterBaseDummyTestCase(unittest.TestCase):

    def setUp(self):
        self.filter = FilterBaseDummy()

    def tearDown(self):
        self.filter = None

    def test_is_abstract_class(self):
        self.assertRaises(TypeError, FilterBase)

    def test_logger_name(self):
        self.assertEqual(self.filter.logger.name, FilterBase.LOGGER_NAME)

    def test_debug_mode_default_on(self):
        self.assertEqual(self.filter.logger.level, logging.DEBUG)

    def test_filter(self):
        self.assertRaises(NotImplementedError, self.filter.filter, '')


data = ('close', QuoteFetcher)
window = 120
rule = avg_gt(2)   # filter out those with average price > 2 within a period

class SimpleDataFilterDummy(SimpleDataFilter):

    def __init__(self):
        SimpleDataFilter.__init__(self, data, window, rule)


class SimpleDataFilterDummyTestCase(unittest.TestCase):

    def setUp(self):
        self.filter = SimpleDataFilterDummy()
        self.fetcher = QuoteFetcher(datetime_index=True, reindex=True)
        self.dates = dateutil.get_startfrom(DATES, '20140801', 20)
        self.startdate, self.enddate = self.dates[0], self.dates[-1]
        self.si, self.ei = map(DATES.index, [self.startdate, self.enddate])

    def tearDown(self):
        self.filter = None

    def test_synth_is_identity(self):
        synth = self.filter.synth
        objs = [None, 1, []]
        self.assertListEqual(objs, [synth(obj) for obj in objs])

    def test_datas_1(self):
        self.assertEqual(len(self.filter.datas), 1)

    def test_datas_2(self):
        self.assertIsInstance(self.filter.datas[0][0], FetcherBase)

    def test_filter_1(self):
        df = self.fetcher.fetch_window('close', DATES[self.si-window: self.ei+1])
        df = pd.rolling_sum(df.fillna(0), window) > 2 * pd.rolling_count(df, window)
        df1 = df.shift(1).iloc[window:].astype(bool)
        df2 = self.filter.filter(self.startdate, self.enddate)
        print 'bm', df1.sum(axis=1)
        self.assertTrue(frames_equal(df1, df2))

    def test_filter_2(self):
        df = self.fetcher.fetch_window('close', DATES[self.si-window: self.ei+1])
        parent = df.notnull()
        df = df.shift(1)
        df[~parent] = None
        df = pd.rolling_sum(df.fillna(0), window) > 2 * pd.rolling_count(df, window)
        df[~parent] = False
        df = df.iloc[window:]
        self.assertTrue(frames_equal(df, self.filter.filter(self.startdate, self.enddate, parent)))
