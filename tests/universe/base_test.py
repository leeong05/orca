"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import logging
from functools import partial
import unittest

import pandas as pd

from orca import DATES
from orca.universe import (
        FilterBase,
        SimpleDataFilter)
from orca.universe.rules import mean_gt
from orca.mongo import QuoteFetcher
from orca.utils.testing import frames_equal

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
rule = mean_gt(2)   # filter out those with average price > 2 within a period

class SimpleDataFilterDummy(SimpleDataFilter):

    def __init__(self):
        SimpleDataFilter.__init__(self, data, window, rule)


class SimpleDataFilterDummyTestCase(unittest.TestCase):

    def setUp(self):
        self.filter = SimpleDataFilterDummy()
        self.startdate, self.enddate = DATES[2000], DATES[2049]

    def tearDown(self):
        self.filter = None

    def test_synth_is_identity(self):
        synth = self.filter.synth
        objs = [None, 1, []]
        self.assertListEqual(objs, [synth(obj) for obj in objs])

    def test_datas_1(self):
        self.assertEqual(len(self.filter.datas), 1)

    def test_datas_2(self):
        self.assertIsInstance(self.filter.datas[0], partial)

    def test_filter_1(self):
        df = QuoteFetcher.fetch_window('close', DATES[2000-window: 2050])
        df = (pd.rolling_mean(df, window) > 2).shift(1)
        df = df.iloc[window:].astype(bool)
        self.assertTrue(frames_equal(df, self.filter.filter(self.startdate, self.enddate)))

    def test_filter_2(self):
        df = QuoteFetcher.fetch_window('close', DATES[2000-window: 2050])
        parent = ~df.isnull()
        df = df.shift(1)
        df[~parent] = None
        df = (pd.rolling_mean(df, window) > 2)
        df[~parent] = False
        df = df.iloc[window:]
        self.assertTrue(frames_equal(df, self.filter.filter(self.startdate, self.enddate, parent)))
