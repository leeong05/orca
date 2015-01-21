"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

import numpy as np

from orca import DATES
from orca.mongo.components import ComponentsFetcher
from orca.utils import dateutil


class ComponentsFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = ComponentsFetcher()
        self.dates = dateutil.get_startfrom(DATES, '20140101', 10)

    def tearDown(self):
        self.fetcher = None

    def test_fetch_window1(self):
        hs300 = self.fetcher.fetch_window('HS300', self.dates)
        self.assertTrue((hs300.sum(axis=1) == 300).all())

    def test_fetch_window2(self):
        hs300 = self.fetcher.fetch_window('HS300', self.dates, as_bool=False)
        self.assertTrue((np.abs(hs300.sum(axis=1)-100) <= 1).all())
