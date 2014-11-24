"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from orca.mongo.interval import IntervalFetcher


class IntervalFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = IntervalFetcher('5min')

    def tearDown(self):
        self.fetcher = None

    def test_freq_property1(self):
        self.fetcher.freq = '6min'
        self.assertEqual(self.fetcher.freq, '5min')

    def test_freq_property2(self):
        self.fetcher.freq = '1min'
        self.assertEqual(self.fetcher.freq, '1min')
