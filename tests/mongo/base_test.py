"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import logging
import unittest

from orca.mongo import (
        FetcherBase,
        KDayFetcher,
        KMinFetcher)


class FetcherBaseDummy(FetcherBase):

    def fetch(self, dname, startdate, enddate=None, **kwargs):
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

