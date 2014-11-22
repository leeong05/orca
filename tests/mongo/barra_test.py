"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

import pandas as pd

from orca import DB
from orca.mongo.barra import (
        BarraFetcher,
        BarraSpecificsFetcher,
        BarraExposureFetcher,
        BarraFactorFetcher,
        BarraCovarianceFetcher,
        )
from orca.utils.testing import (
        frames_equal,
        series_equal,
        )


class BarraFetcherTestCase(unittest.TestCase):

    def test_idmaps1(self):
        barra = BarraFetcher.fetch_idmaps()
        self.assertIsInstance(barra, dict)

    def test_idmaps2(self):
        barra = BarraFetcher.fetch_idmaps()
        sids = BarraFetcher.fetch_idmaps(barra_key=False)
        self.assertDictEqual(barra, {v: k for k, v in sids.iteritems()})


class BarraSpecificsFetcherTestCase(unittest.TestCase):

    def test_model(self):
        barra = BarraSpecificsFetcher('daily')
        barra.model = 'short'
        self.assertEqual(barra.collection, DB.barra_S_specifics)


class BarraExposureFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.barra = BarraExposureFetcher('daily')
        self.dates = self.barra.idmaps.distinct('date')

    def tearDown(self):
        self.barra = None

    def test_daily1(self):
        barra = self.barra.fetch_daily(self.dates[0])
        self.assertSetEqual(set(barra.columns), set(self.barra.all_factors[self.barra.model]))

    def test_daily2(self):
        barra = self.barra.fetch_daily('industry', self.dates[0])
        self.assertSetEqual(set(barra.columns), set(self.barra.industry_factors[self.barra.model]))

    def test_daily3(self):
        barra = self.barra.fetch_daily('style', self.dates[0])
        self.assertSetEqual(set(barra.columns), set(self.barra.style_factors[self.barra.model]))

    def test_daily4(self):
        barra = self.barra.fetch_daily('CNE5D_COUNTRY', self.dates[0])
        self.assertIsInstance(barra, pd.Series)


class BarraFactorFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.barra = BarraFactorFetcher('daily')
        self.dates = self.barra.idmaps.distinct('date')

    def tearDown(self):
        self.barra = None

    def test_fetch_window1(self):
        barra = self.barra.fetch_window(self.dates)
        self.assertSetEqual(set(barra.columns), set(self.barra.factors))

    def test_fetch_window2(self):
        barra = self.barra.fetch_window('industry', self.dates)
        self.assertSetEqual(set(barra.columns), set(self.barra.industry_factors[self.barra.model]))

    def test_fetch_window3(self):
        barra = self.barra.fetch_window(['CNE5D_COUNTRY'], self.dates)
        self.assertListEqual(list(barra.columns), ['CNE5D_COUNTRY'])

    def test_fetch_window4(self):
        barra = self.barra.fetch_window('CNE5D_COUNTRY', self.dates)
        self.assertIsInstance(barra, pd.Series)

    def test_fetch(self):
        barra1_1 = self.barra.fetch(self.dates[0], self.dates[-1])
        barra1_2 = self.barra.fetch_window(self.dates)
        barra2_1 = self.barra.fetch('industry', self.dates[0], self.dates[-1])
        barra2_2 = self.barra.fetch_window('industry', self.dates)
        barra3_1 = self.barra.fetch(['CNE5D_COUNTRY'], self.dates[0], self.dates[-1])
        barra3_2 = self.barra.fetch_window(['CNE5D_COUNTRY'], self.dates)
        barra4_1 = self.barra.fetch('CNE5D_COUNTRY', self.dates[0], self.dates[-1])
        barra4_2 = self.barra.fetch_window('CNE5D_COUNTRY', self.dates)
        print pd.concat([barra4_1, barra4_2], axis=1)
        self.assertTrue(frames_equal(barra1_1, barra1_2)
                      & frames_equal(barra2_1, barra2_2)
                      & frames_equal(barra3_1, barra3_2)
                      & series_equal(barra4_1, barra4_2))

    def test_fetch_history(self):
        barra1_1 = self.barra.fetch_history(self.dates[-1], len(self.dates), delay=0)
        barra1_2 = self.barra.fetch_window(self.dates)
        barra2_1 = self.barra.fetch_history('industry', self.dates[-1], len(self.dates), delay=0)
        barra2_2 = self.barra.fetch_window('industry', self.dates)
        barra3_1 = self.barra.fetch_history(['CNE5D_COUNTRY'], self.dates[-1], len(self.dates), delay=0)
        barra3_2 = self.barra.fetch_window(['CNE5D_COUNTRY'], self.dates)
        barra4_1 = self.barra.fetch_history('CNE5D_COUNTRY', self.dates[-1], len(self.dates), delay=0)
        barra4_2 = self.barra.fetch_window('CNE5D_COUNTRY', self.dates)
        print pd.concat([barra4_1, barra4_2], axis=1)
        self.assertTrue(frames_equal(barra1_1, barra1_2)
                      & frames_equal(barra2_1, barra2_2)
                      & frames_equal(barra3_1, barra3_2)
                      & series_equal(barra4_1, barra4_2))

    def test_fetch_daily1(self):
        cov = self.barra.fetch_daily('covariance', self.dates[0])
        self.assertListEqual(list(cov.index), list(cov.columns))

    def test_fetch_daily2(self):
        ret1 = self.barra.fetch_daily('returns', self.dates[0])
        ret2 = self.barra.fetch_window(self.dates[:2]).iloc[0]
        self.assertTrue(series_equal(ret1, ret2))


class BarraCovarianceFetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.barra = BarraCovarianceFetcher('daily')
        self.dates = self.barra.idmaps.distinct('date')

    def tearDown(self):
        self.barra = None

    def test_fetch(self):
        self.assertRaises(NotImplementedError, self.barra.fetch)

    def test_fetch_window(self):
        self.assertRaises(NotImplementedError, self.barra.fetch_window)

    def test_fetch_history(self):
        self.assertRaises(NotImplementedError, self.barra.fetch_history)

    def test_fetch_daily1(self):
        cov1 = self.barra.fetch_daily(self.dates[0])
        cov2 = self.barra.fetch_daily(self.dates[1], offset=1)
        self.assertTrue(frames_equal(cov1, cov2))

    def test_fetch_daily2(self):
        cov0 = self.barra.fetch_daily(self.dates[0])
        sids = cov0.index[:200]
        cov1 = cov0.ix[sids, sids]
        cov2 = self.barra.fetch_daily(self.dates[0], sids=sids)
        self.assertTrue(frames_equal(cov1, cov2))
