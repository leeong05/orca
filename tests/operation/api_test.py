"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

import numpy as np
import pandas as pd

from orca import (
        DATES,
        SIDS,
        )
from orca.utils.testing import frames_equal
from orca.utils.rand import random_alpha
from orca.operation import api


class APITestCase(unittest.TestCase):

    def test_format(self):
        df1 = pd.DataFrame(np.random.randn(20, 40), index=DATES[:20], columns=SIDS[:40])
        df2 = api.format(df1)
        self.assertTrue((df2.index == pd.to_datetime(df1.index)).all() and \
                (list(df2.columns) == SIDS))

    def test_neutralize(self):
        df = pd.DataFrame(np.random.randn(20, 40))
        df = api.neutralize(df)
        self.assertTrue(np.allclose(df.mean(axis=1), 0))

    def test_scale(self):
        df = pd.DataFrame(np.random.randn(20, 40))
        df = api.scale(df)
        self.assertTrue(np.allclose(np.abs(df).sum(axis=1), 1))

    def test_top(self):
        df = pd.DataFrame(np.random.randn(20, 40))
        top1 = api.top(df, 1)
        maxdf = df.max(axis=1)
        top2 = df.eq(maxdf, axis=0)
        self.assertTrue(frames_equal(top1, top2))

    def test_bottom(self):
        df = pd.DataFrame(np.random.randn(2000, len(SIDS)))
        bot = api.bottom(df, 40)
        self.assertTrue(np.allclose(bot.sum(axis=1), 40))

    def test_qtop(self):
        df = pd.DataFrame(np.random.randn(500, len(SIDS)))
        df[~pd.DataFrame(np.random.randint(4, size=(500, len(SIDS)))).astype(bool)] = None
        top = api.qtop(df, 0.1)
        self.assertTrue(np.allclose(top.sum(axis=1), np.ceil(0.1*df.count(axis=1))))

    def test_qbottom(self):
        df = pd.DataFrame(np.random.randn(50, 500))
        bot1 = api.qbottom(df, 0.01)
        bot2 = api.bottom(df, 5)
        self.assertTrue(frames_equal(bot1, bot2))

    def test_quantiles1(self):
        df = pd.DataFrame(np.random.randn(50, 100))
        qts = api.quantiles(df, 10)
        self.assertTrue(all(np.allclose(qt.sum(axis=1), 10) for qt in qts))

    def test_quantiles2(self):
        df = pd.DataFrame(np.random.randn(50, 100))
        qts = api.quantiles(df, 2)
        top = api.qtop(df, 0.5)
        bot = api.qbottom(df, 0.5)
        self.assertTrue(np.allclose(qts[0], bot) and np.allclose(qts[1], top))

    def test_decay1(self):
        df = pd.DataFrame(
                [[0, 0, np.nan, 1],
                 [1, 0, np.nan, np.nan],
                 [2, 1, np.nan, 1]])
        decay = api.decay(df, 3, dense=False)
        answer = pd.DataFrame(
                [[0, 0, np.nan, 3],
                 [3, 0, np.nan, np.nan],
                 [8, 3, np.nan, 4]])
        self.assertTrue(np.allclose(decay.fillna(0), answer.fillna(0)))

    def test_decay2(self):
        df = pd.DataFrame(
                [[0, 0, np.nan, 1],
                 [1, 0, np.nan, np.nan],
                 [2, 1, np.nan, 1]])
        decay = api.decay(df, 3, dense=True)
        answer = pd.DataFrame(
                [[0, 0, np.nan, 3],
                 [3, 0, np.nan, 2],
                 [8, 3, np.nan, 4]])
        self.assertTrue(np.allclose(decay.fillna(0), answer.fillna(0)))

    def test_industry_neut(self):
        df = random_alpha(n=20)
        neut_df = api.industry_neut(df, 'level1')
        neut_neut_df = api.industry_neut(neut_df, 'level1')
        self.assertTrue(frames_equal(neut_df, neut_neut_df))

    def test_industry_neut1(self):
        df = random_alpha(n=20)
        neut1_df = api.industry_neut(df, 'level2')
        neut2_df = api.industry_neut(neut1_df, 'level1')
        self.assertTrue(frames_equal(neut1_df, neut2_df))

    def test_industry_neut2(self):
        df = random_alpha(n=20)
        neut1_df = api.industry_neut(df, 'level3')
        neut2_df = api.industry_neut(neut1_df, 'level2')
        self.assertTrue(frames_equal(neut1_df, neut2_df))

    def test_barra_neut(self):
        df = random_alpha(n=20)
        neut_df1 = api.barra_neut(df, 'short', 'industry')
        neut_df2 = api.barra_neut(neut_df1, 'short', 'industry')
        self.assertTrue(frames_equal(neut_df1, neut_df2))

    def test_barra_corr_neut(self):
        df = random_alpha(n=20)
        neut_df1 = api.barra_corr_neut(df, 'short', 'industry')
        neut_df2 = api.barra_corr_neut(neut_df1, 'short', 'industry')
        self.assertTrue(frames_equal(neut_df1, neut_df2))
