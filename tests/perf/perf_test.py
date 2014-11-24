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
from orca.utils.testing import (
        series_equal,
        frames_equal,
        )
from orca.utils import dateutil
from orca.perf.analyser import Analyser
from orca.perf.performance import Performance

dates = pd.to_datetime(dateutil.get_startfrom(DATES, '20140101', 120))
alpha1 = pd.DataFrame(np.random.randn(len(dates), len(SIDS)), index=dates, columns=SIDS)
valid = pd.DataFrame(np.random.randint(3, size=alpha1.shape), index=dates, columns=SIDS).astype(bool)
alpha1[~valid] = np.nan
# long-short alpha
alpha1 = alpha1.subtract(alpha1.mean(axis=1), axis=0)

valid = pd.DataFrame(np.random.randint(4, size=alpha1.shape), index=dates, columns=SIDS).astype(bool)
data = pd.DataFrame(np.random.randn(*alpha1.shape), index=dates, columns=SIDS)
data[~valid] = np.nan

# long part
alpha2 = alpha1[alpha1>0]
# short part
alpha3 = -alpha1[alpha1<0]


class PerfTestCase(unittest.TestCase):

    def setUp(self):
        self.alpha1 = Analyser(alpha1, data=data)
        self.alpha2 = Analyser(alpha2, data=data)
        self.alpha3 = Analyser(alpha3, data=data)
        self.perf = Performance(alpha1)
        self.perf.set_returns(data)

    def tearDown(self):
        self.alpha1 = None
        self.alpha2 = None
        self.alpha3 = None
        self.perf = None

    def test_get1(self):
        self.assertTrue(frames_equal(self.perf.get_long().alpha, self.alpha2.alpha)
                and frames_equal(self.perf.get_short().alpha, self.alpha3.alpha))

    def test_get_bms(self):
        b, m, s = self.perf.get_bms()
        b, m, s = b.alpha, m.alpha, s.alpha
        self.assertTrue(series_equal(self.perf.alpha.count(axis=1),
            b.count(axis=1)+m.count(axis=1)+s.count(axis=1)))

    def test_init_1(self):
        self.assertTrue(np.allclose(np.abs(self.alpha1.alpha).sum(axis=1), 1))

    def test_init_2(self):
        self.assertTrue(np.allclose(np.abs(self.alpha2.alpha).sum(axis=1), 1))

    def test_returns1(self):
        ret1 = self.alpha1.get_returns(cost=0)
        ret2 = self.alpha2.get_returns(cost=0)
        ret3 = self.alpha3.get_returns(cost=0)
        self.assertTrue(np.allclose(ret1 * 2, ret2 - ret3))

    def test_returns2(self):
        ret1 = self.alpha1.get_returns(cost=0)
        self.assertTrue((ret1.index == dates[1:]).all())

    def test_turnover(self):
        # |((l1-s1)-(l2-s2))| = |(l1-l2) - (s1-s2)| <= |l1-l2| + |s1-s2|
        tvr1 = self.alpha1.get_turnover()
        tvr2 = self.alpha2.get_turnover()
        tvr3 = self.alpha3.get_turnover()
        self.assertTrue(np.allclose(tvr1 * 2, tvr2 + tvr3))

    def test_ac(self):
        ac1 = self.alpha1.get_ac()
        ac2 = self.alpha1.alpha.corrwith(self.alpha1.alpha.shift(1), axis=1).iloc[1:]
        self.assertFalse(np.allclose(ac1, ac2))

    def test_ic(self):
        ic1 = self.alpha1.get_ic()
        ic2 = data.corrwith(self.alpha1.alpha.shift(1), axis=1).iloc[1:]
        self.assertTrue(series_equal(ic1, ic2))

    def test_summary_ir1(self):
        ir = self.alpha1.summary_ir()
        self.assertListEqual(['days', 'IR1', 'rIR1'], list(ir.index))

    def test_summary_ir2(self):
        ir = self.alpha1.summary_ir(by='A')
        self.assertEqual(len(ir.columns), 1)

    def test_summary_turnover(self):
        tvr = self.alpha1.summary_turnover(freq='weekly')
        self.assertListEqual(['turnover', 'AC1', 'rAC1', 'AC5', 'rAC5'], list(tvr.index))

    def test_summary_returns(self):
        self.alpha1.summary_returns(cost=0.001)
        self.assertTrue(True)

    def test_summary(self):
        ir1 = self.alpha1.summary(group='ir', by='A')
        ir2 = self.alpha1.summary_ir(by='A')
        self.assertTrue(frames_equal(ir1, ir2))
