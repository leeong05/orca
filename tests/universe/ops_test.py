"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from orca.utils.testing import series_equal
from orca.universe.ops import (
        ChainFilter,
        NegateFilter,
        UnionFilter)
from orca.universe.special import TickerFilter
from orca.universe.rules import startswith

SZ = TickerFilter(rule=startswith(['00', '30']))
CYB = TickerFilter(rule=startswith('30'))
ZXB = TickerFilter(rule=startswith('002'))
nCYB = TickerFilter(rule=startswith('00'))

date = '20141017'

class ChainFilterTestCase(unittest.TestCase):

    def test_sequence(self):
        u1 = ChainFilter([SZ, CYB]).filter_daily(date)
        u2 = CYB.filter_daily(date)
        self.assertTrue(series_equal(u1, u2))

    def test_intersect1(self):
        u1 = ChainFilter([(SZ, CYB)]).filter_daily(date)
        u2 = CYB.filter_daily(date)
        self.assertTrue(series_equal(u1, u2))

    def test_intersect2(self):
        u = ChainFilter([(CYB, ZXB)]).filter_daily(date)
        self.assertEqual(u.sum(), 0)


class NegateFilterTestCase(unittest.TestCase):

    def test_negate(self):
        u0 = SZ.filter_daily(date)
        u1 = NegateFilter(CYB).filter_daily(date)
        u1 &= u0
        u2 = nCYB.filter_daily(date)
        self.assertTrue(series_equal(u1, u2))


class UnionFilterTestCase(unittest.TestCase):

    def test_union(self):
        u1 = SZ.filter_daily(date)
        u2 = UnionFilter([CYB, nCYB]).filter_daily(date)
        self.assertTrue(series_equal(u1, u2))


class CompositeTestCase(unittest.TestCase):

    def test_composite(self):
        u0 = SZ.filter_daily(date)
        u1 = CYB.filter_daily(date)
        u2 = ZXB.filter_daily(date)
        big1 = u0 & ~(u1 | u2)
        big2 =ChainFilter([(nCYB, NegateFilter(UnionFilter([CYB, ZXB])))]).filter_daily(date)
        self.assertTrue(series_equal(big1, big2))
