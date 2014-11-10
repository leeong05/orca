"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from orca import DATES, SIDS
from orca.mongo.quote import QuoteFetcher
from orca.mongo.components import ComponentsFetcher
from orca.mongo.industry import IndustryFetcher
from orca.utils.testing import (
        series_equal,
        frames_equal,
        )
from orca.universe.special import (
        TickerFilter,
        TradingDaysFilter,
        ActiveFilter,
        ComponentsFilter,
        IndustryFilter,
        )
from orca.universe.rules import (
        startswith,
        count_gt,
        )

window = DATES[2000: 2050]
close = QuoteFetcher.fetch_window('close', window, datetime_index=True, reindex=True)
hs300 = ComponentsFetcher.fetch_window('HS300', window, datetime_index=True, reindex=True)
sector = IndustryFetcher.fetch_window('sector', window, datetime_index=True, reindex=True)


class SpecialTestCase(unittest.TestCase):

    def test_ticker_filter(self):
        sh = TickerFilter(startswith('60')).filter_daily(window[0])
        dct = {sid: sid[:2] == '60' for sid in SIDS}
        self.assertEqual(sh.to_dict(), dct)

    def test_trading_days_filter(self):
        trd_filter = TradingDaysFilter(50, count_gt(40), delay=0)
        trd1 = trd_filter.filter_daily(window[-1])
        trd2 = close.count() > 40
        self.assertTrue(series_equal(trd1, trd2))

    def test_active_filter(self):
        active1 = ActiveFilter().filter(window[0], window[-1])
        active2 = ~close.isnull()
        self.assertTrue(frames_equal(active1, active2))

    def test_components_filter(self):
        HS300 = ComponentsFilter('HS300').filter(window[0], window[-1])
        self.assertTrue(frames_equal(hs300, HS300))

    def test_industry_filter(self):
        finance = IndustryFilter(['480000', '490000']).filter(window[0], window[-1])
        self.assertTrue(frames_equal(sector.isin(['480000', '490000']), finance))
