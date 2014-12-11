"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from collections import OrderedDict

#import pandas as pd

from orca.universe import common
from orca.universe import rules

from base import UpdaterBase


class UnivUpdater(UpdaterBase):
    """The updater class for collection 'universe'."""

    def __init__(self, source=None, timeout=900):
        self.source = source
        self.univs = OrderedDict()
        super(UnivUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.universe

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection quote')
        self.db.quote.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update_universe(self, date, univ_name, univ_filter):
        univ = univ_filter.filter_daily(date)
        univ = univ[univ].astype(int)
        self.univs[univ_name] = len(univ)
        self.collection.update({'dname': univ_name, 'date': date}, {'$set': {'dvalue': univ.to_dict()}}, upsert=True)

    def update(self, date):
        """Update universe data for the **same** day before market open."""
        self.update_universe(date, 'SH', common.SH)
        self.update_universe(date, 'SZ', common.SZ)
        self.update_universe(date, 'CYB', common.CYB)
        self.update_universe(date, 'ZXB', common.ZXB)

        self.update_universe(date, 'T1Y', common.T1Y)

        self.update_universe(date, 'HS300', common.HS300)
        self.update_universe(date, 'CS500', common.CS500)
        self.update_universe(date, 'CS800', common.CS800)

        self.update_universe(date, 'FINANCE', common.FINANCE)
        self.update_universe(date, 'NONFIN', common.NONFIN)
        self.update_universe(date, 'BANK', common.BANK)
        self.update_universe(date, 'NONBANK', common.NONBANK)

        TotalCap70Q = common.TotalCapFilter(common.DAYS_IN_QUARTER, rules.avg_rank_pct_lte(70))
        TotalCap70S = common.TotalCapFilter(common.DAYS_IN_QUARTER*2, rules.avg_rank_pct_lte(70))
        TotalCap70Y = common.TotalCapFilter(common.DAYS_IN_YEAR, rules.avg_rank_pct_lte(70))
        self.update_universe(date, 'TotalCap70Q', TotalCap70Q)
        self.update_universe(date, 'TotalCap70S', TotalCap70S)
        self.update_universe(date, 'TotalCap70Y', TotalCap70Y)
        FloatCap70Q = common.FloatCapFilter(common.DAYS_IN_QUARTER, rules.avg_rank_pct_lte(70))
        FloatCap70S = common.FloatCapFilter(common.DAYS_IN_QUARTER*2, rules.avg_rank_pct_lte(70))
        FloatCap70Y = common.FloatCapFilter(common.DAYS_IN_YEAR, rules.avg_rank_pct_lte(70))
        self.update_universe(date, 'FloatCap70Q', FloatCap70Q)
        self.update_universe(date, 'FloatCap70S', FloatCap70S)
        self.update_universe(date, 'FloatCap70Y', FloatCap70Y)

        Liq70Q = common.AmountFilter(common.DAYS_IN_QUARTER, rules.sum_rank_pct_lte(70))
        Liq70S = common.AmountFilter(common.DAYS_IN_QUARTER*2, rules.sum_rank_pct_lte(70))
        Liq70Y = common.AmountFilter(common.DAYS_IN_YEAR, rules.sum_rank_pct_lte(70))
        self.update_universe(date, 'Liq70Q', Liq70Q)
        self.update_universe(date, 'Liq70S', Liq70S)
        self.update_universe(date, 'Liq70Y', Liq70Y)

        self.update_universe(date, 'Tradable', common.ACTIVE)
        self.update_universe(date, 'Active', common.ACTIVE)

        Cap70Liq70Q = common.create_topliquid_filter(70, 70, window=common.DAYS_IN_QUARTER)
        Cap70Liq70S = common.create_topliquid_filter(70, 70, window=common.DAYS_IN_QUARTER*2)
        Cap70Liq70Y = common.create_topliquid_filter(70, 70, window=common.DAYS_IN_YEAR)
        self.update_universe(date, 'Cap70Liq70Q', Cap70Liq70Q)
        self.update_universe(date, 'Cap70Liq70S', Cap70Liq70S)
        self.update_universe(date, 'Cap70Liq70Y', Cap70Liq70Y)
        self.update_universe(date, 'TOP70Q', Cap70Liq70Q)
        self.update_universe(date, 'TOP70S', Cap70Liq70S)
        self.update_universe(date, 'TOP70Y', Cap70Liq70Y)

        BCap70Liq70Q = common.create_backtesting_topliquid_filter(70, 70, window=common.DAYS_IN_QUARTER)
        BCap70Liq70S = common.create_backtesting_topliquid_filter(70, 70, window=common.DAYS_IN_QUARTER*2)
        BCap70Liq70Y = common.create_backtesting_topliquid_filter(70, 70, window=common.DAYS_IN_YEAR)
        self.update_universe(date, 'BCap70Liq70Q', BCap70Liq70Q)
        self.update_universe(date, 'BCap70Liq70S', BCap70Liq70S)
        self.update_universe(date, 'BCap70Liq70Y', BCap70Liq70Y)
        self.update_universe(date, 'BTOP70Q', BCap70Liq70Q)
        self.update_universe(date, 'BTOP70S', BCap70Liq70S)
        self.update_universe(date, 'BTOP70Y', BCap70Liq70Y)

        self.logger.info('UPSERT {} universes into (c: [{}]) of (d: [{}]) on {}', len(self.univs), self.collection.name, self.db.name, date)
        self.logger.info('Detailed information:\n{}', self.univs)

if __name__ == '__main__':
    univ = UnivUpdater()
    univ.run()
