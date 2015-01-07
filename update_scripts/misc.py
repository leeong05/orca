"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

#import pandas as pd

from orca.universe import common

from base import UpdaterBase
import misc_sql


class MiscUpdater(UpdaterBase):
    """The updater class for collection 'misc'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        super(MiscUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.misc

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection quote')
        self.db.quote.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update_universe(self, date, univ_name, univ_filter):
        univ = univ_filter.filter_daily(date)
        univ = univ[univ].astype(int)
        self.db.universe.update({'dname': univ_name, 'date': date}, {'$set': {'dvalue': univ.to_dict()}}, upsert=True)

    def update(self, date):
        self.update_tradable(date)

    def update_tradable(self, date):
        """Update daily tradable data for the **same** day before market open."""
        fpath = misc_sql.gp_tradable(date)
        if not os.path.exists(fpath):
            self.logger.warning('File not exists on {}', date)
            return

        tradable = {}
        with open(fpath) as file:
            for line in file:
                try:
                    sid = line.strip()
                    assert len(sid) == 6 and sid[:2] in ('00', '30', '60')
                    tradable[sid] = 1
                except:
                    pass
        self.collection.update({'dname': 'tradable', 'date': date}, {'$set': {'dvalue': tradable}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname={}]) of (d: [{}]) on {}',
                len(tradable), self.collection.name, 'tradable', self.db.name, date)

        self.update_universe(date, 'Tradable', common.ACTIVE)
        self.update_universe(date, 'Active', common.ACTIVE)

        BCap70Liq70Q = common.create_backtesting_topliquid_filter(70, 70, window=common.DAYS_IN_QUARTER)
        BCap70Liq70S = common.create_backtesting_topliquid_filter(70, 70, window=common.DAYS_IN_QUARTER*2)
        BCap70Liq70Y = common.create_backtesting_topliquid_filter(70, 70, window=common.DAYS_IN_YEAR)
        self.update_universe(date, 'BCap70Liq70Q', BCap70Liq70Q)
        self.update_universe(date, 'BCap70Liq70S', BCap70Liq70S)
        self.update_universe(date, 'BCap70Liq70Y', BCap70Liq70Y)
        self.update_universe(date, 'BTOP70Q', BCap70Liq70Q)
        self.update_universe(date, 'BTOP70S', BCap70Liq70S)
        self.update_universe(date, 'BTOP70Y', BCap70Liq70Y)


if __name__ == '__main__':
    misc = MiscUpdater()
    misc.run()
