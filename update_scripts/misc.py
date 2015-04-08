"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca.universe import common

from base import UpdaterBase
import misc_mssql as sql


class MiscUpdater(UpdaterBase):
    """The updater class for collection 'misc'."""

    def __init__(self, timeout=3000):
        super(MiscUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.misc
        if not self.skip_update:
            self.connect_wind()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update_universe(self, date, univ_name, univ_filter):
        univ = univ_filter.filter_daily(date)
        univ = univ[univ].astype(int)
        self.db.universe.update({'dname': univ_name, 'date': date}, {'$set': {'dvalue': univ.to_dict()}}, upsert=True)

    def update(self, date):
        self.update_tradable(date)

    def update_tradable(self, date):
        """Update daily tradable data for the **same** day before market open."""
        pdate = self.dates[self.dates.index(date)-1]
        CMD = sql.CMD0.format(date=pdate)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        sids = [sid[0] for sid in self.cursor]

        CMD = sql.CMD1.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        sids1 = [sid[0] for sid in self.cursor]
        CMD = sql.CMD2.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        sids2 = [sid[0] for sid in self.cursor]

        tradable = {sid: 1 for sid in sids if sid not in sids1 and sid not in sids2}

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

    def monitor(self, date):
        self.monitor_tradable(date)

    def monitor_tradable(self, date):
        statistics = ('count',)
        SQL1 = "SELECT * FROM mongo_universe WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_universe SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_universe (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.db.universe.distinct('dname'):
            try:
                ser = pd.Series(self.db.universe.find_one({'dname': dname, 'date': date})['dvalue'])
            except:
                continue
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    misc = MiscUpdater()
    misc.run()
