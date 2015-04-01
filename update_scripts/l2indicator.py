"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import l2indicator_mssql as sql


class L2IndicatorUpdater(UpdaterBase):
    """The updater class for collection 'l2indicator'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        super(L2IndicatorUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.l2indicator
        if not self.skip_update:
            self.connect_wind()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update daily l2indicator data for the **same** day after market open."""
        CMD = sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.collection.name, date)
            return

        df.columns = ['sid'] + sql.dnames
        df = df.ix[[sid[-2:] in ('SH', 'SZ') and len(sid) == 9 and sid[:2] in ('00', '60', '30') for sid in df.sid]]
        df.index = [sid[:6] for sid in df.sid]

        for dname in sql.dnames:
            key = {'dname': dname, 'date': date}
            self.collection.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.collection.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_l2indicator WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_l2indicator SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_l2indicator (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distinct('dname'):
            ser = pd.Series(self.collection.find_one({'dname': dname, 'date': date})['dvalue'])
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    value = L2IndicatorUpdater()
    value.run()
