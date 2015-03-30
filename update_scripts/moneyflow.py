"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import moneyflow_mssql


class MoneyflowUpdater(UpdaterBase):
    """The updater class for collection 'moneyflow'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        super(MoneyflowUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.moneyflow
        if not self.skip_update:
            self.connect_wind()
            self.sql = moneyflow_mssql
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update daily moneyflow data for the **same** day after market open."""
        CMD = self.sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.collection.name, date)
            return

        df.columns = ['sid'] + self.sql.dnames
        df = df.ix[[sid[-2:] in ('SH', 'SZ') and len(sid) == 9 and sid[:2] in ('00', '60', '30') for sid in df.sid]]
        df.index = [sid[:6] for sid in df.sid]

        df['buy_amount'] = df['buy_amount_exlarge']+df['buy_amount_large']+df['buy_amount_medium']+df['buy_amount_small']
        df['sell_amount'] = df['sell_amount_exlarge']+df['sell_amount_large']+df['sell_amount_medium']+df['sell_amount_small']
        df['buy_volume'] = df['buy_volume_exlarge']+df['buy_volume_large']+df['buy_volume_medium']+df['buy_volume_small']
        df['sell_volume'] = df['sell_volume_exlarge']+df['sell_volume_large']+df['sell_volume_medium']+df['sell_volume_small']
        df['buy_trades'] = df['buy_trades_exlarge']+df['buy_trades_large']+df['buy_trades_medium']+df['buy_trades_small']
        df['sell_trades'] = df['sell_trades_exlarge']+df['sell_trades_large']+df['sell_trades_medium']+df['sell_trades_small']
        df['volume_diff'] = df['volume_diff_exlarge']+df['volume_diff_large']+df['volume_diff_medium']+df['volume_diff_small']
        df['volume_diff_act'] = df['volume_diff_exlarge_act']+df['volume_diff_large_act']+df['volume_diff_medium_act']+df['volume_diff_small_act']
        df['amount_diff'] = df['amount_diff_exlarge']+df['amount_diff_large']+df['amount_diff_medium']+df['amount_diff_small']
        df['amount_diff_act'] = df['amount_diff_exlarge_act']+df['amount_diff_large_act']+df['amount_diff_medium_act']+df['amount_diff_small_act']

        for dname in self.sql.dnames + ['buy_amount', 'sell_amount', 'buy_volume', 'sell_volume',
                'buy_trades', 'sell_trades', 'volume_diff', 'amount_diff', 'volume_diff_act', 'amount_diff_act']:
            key = {'dname': dname, 'date': date}
            self.collection.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.collection.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_moneyflow WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_moneyflow SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_moneyflow (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

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
    value = MoneyflowUpdater()
    value.run()
