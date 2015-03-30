"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import cax_mssql
import cax_oracle


class CaxUpdater(UpdaterBase):
    """The updater class for collections 'cax', 'shares'"""

    def __init__(self, source=None, timeout=30):
        self.source = source
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        if not self.skip_update:
            self.connect_jydb()
            if self.source == 'mssql':
                self.cax_sql = cax_mssql
            elif self.source == 'oracle':
                self.cax_sql = cax_oracle
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update adjusting factors and shares structures for the **same** day before market open."""
        CMD = self.cax_sql.CMD0.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        if date != list(self.cursor)[0][0]:
            self.logger.warning('{} is not a trading day?', date)
            return
        self.db.dates.update({'date': date}, {'date': date}, upsert=True)
        self.dates = self.db.dates.distinct('date')
        self._update(date, self.cax_sql.CMD1, self.cax_sql.dnames1, self.db.cax, float)
        self._update(date, self.cax_sql.CMD2, self.cax_sql.dnames2, self.db.shares, int)

    def _update(self, date, CMD, dnames, col, dtype):
        CMD = CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {} on {}', col.name, date)
            return

        df.columns = ['sid'] + dnames
        df.index = df.sid

        for dname in dnames:
            key = {'dname': dname, 'date': date}
            col.update(key, {'$set': {'dvalue': df[dname].dropna().astype(dtype).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}',
                len(df), col.name, self.db.name, date)

    def monitor(self, date):
        if date not in self.dates:
            return

        self._monitor(date, self.db.cax)
        self._monitor(date, self.db.shares)

    def _monitor(self, date, col):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_{} WHERE trading_day=%s AND data=%s AND statistic=%s".format(col.name)
        SQL2 = "UPDATE mongo_{} SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s".format(col.name)
        SQL3 = "INSERT INTO mongo_{} (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)".format(col.name)

        cursor = self.monitor_connection.cursor()
        for dname in col.distinct('dname'):
            ser = pd.Series(col.find_one({'dname': dname, 'date': date})['dvalue'])
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()

if __name__ == '__main__':
    cax = CaxUpdater()
    cax.run()
