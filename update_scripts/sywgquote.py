"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
import warnings
warnings.simplefilter(action="ignore", category=pd.core.common.SettingWithCopyWarning)

from base import UpdaterBase
import sywgquote_sql as sql


class SYWGQuoteUpdater(UpdaterBase):
    """The updater class for collection 'sywgindex_quote'."""

    def __init__(self, timeout=3000):
        UpdaterBase.__init__(self, timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.sywgindex_quote
        if not self.skip_update:
            self.connect_wind()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update SYWG index quote for the **same** day after market close."""
        CMD = sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.db.sywgindex_quote.name, date)
            return

        df.columns = ['sid'] + sql.dnames
        for dname in sql.dnames:
            df[dname] = df[dname].astype(float)
        df.index = [sid[:6] for sid in df.sid]
        df['vwap'] = df['amount']/df['volume']
        df['returns'] = (df['close']/df['prev_close']).astype(float) - 1.
        if df['returns'].isnull().sum():
            isnull = df['returns'][df.returns.isnull()]
            pdate = self.dates[self.dates.index(date)-1]
            CMD = sql.CMD.format(date=pdate)
            self.cursor.execute(CMD)
            pdf = pd.DataFrame(list(self.cursor))
            pdf.columns = ['sid'] + sql.dnames
            for dname in sql.dnames:
                pdf[dname] = pdf[dname].astype(float)
            pdf.index = [sid[:6] for sid in pdf.sid]
            for sid in isnull.index:
                if sid in pdf.index:
                    df['prev_close'][sid] = float(pdf['close'][sid])
            df['returns'] = df['close']/df['prev_close'] - 1.

        if df['returns'].isnull().sum():
            isnull = df['returns'][df.returns.isnull()]
            self.logger.warning('Null value found for index {}', list(isnull.index))

        for dname in sql.dnames+['returns', 'vwap']:
            key = {'dname': dname, 'date': date}
            self.collection.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} indice into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.db.sywgindex_quote.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_sywgquote WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_sywgquote SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_sywgquote (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

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
    quote = SYWGQuoteUpdater()
    quote.run()
