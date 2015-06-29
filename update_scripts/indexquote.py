"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import indexquote_sql as sql


class IndexQuoteUpdater(UpdaterBase):
    """The updater class for collection 'index_quote'."""

    def __init__(self, timeout=3000):
        super(IndexQuoteUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.index_quote
        if not self.skip_update:
            self.connect_wind()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update index quote for the **same** day after market close."""
        CMD = sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.collection.name, date)
            return

        df.columns = ['sid'] + sql.dnames
        df = df.ix[[sid[-2:] in ('SH', 'SZ') and len(sid) == 9 for sid in df.sid]]
        df.index = [sid[-2:]+sid[:6] for sid in df.sid]
        for dname in sql.dnames:
            df[dname] = df[dname].astype(float)
        df['vwap'] = df['amount']/df['volume']
        df['returns'] = df['close']/df['prev_close'] - 1.

        for _, row in df.iterrows():
            key = {'index': row.name, 'date': date}
            res = {}
            for dname in sql.dnames+['vwap', 'returns']:
                res[dname] = row[dname]
            self.collection.update(key, {'$set': res}, upsert=True)
        self.logger.info('UPSERT documents for {} indice into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.collection.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_indexquote WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_indexquote SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_indexquote (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        df = pd.DataFrame(list(self.collection.find({'date': date}, {'_id': 0, 'date': 0})))
        df.index = df['index']
        del df['index']

        cursor = self.monitor_connection.cursor()
        for dname, col in df.iteritems():
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(col, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(col, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    quote = IndexQuoteUpdater()
    quote.run()
