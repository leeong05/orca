"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import indexquote_mssql


class IndexQuoteUpdater(UpdaterBase):
    """The updater class for collection 'index_quote'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.connect_wind()
        self.sql = indexquote_mssql

    def pro_update(self):
        return

        self.logger.debug('Ensuring index index_1_date_1 on collection index_quote')
        self.db.index_quote.ensure_index([('index', 1), ('date', 1)], background=True)

    def update(self, date):
        """Update index quote for the **same** day after market close."""
        CMD = self.sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.db.index_quote.name, date)
            return

        df.columns = ['sid'] + self.sql.dnames
        df = df.ix[[sid[-2:] in ('SH', 'SZ') and len(sid) == 9 for sid in df.sid]]
        df.index = [sid[-2:]+sid[:6] for sid in df.sid]
        for dname in self.sql.dnames:
            df[dname] = df[dname].astype(float)
        df['vwap'] = df['amount']/df['volume']
        df['returns'] = df['close']/df['prev_close'] - 1.

        for _, row in df.iterrows():
            key = {'index': row.name, 'date': date}
            res = {}
            for dname in self.sql.dnames+['vwap', 'returns']:
                res[dname] = row[dname]
            self.db.index_quote.update(key, {'$set': res}, upsert=True)
        self.logger.info('UPSERT documents for {} indice into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.db.index_quote.name, self.db.name, date)


if __name__ == '__main__':
    quote = IndexQuoteUpdater()
    quote.run()
