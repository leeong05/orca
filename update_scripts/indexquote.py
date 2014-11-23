"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from base import UpdaterBase
import indexquote_mssql
import indexquote_oracle


class IndexQuoteUpdater(UpdaterBase):
    """The updater class for collection 'index_quote'."""

    def __init__(self, source=None, timeout=10):
        self.source = source
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.connect_jydb()
        self.dates = self.db.dates.distinct('date')
        if self.source == 'mssql':
            self.indexquote_sql = indexquote_mssql
        elif self.source == 'oracle':
            self.indexquote_sql = indexquote_oracle

    def pro_update(self):
        return

        self.logger.debug('Ensuring index index_1_date_1 on collection index_quote')
        self.db.index_quote.ensure_index([('index', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update index quote for the **same** day after market close."""
        CMD = self.indexquote_sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for %s on %s', self.db.index_quote.name, date)
            return

        df.columns = ['sid', 'market'] + self.indexquote_sql.dnames
        df.index = ['SH'+sid if mkt == 83 else 'SZ'+sid for mkt, sid in zip(df.market, df.sid)]

        for _, row in df.iterrows():
            key = {'index': row.name, 'date': date}
            res = {}
            for dname in self.indexquote_sql.dnames:
                try:
                    res[dname] = float(str(row[dname]))
                except:
                    res[dname] = np.nan
            self.db.index_quote.update(key, {'$set': res}, upsert=True)
        self.logger.info('UPSERT documents for %d indice into (c: [%s]) of (d: [%s]) on %s',
                len(df), self.db.index_quote.name, self.db.name, date)

if __name__ == '__main__':
    quote = IndexQuoteUpdater()
    quote.run()
