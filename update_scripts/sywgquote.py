"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import sywgquote_mssql
import sywgquote_oracle


class SYWGQuoteUpdater(UpdaterBase):
    """The updater class for collection 'sywgindex_quote'."""

    def __init__(self, source=None, timeout=10):
        self.source = source
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.connect_jydb()
        self.dates = self.db.dates.distinct('date')
        if self.source == 'mssql':
            self.sywgquote_sql = sywgquote_mssql
        elif self.source == 'oracle':
            self.sywgquote_sql = sywgquote_oracle

    def pro_update(self):
        return

        self.logger.debug('Ensuring index date_1_dname_1 on collection sywgindex_quote')
        self.db.sywgindex_quote.ensure_index([('date', 1), ('dname', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index dname_1_date_1 on collection sywgindex_quote')
        self.db.sywgindex_quote.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update SYWG index quote for the **same** day after market close."""
        CMD = self.sywgquote_sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for %s on %s', self.db.sywgindex_quote.name, date)
            return

        df.columns = ['sid'] + self.sywgquote_sql.dnames
        df.index = df.sid

        for dname in self.sywgquote_sql.dnames:
            key = {'dname': dname, 'date': date}
            self.db.sywgindex_quote.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for %d sids into (c: [%s]) of (d: [%s]) on %s',
                len(df), self.db.sywgindex_quote.name, self.db.name, date)

if __name__ == '__main__':
    quote = SYWGQuoteUpdater()
    quote.run()
