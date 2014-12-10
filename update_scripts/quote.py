"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import quote_mssql
import quote_oracle


class QuoteUpdater(UpdaterBase):
    """The updater class for collection 'quote'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        super(QuoteUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.connect_jydb()
        self.dates = self.db.dates.distinct('date')
        if self.source == 'mssql':
            self.quote_sql = quote_mssql
        elif self.source == 'oracle':
            self.quote_sql = quote_oracle

    def pro_update(self):
        return

        self.logger.debug('Ensuring index sid_1 on collection sids')
        self.db.sids.ensure_index([('sid', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index dname_1_date_1 on collection quote')
        self.db.quote.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update daily quote data for the **same** day after market open."""
        CMD = self.quote_sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.db.sywgindex_quote.name, date)
            return

        df.columns = ['sid'] + self.quote_sql.dnames
        df.index = df.sid

        new_sids = set(df.sid) - set(self.db.sids.distinct('sid'))
        if len(new_sids):
            self.logger.info('Found {} new sids', len(new_sids))
            for sid in new_sids:
                self.db.sids.update({'sid': sid}, {'sid': sid}, upsert=True)

        di = self.dates.index(date)
        if di >= 1:
            prev_date = self.dates[di-1]
            prev_fclose = self.db.quote.find_one({'dname': 'fclose', 'date': prev_date}, {'_id': 0, 'dvalue': 1})['dvalue']
            nsids = set(prev_fclose.keys()) - set(df.index)
        else:
            prev_date = None
            prev_fclose = {}
            nsids = {}
        fclose = df.close.dropna().astype(float).to_dict()
        for sid in nsids:
            fclose[sid] = prev_fclose[sid]

        for dname in self.quote_sql.dnames:
            key = {'dname': dname, 'date': date}
            self.db.quote.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.db.quote.update({'dname': 'fclose', 'date': date}, {'$set': {'dvalue': fclose}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.db.quote.name, self.db.name, date)

if __name__ == '__main__':
    quote = QuoteUpdater()
    quote.run()
