"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
import warnings
warnings.simplefilter(action="ignore", category=pd.core.common.SettingWithCopyWarning)

from base import UpdaterBase
import sywgquote_mssql


class SYWGQuoteUpdater(UpdaterBase):
    """The updater class for collection 'sywgindex_quote'."""

    def __init__(self, source=None, timeout=600):
        self.source = source
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.connect_wind()
        self.sywgquote_sql = sywgquote_mssql

    def pro_update(self):
        pass

    def update(self, date):
        """Update SYWG index quote for the **same** day after market close."""
        CMD = self.sywgquote_sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.db.sywgindex_quote.name, date)
            return

        df.columns = ['sid'] + self.sywgquote_sql.dnames
        for dname in self.sywgquote_sql.dnames:
            df[dname] = df[dname].astype(float)
        df['vwap'] = df['amount']/df['volume']
        df['returns'] = (df['close']/df['prev_close']).astype(float) - 1.
        if df['returns'].isnull().sum():
            isnull = df['returns'][df.returns.isnull()]
            pdate = self.dates[self.dates.index(date)-1]
            CMD = self.sywgquote_sql.CMD.format(date=pdate)
            self.cursor.execute(CMD)
            pdf = pd.DataFrame(list(self.cursor))
            pdf.columns = ['sid'] + self.sywgquote_sql.dnames
            for dname in self.sywgquote_sql.dnames:
                pdf[dname] = pdf[dname].astype(float)
            pdf.index = [sid[:6] for sid in pdf.sid]
            for sid in isnull.index:
                if sid in pdf.index:
                    df['prev_close'][sid] = float(pdf['close'][sid])
            df['returns'] = df['close']/df['prev_close'] - 1.

        if df['returns'].isnull().sum():
            isnull = df['returns'][df.returns.isnull()]
            self.logger.warning('Null value found for index {}', list(isnull.index))

        for dname in self.sywgquote_sql.dnames+['returns', 'vwap']:
            key = {'dname': dname, 'date': date}
            self.db.sywgindex_quote.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} indice into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.db.sywgindex_quote.name, self.db.name, date)

if __name__ == '__main__':
    quote = SYWGQuoteUpdater()
    quote.run()
