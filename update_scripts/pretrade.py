"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import numpy as np
import pandas as pd

from base import UpdaterBase
import pretrade_sql as sql


class PreTradeUpdater(UpdaterBase):
    """The updater class for pretrading auction data."""

    def __init__(self, bar='1min', timeout=300):
        super(PreTradeUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.auction
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update TinySoft pretrading auction data for the **same** day rightly after market open."""
        srcfile = None
        fname = '%s-%s-%s.csv' % (date[:4], date[4:6], date[6:8])
        srcfile1 = os.path.join(sql.srcdir1, fname)
        srcfile2 = os.path.join(sql.srcdir2, fname)
        if os.path.exists(srcfile1):
            srcfile = srcfile1
        if os.path.exists(srcfile2):
            srcfile = srcfile2
        if not srcfile:
            self.logger.error('No records found for pretrading data on {}', date)
            return

        df = pd.read_csv(srcfile, header=0, names=sql.col_names, dtype={0: np.str}, parse_dates=[1])
        df['date'] = df['datetime'].apply(lambda x: x.strftime('%Y%m%d'))
        is_stock = df.sid.apply(sql.is_stock)
        df = df.ix[is_stock]
        df['sid'] = df['sid'].apply(lambda x: x[2:])
        df.to_msgpack(sql.msgpack_path.format(date=date))
        self.logger.info('Updating data for {} sids on {}', len(df['sid'].unique()), date)

        def func1(df):
            price = 0.5*(df['ask1']+df['bid1'])
            amount = df['aks1']+df['bds1']
            return (amount*price).mean()/amount.mean()

        vwap = df.groupby('sid').apply(func1).dropna()
        self.collection.update({'dname': 'vwap', 'date': date}, {'$set': {'dvalue': vwap.to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', len(vwap), self.collection.name, 'vwap', self.db.name, date)

        def func2(df):
            return len(df)

        ntrd = df.groupby('sid').apply(func2).dropna()
        self.collection.update({'dname': 'ntrd', 'date': date}, {'$set': {'dvalue': ntrd.to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', len(ntrd), self.collection.name, 'ntrd', self.db.name, date)

    def monitor(self, date):
        return

if __name__ == '__main__':
    pre_trade = PreTradeUpdater()

    pre_trade.run()
