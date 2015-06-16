"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import numpy as np
import pandas as pd

from base import UpdaterBase
import tsopen_sql as sql


class TSMinUpdater(UpdaterBase):
    """The updater class for collections 'ts_1min', 'ts_5min'."""

    def __init__(self, bar='1min', timeout=300):
        self.bar = bar
        super(TSMinUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db['ts_'+self.bar]
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update TinySoft opening minute-bar data for the **same** day after market open."""
        srcfile = None
        fname = '%s-%s-%s_%s.csv' % (date[:4], date[4:6], date[6:8], self.bar)
        srcfile1 = os.path.join(sql.srcdir1, fname)
        srcfile2 = os.path.join(sql.srcdir2, fname)
        if os.path.exists(srcfile1):
            srcfile = srcfile1
        if os.path.exists(srcfile2):
            srcfile = srcfile2
        if not srcfile:
            self.logger.error('No records found for {} on {}', self.collection.name, date)
            return

        df = pd.read_csv(srcfile, header=0, usecols=sql.col_index, names=sql.col_names, dtype={0: np.str})
        df['date'] = date
        df['time'] = [dt[11:13]+dt[14:16]+dt[17:19] for dt in df.datetime]
        df = df.query('volume > 0').copy()
        vwap = df.amount / df.volume
        vwap[np.isinf(vwap)] = np.nan
        df['vwap'] = vwap
        df.drop('datetime', axis=1, inplace=True)
        is_stock = df.sid.apply(sql.is_stock)

        sdf = df.ix[is_stock].copy()
        sdf.sid = [sid[2:8] for sid in sdf.sid]
        sdf.index = sdf.sid

        grouped = sdf.groupby('time')
        for time, df in grouped:
            for dname in sql.dnames:
                key = {'dname': dname, 'time': time, 'date': date}
                dvalue = df[dname].dropna().to_dict()
                self.collection.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', grouped.count().max().ix[0], self.collection.name, self.db.name, date)

    def monitor(self, date):
        return


if __name__ == '__main__':
    ts_5min = TSMinUpdater(bar='5min')
    ts_1min = TSMinUpdater(bar='1min')

    ts_5min.connect_mongo()
    ts_5min.run()

    ts_1min.connect_mongo()
    ts_1min.run()
