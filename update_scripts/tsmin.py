"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

from multiprocessing import Pool

import numpy as np
import pandas as pd

from base import UpdaterBase
import tsmin_sql

def worker(args):
    date, time, df = args
    for dname in tsmin_sql.dnames:
        key = {'dname': dname, 'time': time, 'date': date}
        dvalue = df[dname].dropna().to_dict()
        COLLECTION.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)


class TSMinUpdater(UpdaterBase):
    """The updater class for collections 'ts_1min', 'ts_5min'."""

    def __init__(self, bar='1min', timeout=300, threads=16):
        UpdaterBase.__init__(self, timeout)
        self.bar = bar
        self.srcdir = os.path.join(tsmin_sql.srcdir, self.bar)
        self.threads = threads

    def pre_update(self):
        self.__dict__.update({
                'dates': self.db.dates.distinct('date'),
                'collection': self.db['ts_'+self.bar],
                })

    def pro_update(self):
        return

        self.logger.debug('Ensuring index date_1_dname_1_time_1 on collection %s', self.collection.name)
        self.collection.ensure_index([('date', 1), ('dname', 1), ('time', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index dname_1_date_1_time_1 on collection %s', self.collection.name)
        self.collection.ensure_index([('dname', 1), ('date', 1), ('time', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update TinySoft minute-bar data for the **same** day after market close."""
        fname = '%s-%s-%s.csv' % (date[:4], date[4:6], date[6:8])
        srcfile = os.path.join(self.srcdir, fname)
        if not os.path.exists(srcfile):
            self.logger.error('No records found for %s on %s', self.collection.name, date)
            return

        df = pd.read_csv(srcfile, header=0, usecols=tsmin_sql.col_index, names=tsmin_sql.col_names, dtype={0: np.str})
        df = df.ix[df.sid.apply(tsmin_sql.is_stock)]
        df.sid = [sid[2:8] for sid in df.sid]
        df['date'] = date
        df['time'] = [dt[11:13]+dt[14:16]+dt[17:19] for dt in df.datetime]
        vwap = df.amount / df.volume
        vwap[np.isinf(vwap)] = np.nan
        df['vwap'] = vwap
        df.drop('datetime', axis=1, inplace=True)
        df.index = df.sid

        grouped = df.groupby('time')
        pool = Pool(self.threads)
        pool.imap(worker, ((date, time, df) for time, df in grouped), self.threads)
        self.logger.info('UPSERT documents for %d sids into (c: [%s]) of (d: [%s]) on %s', grouped.count().max().ix[0], self.collection.name, self.db.name, date)

if __name__ == '__main__':
    ts_5min = TSMinUpdater(bar='5min')
    ts_1min = TSMinUpdater(bar='1min')

    ts_5min.connect_mongo()
    COLLECTION = ts_5min.db.ts_5min
    ts_5min.run()

    ts_1min.connect_mongo()
    COLLECTION = ts_1min.db.ts_1min
    ts_1min.run()
