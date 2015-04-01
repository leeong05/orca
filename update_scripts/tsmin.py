"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

from multiprocessing import Pool, cpu_count

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
    """The updater class for collections 'ts_1min', 'ts_5min', 'ts_min30'."""

    def __init__(self, bar='1min', timeout=300, threads=cpu_count()):
        self.bar = bar
        self.srcdir1 = os.path.join(tsmin_sql.srcdir1, self.bar)
        self.srcdir2 = os.path.join(tsmin_sql.srcdir2, self.bar)
        self.threads = threads
        super(TSMinUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db['ts_'+self.bar]
        self.index_collection = self.db['tsindex_'+self.bar]
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update TinySoft minute-bar data for the **same** day after market close."""
        srcfile = None
        fname = '%s-%s-%s.csv' % (date[:4], date[4:6], date[6:8])
        srcfile1 = os.path.join(self.srcdir1, fname)
        srcfile2 = os.path.join(self.srcdir2, fname)
        if os.path.exists(srcfile1):
            srcfile = srcfile1
        if os.path.exists(srcfile2):
            srcfile = srcfile2
        if not srcfile:
            self.logger.error('No records found for {} on {}', self.collection.name, date)
            return

        df = pd.read_csv(srcfile, header=0, usecols=tsmin_sql.col_index, names=tsmin_sql.col_names, dtype={0: np.str})
        df['date'] = date
        df['time'] = [dt[11:13]+dt[14:16]+dt[17:19] for dt in df.datetime]
        vwap = df.amount / df.volume
        vwap[np.isinf(vwap)] = np.nan
        df['vwap'] = vwap
        df.drop('datetime', axis=1, inplace=True)
        is_stock = df.sid.apply(tsmin_sql.is_stock)

        idf = df.ix[~is_stock].copy()
        idf.index = idf.sid
        for sid, ser in idf.iterrows():
            key = {'dname': sid, 'date': date, 'time': ser['time']}
            ser = ser.ix[tsmin_sql.index_dnames]
            self.index_collection.update(key, {'$set': ser.to_dict()}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', len(idf.sid.unique()), self.index_collection.name, self.db.name, date)

        sdf = df.ix[is_stock].copy()
        sdf.sid = [sid[2:8] for sid in sdf.sid]
        sdf.index = sdf.sid

        grouped = sdf.groupby('time')
        pool = Pool(self.threads)
        pool.imap_unordered(worker, ((date, time, df) for time, df in grouped), self.threads)
        pool.close()
        pool.join()
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', grouped.count().max().ix[0], self.collection.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_{} WHERE trading_day=%s AND data=%s AND statistic=%s".format(''.join(self.collection.name.split('_')))
        SQL2 = "UPDATE mongo_{} SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s".format(''.join(self.collection.name.split('_')))
        SQL3 = "INSERT INTO mongo_{} (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)".format(''.join(self.collection.name.split('_')))

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distinct('dname'):
            ser = pd.Series(self.collection.find_one({'dname': dname, 'date': date, 'time': '150000'})['dvalue'])
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    ts_30min = TSMinUpdater(bar='30min')
    ts_5min = TSMinUpdater(bar='5min')
    ts_1min = TSMinUpdater(bar='1min')

    ts_5min.connect_mongo()
    COLLECTION = ts_5min.db.ts_5min
    ts_5min.run()

    ts_30min.connect_mongo()
    COLLECTION = ts_30min.db.ts_30min
    ts_30min.run()

    ts_1min.connect_mongo()
    COLLECTION = ts_1min.db.ts_1min
    ts_1min.run()
