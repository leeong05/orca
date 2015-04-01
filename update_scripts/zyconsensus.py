"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import itertools
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd

from base import UpdaterBase
import zyconsensus_oracle as zysql

def worker(args):
    sid, df = args
    actual = df.iloc[0]
    if actual['consensus_type'] != 0:
        return sid, None
    summary = {'growth': actual['growth']}
    for i, _dname in itertools.product(range(-1, 3), zysql._dnames2):
        try:
            summary[_dname+'_'+str(i)] = df[_dname].iloc[i+1]
        except:
            summary[_dname+'_'+str(i)] = np.nan
    return sid, summary


class ZYConsensusUpdater(UpdaterBase):
    """The updater class for collection 'zyconsensus'."""

    def __init__(self, threads=cpu_count(), cutoff='08:30:00', timeout=60):
        self.threads = threads
        self.cutoff = cutoff
        super(ZYConsensusUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date'),
        self.collection = self.db.zyconsensus
        if not self.skip_update:
            self.connect_zyyx()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update target prices, consensus data for the **previous** day before market open."""
        prev_date = self.dates[self.dates.index(date)-1]
        self.update_target_price(date, prev_date)
        self.update_consensus(date, prev_date)

    def update_target_price(self, date, prev_date):
        CMD = zysql.CMD1.format(date=date, prev_date=prev_date, cutoff=self.cutoff)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=target_price on {}', self.collection.name, prev_date)
            return

        df.columns = ['sid'] + zysql.dnames1
        df.index = df.sid

        for dname in zysql.dnames1:
            key = {'dname': dname, 'date': prev_date}
            self.collection.update(key, {'$set': {'dvalue': df[dname].dropna().to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=target_price]) of (d: [{}]) on {}', len(df), self.collection.name, self.db.name, prev_date)

    def update_consensus(self, date, prev_date):
        CMD = zysql.CMD2.format(date=date, prev_date=prev_date, cutoff=self.cutoff)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=consensus on {}', self.collection.name, prev_date)
            return

        df.columns = ['sid', 'consensus_type', 'forecast_year', 'growth'] + zysql._dnames2
        grouped = df.groupby('sid')

        pool = Pool(self.threads)
        res = pool.imap(worker, grouped, self.threads)

        df = pd.DataFrame({sid: summary for sid, summary in res}).T.dropna(how='all')
        for dname in zysql.dnames2:
            key = {'dname': dname, 'date': prev_date}
            self.collection.update(key, {'$set': {'dvalue': df[dname].dropna().to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=consensus]) of (d: [{}]) on {}', len(df), self.collection.name, self.db.name, prev_date)

    def monitor(self, date):
        date = self.dates[self.dates.index(date)-1]
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_zyconsensus WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_zyconsensus SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_zyconsensus (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distinct('dname'):
            ser = pd.Series(self.collection.find_one({'dname': dname, 'date': date})['dvalue'])
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    zy = ZYConsensusUpdater()
    zy.run()
