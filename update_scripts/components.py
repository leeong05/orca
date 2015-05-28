"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from multiprocessing import Pool, cpu_count
import pandas as pd

from base import UpdaterBase
import components_oracle as sql

from orca.mongo.quote import QuoteFetcher
from orca.mongo.components import ComponentsFetcher
quote_fetcher = QuoteFetcher()
components_fetcher = ComponentsFetcher(as_bool=False)


def worker(args):
    date, dname, df1, df2 = args
    dvalue = {}
    sdf2 = df2.ix[df2.dname == dname] if df2 is not None else None
    for sid in df1.sid:
        try:
            dvalue[sid] = sdf2.weight.ix[sid]
        except:
            dvalue[sid] = -1
    COLLECTION.update({'date': date, 'dname': dname}, {'$set': {'dvalue': dvalue}}, upsert=True)


class ComponentsUpdater(UpdaterBase):
    """The updater class for collection 'index_components'."""

    def __init__(self, timeout=600, threads=cpu_count()):
        self.threads = threads
        super(ComponentsUpdater, self).__init__(timeout=timeout)
        self.index_dname = {
                'SH50': 'SH000016',
                'HS300': 'SH000300',
                'CS500': 'SH000905',
                }

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.index_components
        if not self.skip_update:
            self.connect_jydb()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update index components (and weight) for the **same** day before market open."""
        CMD = sql.CMD1.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df1 = pd.DataFrame(list(self.cursor))
        if len(df1) == 0:
            self.logger.warning('No records found for {} on {}', self.db.index_components.name, date)
            return

        df1.columns = ['dname', 'market', 'sid']
        df1.dname = ['SH'+dname if mkt == 83 else 'SZ'+dname for mkt, dname in zip(df1.market, df1.dname)]
        df1.index = df1.sid

        CMD = sql.CMD2.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        try:
            df2 = pd.DataFrame(list(self.cursor))
            df2.columns = ['dname', 'market', 'sid', 'weight']
            df2.dname = ['SH'+dname if mkt == 83 else 'SZ'+dname for mkt, dname in zip(df2.market, df2.dname)]
            df2.index = df2.sid
        except:
            df2 = None

        grouped = df1.groupby('dname')
        pool = Pool(self.threads)
        pool.imap_unordered(worker, [(date, dname, _df1, df2) for dname, _df1 in grouped], self.threads)
        pool.close()
        pool.join()

        pdate = self.dates[self.dates.index(date)-1]
        returns = quote_fetcher.fetch_daily('returns', pdate).fillna(0)
        for sid, dname in self.index_dname.iteritems():
            CMD = sql.CMD3.format(date=date, sid=dname[2:])
            self.cursor.execute(CMD)
            date_ = list(self.cursor)[0][0]
            if pdate < date_ and date_ <= date:
                continue
            weight = components_fetcher.fetch_daily(sid, pdate)
            new_weight = weight * (1+returns.ix[weight.index])
            new_weight *= weight.sum()/new_weight.sum()
            dvalue = new_weight.dropna().to_dict()
            COLLECTION.update({'date': date, 'dname': dname}, {'$set': {'dvalue': dvalue}}, upsert=True)
            self.logger.info('Adjusting components weight for {} on date {}', sid, date)

        self.logger.info('UPSERT documents for {} indice into (c: [{}]) of (d: [{}]) on {}', len(grouped), COLLECTION.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_components WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_components SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_components (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for index, dname in self.index_dname.iteritems():
            ser = pd.Series(self.collection.find_one({'dname': dname, 'date': date})['dvalue']).dropna()
            for statistic in statistics:
                cursor.execute(SQL1, (date, index, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, index, statistic))
                else:
                    cursor.execute(SQL3, (date, index, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', index, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    comp = ComponentsUpdater()

    comp.connect_mongo()
    COLLECTION = comp.db.index_components
    comp.run()
