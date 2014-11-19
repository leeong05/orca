"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from multiprocessing import Pool
import pandas as pd

from base import UpdaterBase
import components_sql


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

    def __init__(self, timeout=600, threads=16):
        UpdaterBase.__init__(self, timeout)
        self.threads = threads

    def pre_update(self):
        self.connect_jydb()
        self.__dict__.update({'dates': self.db.dates.distinct('date')})

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection index_components')
        self.db.index_components.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update index components (and weight) for the **same** day before market open."""
        CMD = components_sql.CMD1.format(date=date)
        self.logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        df1 = pd.DataFrame(list(self.cursor))
        if len(df1) == 0:
            self.logger.warning('No records found for %s on %s', self.db.index_components.name, date)
            return

        df1.columns = ['dname', 'market', 'sid']
        df1.dname = ['SH'+dname if mkt == 83 else 'SZ'+dname for mkt, dname in zip(df1.market, df1.dname)]
        df1.index = df1.sid

        CMD = components_sql.CMD2.format(date=date)
        self.logger.debug('Executing command:\n%s', CMD)
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
        pool.imap(worker, [(date, dname, _df1, df2) for dname, _df1 in grouped], self.threads)

        self.logger.info('UPSERT documents for %d indice into (c: [%s]) of (d: [%s]) on %s', len(grouped), self.db.index_components.name, self.db.name, date)

if __name__ == '__main__':
    comp = ComponentsUpdater()

    comp.connect_mongo()
    COLLECTION = comp.db.index_components
    comp.run()
