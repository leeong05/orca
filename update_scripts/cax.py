"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import cax_mssql


class CaxUpdater(UpdaterBase):
    """The updater class for collections 'date' and 'shares'"""

    def __init__(self, source=None, timeout=3000):
        self.source = source
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.sids = self.db.sids.distinct('sid')
        self.collection = self.db.shares
        if not self.skip_update:
            self.connect_wind()
            self.sql = cax_mssql
        if not self.skip_monitor:
            self.dates = self.db.dates.distinct('date')
            self.connect_monitor()

    def pro_update(self):
        pass

    def get_datas(self):
        CMD1 = self.sql.CMD1_1
        self.logger.debug('Executing command:\n{}', CMD1)
        self.cursor.execute(CMD1)
        self.df1 = pd.DataFrame(list(self.cursor), columns=['sid', 'cdate', 'cdate1', 'date', 'total_shares', 'float_shares', 'restricted_shares', 'non_tradable_shares'])
        CMD2 = self.sql.CMD1_2
        self.logger.debug('Executing command:\n{}', CMD2)
        self.cursor.execute(CMD2)
        self.df2 = pd.DataFrame(list(self.cursor), columns=['sid', 'cdate', 'cdate1', 'date', 'free_float_shares'])

    def update(self, date):
        CMD = self.sql.CMD0.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        if not list(self.cursor):
            self.logger.warning('{} is not a trading day?', date)
            return
        self.db.dates.update({'date': date}, {'date': date}, upsert=True)
        self.dates = self.db.dates.distinct('date')
        self.logger.info('UPSERT {} into dates', date)

        if not hasattr(self, 'df1'):
            self.get_datas()
        df1 = self.df1.query('date <= {!r} & cdate <= {!r} & cdate1 <= {!r}'.format(date, date, date))
        df1 = df1.query('date >= {!r}'.format(str(int(date)-2*10000)))
        df1 = df1.sort(['date', 'cdate1', 'cdate'])
        df1 = df1.drop_duplicates('sid', take_last=True)
        df1.index = df1.sid
        for dname in ['total_shares', 'float_shares', 'restricted_shares', 'non_tradable_shares']:
            self.collection.update({'date': date, 'dname': dname}, {'$set': {'dvalue': (df1[dname].astype(float)*10000).dropna().astype(int).to_dict()}}, upsert=True)

        df2 = self.df2.query('date <= {!r} & cdate <= {!r} & cdate1 <= {!r}'.format(date, date, date))
        df2 = df2.query('date >= {!r}'.format(str(int(date)-2*10000)))
        df2 = df2.sort(['date', 'cdate1', 'cdate'])
        df2 = df2.drop_duplicates('sid', take_last=True)
        df2.index = df2.sid
        self.collection.update({'date': date, 'dname': 'free_float_shares'}, {'$set': {'dvalue': (df2['free_float_shares'].astype(float)*10000).dropna().astype(int).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', len(df2), self.collection.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_shares WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_shares SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_shares (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

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
    cax = CaxUpdater()
    cax.run()
