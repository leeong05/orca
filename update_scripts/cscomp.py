"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import cscomp_sql as sql


class CSCompUpdater(UpdaterBase):
    """The updater class for collection 'index_components'."""

    def __init__(self, timeout=600):
        super(CSCompUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.index_components
        if not self.skip_update:
            self.connect_wind()
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
            self.logger.warning('No records found for HS300 on {}', date)
            return

        df1.columns = ['sid', 'weight']
        df1['weight'] = df1.weight.astype(float)
        df1['sid'] = df1['sid'].apply(lambda x: x[:6])
        df1.index = df1.sid
        assert len(df1) == 300
        self.collection.update({'date': date, 'dname': 'SH000300'}, {'$set': {'dvalue': df1.weight.dropna().to_dict()}}, upsert=True)
        sh50 = self.collection.find_one({'date': date, 'dname': 'SH000016'})['dvalue'].keys()
        sdf1 = df1.ix[sh50].copy()
        assert len(sdf1) == 50
        sdf1['weight'] *= 100./sdf1['weight'].sum()
        self.collection.update({'date': date, 'dname': 'SH000016'}, {'$set': {'dvalue': sdf1.weight.dropna().to_dict()}}, upsert=True)

        CMD = sql.CMD2.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df2 = pd.DataFrame(list(self.cursor))
        if len(df2) == 0:
            self.logger.warning('No records found for CS500 on {}', date)
        df2.columns = ['sid', 'weight']
        df2['weight'] = df2.weight.astype(float)
        df2['sid'] = df2['sid'].apply(lambda x: x[:6])
        df2.index = df2.sid
        assert len(df2) == 500
        self.collection.update({'date': date, 'dname': 'SH000905'}, {'$set': {'dvalue': df2.weight.dropna().to_dict()}}, upsert=True)

    def monitor(self, date):
        return


if __name__ == '__main__':
    comp = CSCompUpdater()

    comp.run()
