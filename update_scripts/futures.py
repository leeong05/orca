"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd

from base import UpdaterBase

columns = ['dt', 'price', 'volume', 'bid1', 'ask1', 'bds1', 'aks1', 'open_interest']
dnames = ['price', 'volume', 'open_interest', 'bid1', 'bds1', 'ask1', 'aks1']


class IFUpdater(UpdaterBase):
    """The updater class for collection 'IF'."""

    def __init__(self, timeout=60):
        super(IFUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.IF

    def pro_update(self):
        return

        self.logger.debug('Ensuring index sid_1_dname_1_date_1 on collection {}', self.collection.name)
        self.collection.ensure_index([('sid', 1), ('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    @staticmethod
    def to_milliseconds(dt, m):
        return str(dt * 1000 + m)

    def update(self, date):
        """Update daily IF tick data for the **same** day after market close."""
        dirname = os.path.join('/home/SambaServer/TinySoftData/IF', date[:4]+'-'+date[4:6]+'-'+date[6:8])
        if not os.path.exists(dirname):
            self.logger.warning('No data exists for date {}', date)
            return

        for fname in os.listdir(dirname):
            sid = fname[:6]
            fname = os.path.join(dirname, fname)
            df = pd.read_csv(fname)
            df.columns = columns
            df['ms'] = (pd.to_datetime(df['dt']) - pd.to_datetime(date)).astype(int) / 1000000
            res = []
            for dt, sdf in df.groupby('ms'):
                if len(sdf) >= 3:
                    sdf = sdf.iloc[-2:]
                    self.logger.warning('Wrong records for {} @{}', fname, sdf['dt'].iloc[-1])
                if len(sdf) == 2:
                    sdf['ms'] += [0, 500]
                else:
                    sdf['ms'] += 0
                res.append(sdf)
            df = pd.concat(res).sort('ms')
            df.index = df.ms.astype(int).astype(str)

            for dname in dnames:
                key = {'date': date, 'sid': sid, 'dname': dname}
                if dname in ['price', 'bid1', 'ask1']:
                    doc = {'dvalue': df[dname].astype(float).to_dict()}
                else:
                    doc = {'dvalue': df[dname].astype(int).to_dict()}
                self.collection.update(key, {'$set': doc}, upsert=True)
            self.logger.info('UPSERT documents into (c: [{}@sid={}] of (d: [{}]) on {}', self.collection.name, sid, self.db.name, date)


if __name__ == '__main__':
    IF = IFUpdater()
    IF.run()
