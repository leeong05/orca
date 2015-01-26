"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd

from base import UpdaterBase

columns = ['dt', 'price', 'volume', 'bid1', 'ask1', 'bds1', 'aks1']
dnames = ['price', 'volume', 'bid1', 'bds1', 'ask1', 'aks1']


class LOFUpdater(UpdaterBase):
    """The updater class for collection 'lof'."""

    def __init__(self, timeout=60):
        super(LOFUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.lof

    def pro_update(self):
        return

        self.logger.debug('Ensuring index sid_1_dname_1_date_1 on collection {}', self.collection.name)
        self.collection.ensure_index([('sid', 1), ('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update daily LOF tick data for the **same** day after market close."""
        dirname = os.path.join('/home/SambaServer/TinySoftData/LOF', date[:4]+'-'+date[4:6]+'-'+date[6:8])
        if not os.path.exists(dirname):
            self.logger.warning('No data exists for date {}', date)
            return

        sids = []
        for fname in os.listdir(dirname):
            sid = fname[2:8]
            sids.append(sid)
            fname = os.path.join(dirname, fname)
            df = pd.read_csv(fname)
            df.columns = columns
            df.index = pd.to_datetime(df['dt']).astype(int).astype(str)

            for dname in dnames:
                key = {'date': date, 'sid': sid, 'dname': dname}
                if dname in ['price', 'bid1', 'ask1']:
                    doc = {'dvalue': df[dname].astype(float).to_dict()}
                else:
                    doc = {'dvalue': df[dname].astype(int).to_dict()}
                self.collection.update(key, {'$set': doc}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}] of (d: [{}]) on {}', len(sids), self.collection.name, self.db.name, date)


if __name__ == '__main__':
    lof = LOFUpdater()
    lof.run()
