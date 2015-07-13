"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=pd.core.common.SettingWithCopyWarning)

from base import UpdaterBase

columns = ['dt', 'price', 'volume', 'bid1', 'ask1', 'bds1', 'aks1', 'open_interest']
dnames = ['price', 'volume', 'open_interest', 'bid1', 'bds1', 'ask1', 'aks1']
columns1 = ['dt', 'price', 'volume']
dnames1 = ['price', 'volume']


class IFUpdater(UpdaterBase):
    """The updater class for collection 'IF'."""

    def __init__(self, timeout=1200):
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
            sid = fname[:-9]
            if sid[:2] not in ('IC', 'IF', 'IH', 'TF', 'SH'):
                continue
            if sid == 'SH000016':
                sid = 'SH50'
            elif sid == 'SH000300':
                sid = 'HS300'
            elif sid == 'SH000905':
                sid = 'CS500'
            fname = os.path.join(dirname, fname)
            df = pd.read_csv(fname)
            if sid in ('SH50', 'HS300', 'CS500'):
                _columns = columns1
            else:
                _columns = columns
            df = df.iloc[:, :len(_columns)]
            df.columns = _columns
            df['ms'] = (pd.to_datetime(df['dt']) - pd.to_datetime(date)).astype(int) / 1000000

            res = []
            for ms, sdf in df.groupby('ms'):
                sdf = sdf.copy()
                sdf['ms'] += [int(1000/len(sdf)) * i for i in range(0, len(sdf))]
                res.append(sdf)

            df = pd.concat(res).sort('ms')
            df.index = df.ms.astype(int).astype(str)

            if sid in ('SH50', 'HS300', 'CS500'):
                _dnames = dnames1
            else:
                _dnames = dnames
            for dname in _dnames:
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
