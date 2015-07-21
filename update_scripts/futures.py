"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=pd.core.common.SettingWithCopyWarning)

from base import UpdaterBase
from orca.utils.dateutil import generate_intervals
times_1min = generate_intervals(1*60, begin='091500', end='151500')
times_5min = generate_intervals(5*60, begin='091500', end='151500')

columns = ['dt', 'price', 'volume', 'bid1', 'ask1', 'bds1', 'aks1', 'open_interest']
dnames = ['price', 'volume', 'open_interest', 'bid1', 'bds1', 'ask1', 'aks1']
columns1 = ['dt', 'price', 'volume']
dnames1 = ['price', 'volume']


class IFUpdater(UpdaterBase):
    """The updater class for collection 'IF'."""

    def __init__(self, timeout=1200):
        super(IFUpdater, self).__init__(timeout=timeout)
        self.sids = set()

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
        self.sids.clear()
        dirname = os.path.join('/home/SambaServer/TinySoftData/IF', date[:4]+'-'+date[4:6]+'-'+date[6:8])
        if not os.path.exists(dirname):
            self.logger.warning('No data exists for date {}', date)
            return

        for fname in os.listdir(dirname):
            sid = fname[:-9]
            if sid[:1] not in ('I', 'T', 'S'):
                continue
            if sid == 'SH000016':
                sid = 'SH50'
            elif sid == 'SH000300':
                sid = 'HS300'
            elif sid == 'SH000905':
                sid = 'CS500'
            elif sid[:2] in ('IF', 'IH', 'IC'):
                self.sids.add(sid)
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

        self.update_interval(date)

    def update_interval(self, date):
        for sid in self.sids:
            self.update_price(date, sid)
            self.update_ab(date, sid)
            self.update_volume(date, sid)

    def update_price(self, date, sid):
        try:
            p = pd.Series(self.collection.find_one({'sid': sid, 'date': date, 'dname': 'price'})['dvalue'])
        except:
            self.logger.warning('No price found for {} on {}', sid, date)
            return
        p.index = [datetime.strptime(date, '%Y%m%d')+timedelta(milliseconds=int(s)) for s in p.index]

        df5 = p.resample('5min', 'ohlc', label='right', closed='right')
        df5.columns = ['into', 'inth', 'intl', 'intc']
        df5['lstl'] = pd.expanding_min(p).resample('5min', 'last', label='right', closed='right')
        df5['lsth'] = pd.expanding_max(p).resample('5min', 'last', label='right', closed='right')
        df5['vlty'] = p.resample('5min', lambda x: x.std(), label='right', closed='right')
        df5['lstp'] = p.resample('5min', 'last', label='right', closed='right')
        df5['tvwp'] = p.resample('5min', 'mean', label='right', closed='right')
        df5.index = [dt.strftime('%H%M%S') for dt in df5.index]
        df5 = df5.ix[times_5min]
        for dname, ser in df5.iteritems():
            self.db.IF_5min.update({'sid': sid, 'date': date, 'dname': dname}, {'$set': {'dvalue': ser.to_dict()}}, upsert=True)

        df1 = p.resample('1min', 'ohlc', label='right', closed='right')
        df1.columns = ['into', 'inth', 'intl', 'intc']
        df1['lstl'] = pd.expanding_min(p).resample('1min', 'last', label='right', closed='right')
        df1['lstl'] = pd.expanding_max(p).resample('1min', 'last', label='right', closed='right')
        df1['vlty'] = p.resample('1min', lambda x: x.std(), label='right', closed='right')
        df1['lstp'] = p.resample('1min', 'last', label='right', closed='right')
        df1.index = [dt.strftime('%H%M%S') for dt in df1.index]
        df1 = df1.ix[times_1min]
        for dname, ser in df1.iteritems():
            self.db.IF_1min.update({'sid': sid, 'date': date, 'dname': dname}, {'$set': {'dvalue': ser.to_dict()}}, upsert=True)

    def update_ab(self, date, sid):
        try:
            aks1 = pd.Series(self.collection.find_one({'sid': sid, 'date': date, 'dname': 'aks1'})['dvalue'])
            ask1 = pd.Series(self.collection.find_one({'sid': sid, 'date': date, 'dname': 'ask1'})['dvalue'])
            bds1 = pd.Series(self.collection.find_one({'sid': sid, 'date': date, 'dname': 'bds1'})['dvalue'])
            bid1 = pd.Series(self.collection.find_one({'sid': sid, 'date': date, 'dname': 'bid1'})['dvalue'])
            df = pd.concat([aks1, ask1, bds1, bid1], axis=1)
            df.columns = ['iaks', 'iask', 'ibds', 'ibid']
        except:
            self.logger.warning('No bid/ask found for {} on {}', sid, date)
            return
        df.index = [datetime.strptime(date, '%Y%m%d')+timedelta(milliseconds=int(s)) for s in df.index]
        df['tlrt'] = np.log(df['ibds']/df['iaks'])

        df5 = df.resample('5min', 'mean', label='right', closed='right')
        df5.index = [dt.strftime('%H%M%S') for dt in df5.index]
        df5 = df5.ix[times_5min]
        for dname, ser in df5.iteritems():
            self.db.IF_5min.update({'sid': sid, 'date': date, 'dname': dname}, {'$set': {'dvalue': ser.to_dict()}}, upsert=True)

        df1 = df.resample('1min', 'mean', label='right', closed='right')
        df1.index = [dt.strftime('%H%M%S') for dt in df1.index]
        df1 = df1.ix[times_1min]
        for dname, ser in df1.iteritems():
            self.db.IF_1min.update({'sid': sid, 'date': date, 'dname': dname}, {'$set': {'dvalue': ser.to_dict()}}, upsert=True)

    def update_volume(self, date, sid):
        try:
            v = pd.Series(self.collection.find_one({'sid': sid, 'date': date, 'dname': 'volume'})['dvalue'])
            p = pd.Series(self.collection.find_one({'sid': sid, 'date': date, 'dname': 'price'})['dvalue'])
            df = pd.concat([v, p], axis=1)
            df.columns = ['intv', 'lstp']
        except:
            self.logger.warning('No volume found for {} on {}', sid, date)
            return
        df.index = [datetime.strptime(date, '%Y%m%d')+timedelta(milliseconds=int(s)) for s in df.index]
        df['inta'] = df['intv']*df['lstp']

        df5 = df.resample('5min', 'sum', label='right', closed='right')
        df5['ivwp'] = df5['inta']/df5['intv']
        df5.index = [dt.strftime('%H%M%S') for dt in df5.index]
        df5 = df5.ix[times_5min][['intv', 'ivwp']]
        for dname, ser in df5.iteritems():
            self.db.IF_5min.update({'sid': sid, 'date': date, 'dname': dname}, {'$set': {'dvalue': ser.to_dict()}}, upsert=True)

        df1 = df.resample('1min', 'sum', label='right', closed='right')
        df1['ivwp'] = df1['inta']/df1['intv']
        df1.index = [dt.strftime('%H%M%S') for dt in df1.index]
        df1 = df1.ix[times_1min][['intv', 'ivwp']]
        for dname, ser in df1.iteritems():
            self.db.IF_1min.update({'sid': sid, 'date': date, 'dname': dname}, {'$set': {'dvalue': ser.to_dict()}}, upsert=True)


if __name__ == '__main__':
    IF = IFUpdater()
    IF.run()
