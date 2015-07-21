"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import DB
from orca.utils import dateutil

from base import KDayFetcher


class IFIntervalFetcher(KDayFetcher):
    """Class to fetch TinySoft futures minute-bar data.

    :param str freq: Frequency of minute-bar data, currently only supports: ('5min', '1min')
    """

    collections = {
            '5min': DB.IF_5min,
            '1min': DB.IF_1min,
            }
    intervals = {
            '5min': dateutil.generate_intervals(5*60, begin='091500', end='151500'),
            '1min': dateutil.generate_intervals(1*60, begin='091500', end='151500'),
            }
    dnames = DB.IF_5min.distinct('dname')
    freqs = ('30min', '5min', '1min')

    def __init__(self, freq, **kwargs):
        if freq not in IFIntervalFetcher.freqs:
            raise ValueError('No minute-bar data of frequency {0!r} exists'.format(freq))
        self._freq = freq
        self.collection = IFIntervalFetcher.collections[freq]
        self.intervals = IFIntervalFetcher.intervals[freq]
        super(IFIntervalFetcher, self).__init__(**kwargs)

    @property
    def freq(self):
        """Property."""
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq not in IFIntervalFetcher.freqs:
            self.warning('No minute-bar data of frequency {0!r} exists. Nothing has changed'.format(freq))
            return
        self._freq = freq
        self.collection = IFIntervalFetcher.collections[freq]
        self.intervals = IFIntervalFetcher.intervals[freq]

    def fetch_window(self, dname, window, sid=None, **kwargs):
        assert sid
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        reindex = kwargs.get('reindex', self.reindex)

        query = {'sid': sid, 'dname': dname, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'dvalue': 1, 'date': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
        del cursor
        if reindex:
            ser = df.T.unstack()
            ser.index = pd.to_datetime([d+t for d, t in ser.index])
            return ser
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df

    def fetch(self, dname, startdate=None, **kwargs):
        if startdate is None:
            startdate = '20100416'
        return super(IFIntervalFetcher, self).fetch(dname, startdate, **kwargs)
