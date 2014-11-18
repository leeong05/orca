"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DB
from base import KMinFetcher

class IntervalFetcher(KMinFetcher):
    """Class to fetch TinySoft minute-bar data.

    :param str freq: Frequency of minute-bar data, currently only supports: ('5min', '1min')

    """

    collections = {
            '5min': DB.ts_5min,
            '1min': DB.ts_1min,
            }
    dnames = DB.ts_5min.distinct('dname')

    def __init__(self, freq, **kwargs):
        if freq not in ('5min', '1min'):
            raise ValueError('No minute-bar data of frequency {0!r} exists'.format(freq))
        self._freq = freq
        self.collection = IntervalFetcher.collections[freq]
        super(IntervalFetcher, self).__init__(**kwargs)

    @property
    def freq(self):
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq not in ('5min', '1min'):
            self.warning('No minute-bar data of frequency {0!r} exists. Nothing has changed'.format(freq))
            return
        self._freq = freq
        self.collection = IntervalFetcher.collections[freq]
