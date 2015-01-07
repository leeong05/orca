"""
.. moduleauthor: Li, Wang <wangziqi@foreseefund.com>
"""

from datetime import (
        datetime,
        timedelta,
        )

import pandas as pd

from orca import (
        DB,
        DATES,
        )

from base import KDayFetcher


class IFFetcher(KDayFetcher):
    """Class to fetch IF tick data in wholesale by day."""

    dnames = ['price', 'volume', 'open_interest', 'bid1', 'bds1', 'ask1', 'aks1']

    def __init__(self, **kwargs):
        self.collection = DB.IF
        super(IFFetcher, self).__init__(**kwargs)

    @staticmethod
    def to_datetime(date, ms):
        dt = datetime.strptime(date, '%Y%m%d')
        return dt + timedelta(milliseconds=ms)

    def fetch(self, dname, startdate=None, enddate=None, backdays=0, **kwargs):
        if startdate is None:
            if enddate is None:
                return self.fetch_window(dname, **kwargs)
            startdate = DATES[0]
        return super(IFFetcher, self).fetch(dname, startdate, enddate=enddate, backdays=backdays, **kwargs)

    def fetch_window(self, dname, window=None, IF=None, **kwargs):
        if isinstance(dname, str):
            return self._fetch_window(dname, window=window, IF=IF, **kwargs)
        res = {}
        for _dname in dname:
            res[_dname] = self._fetch_window(_dname, window=window, IF=IF, **kwargs)
        return pd.DataFrame(res)

    def _fetch_window(self, dname, window=None, IF=None, **kwargs):
        """
        :param str IF: IF name
        """
        assert IF is not None
        query = {'sid': IF, 'dname': dname}
        if window is not None:
            query.update({'date': {'$gte': window[0], '$lte': window[-1]}})
        proj = {'_id': 0, 'dvalue': 1, 'date': 1}
        res = {}
        for row in self.collection.find(query, proj):
            date = row['date']
            res.update({self.to_datetime(date, int(k)): v for k, v in row['dvalue'].iteritems()})
        res = pd.Series(res)
        res.name = dname
        return res

    def fetch_history(self, *args, **kwargs):
        return super(IFFetcher, self).fetch_history(*args, **kwargs)

    def fetch_daily(self, dname, date, offset=0, **kwargs):
        return self.fetch_history(dname, date, 1, delay=offset, **kwargs)
