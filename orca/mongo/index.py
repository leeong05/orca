"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import (
        DB,
        DATES,
        )
from orca.utils import dateutil

from base import (
        KDayFetcher,
        KMinFetcher,
        )


class IndexQuoteFetcher(KDayFetcher):
    """Class to fetch index quote data."""

    index_dname = {
            'HS300': 'SH000300',
            'CS500': 'SH000905',
            'CS800': 'SH000906',
            'SH50': 'SH000016',
            }
    dnames = ['amount', 'close', 'high', 'low', 'open', 'prev_close', 'returns', 'volume', 'vwap']

    def __init__(self, **kwargs):
        self.collection = DB.index_quote
        super(IndexQuoteFetcher, self).__init__(**kwargs)
        if self.reindex is True:
            self.warning('Force reindex to be False')
            self.reindex = False

    def fetch_window(self, dname, window, index=None, **kwargs):
        """
        :param dname: Data name or a list of data names or None(to fetch all daily quote items)
        :type dname: str, list, None
        :param str index: Index name
        :returns: Series if ``dname`` is only a string or DataFrame with ``dname`` in the columns
        """
        assert index is not None
        index = IndexQuoteFetcher.index_dname.get(index, index)
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        if dname is None:
            dname = IndexQuoteFetcher.dnames

        query = {'index': index, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'date': 1}
        _dname = [dname] if isinstance(dname, str) else dname
        for d in _dname:
            proj.update({d: 1})
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame(list(cursor))
        del cursor
        df.index = df.date
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df[dname]


class IndexIntervalFetcher(KMinFetcher):
    """Class to fetch TinySoft index minute-bar data.

    :param str freq: Frequency of minute-bar data, currently only supports: ('5min', '1min')
    """

    collections = {
            '5min': DB.tsindex_5min,
            '1min': DB.tsindex_1min,
            }

    index_dname = {
            'SHZS': 'SH000001',
            'SZZS': 'SZ399107',
            'ZXBZ': 'SZ399005',
            'CYBZ': 'SZ399006',
            'JCAZ': 'SZ399317',
            'HS300':'SH000300',
            'SH50': 'SH000016',
            }

    dnames = ['open', 'high', 'low', 'close', 'volume', 'amount', 'vwap']

    def __init__(self, freq, **kwargs):
        if freq not in ('5min', '1min'):
            raise ValueError('No minute-bar data of frequency {0!r} exists'.format(freq))
        self._freq = freq
        self.collection = IndexIntervalFetcher.collections[freq]
        super(IndexIntervalFetcher, self).__init__(**kwargs)
        if self.datetime_index is not True:
            self.datetime_index = True
            self.debug('Force self.datetime_index to be True')

    @property
    def freq(self):
        """Property."""
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq not in ('5min', '1min'):
            self.warning('No minute-bar data of frequency {0!r} exists. Nothing has changed'.format(freq))
            return
        self._freq = freq
        self.collection = IndexIntervalFetcher.collections[freq]

    def fetch_window(self, dname, window, index=None, **kwargs):
        """
        :param dname: Data name or a list of data names or None(to fetch all interval quote items)
        :type dname: str, list, None
        :param str index: Index name
        :returns: Series if ``dname`` is only a string or DataFrame with ``dname`` in the columns
        """
        assert index is not None
        index = IndexIntervalFetcher.index_dname.get(index, index)
        if dname is None:
            dname = IndexIntervalFetcher.dnames

        query = {'dname': index, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'date': 1, 'time': 1}
        _dname = [dname] if isinstance(dname, str) else dname
        for d in _dname:
            proj.update({d: 1})
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame(list(cursor))
        del cursor
        df.index = pd.to_datetime(df.date + ' ' + df.time)
        df = df[_dname]
        return df[dname] if isinstance(dname, str) else df


class IndexIntervalReturnsFetcher(KDayFetcher):
    """Class to fetch TinySoft index interal returns data.

    :param str freq: Frequency of interval returns, currently only supports: ('1min', '5min', '15min', '30min', '60min', '120min')
    """

    freqs = ('1min', '5min', '15min', '30min', '60min', '120min')

    index_dname = {
            'SHZS': 'SH000001',
            'SZZS': 'SZ399107',
            'ZXBZ': 'SZ399005',
            'CYBZ': 'SZ399006',
            'JCAZ': 'SZ399317',
            'HS300':'SH000300',
            'SH50': 'SH000016',
            }

    def __init__(self, freq, **kwargs):
        if freq not in IndexIntervalReturnsFetcher.freqs:
            raise ValueError('No interval returns of frequency {0!r} exists'.format(freq))
        self._freq = freq
        self._dname = 'returns'+freq[:-3]
        self.collection = DB.tsindex_ret
        super(IndexIntervalReturnsFetcher, self).__init__(**kwargs)
        if self.datetime_index is not True:
            self.datetime_index = True
            self.debug('Force self.datetime_index to be True')

    @property
    def freq(self):
        """Property."""
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq not in IndexIntervalReturnsFetcher.freqs:
            self.warning('No interval returns of frequency {0!r} exists. Nothing has changed'.format(freq))
            return
        self._freq = freq
        self._dname = 'returns'+freq[:-3]

    def fetch(self, *args, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)
        enddate = kwargs.get('enddate', None)
        backdays = kwargs.get('backdays', 0)
        try:
            startdate = dateutil.compliment_datestring(str(args[0]), -1, date_check)
            dname = self._dname
            args = args[1:]
        except ValueError:
            startdate = dateutil.compliment_datestring(str(args[1]), -1, date_check)
            dname = args[0]
            args = args[2:]
        if args:
            enddate = args[0]
        window = dateutil.cut_window(
                DATES,
                startdate,
                dateutil.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, window, **kwargs)

    def fetch_window(self, *args, **kwargs):
        assert 'index' in kwargs
        index = kwargs['index']
        index = IndexIntervalReturnsFetcher.index_dname.get(index, index)
        if not isinstance(args[0], str):    # dname not given
            dname = self._dname
            window = args[0]
        else:
            dname = args[0]
            window = args[1]
        query = {'index': index, 'dname': dname, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'date': 1, 'dvalue': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['date']: row['dvalue'] for row in cursor})
        del cursor
        s = df.unstack()
        s.index = pd.to_datetime(pd.Series(s.index.get_level_values(0)) + ' ' + \
                                 pd.Series(s.index.get_level_values(1)))
        return s.sort_index()

    def fetch_history(self, *args, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)
        try:
            date = dateutil.compliment_datestring(args[0], -1, date_check)
            backdays = args[1]
            dname = self._dname
        except ValueError:
            date = dateutil.compliment_datestring(args[1], -1, date_check)
            backdays = args[2]
            dname = args[0]

        date = dateutil.compliment_datestring(date, -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        return self.fetch_window(dname, window, **kwargs)

    def fetch_daily(self, *args, **kwargs):
        args = args + (1,)
        kwargs['delay'] = kwargs.get('offset', 0)
        return self.fetch_history(*args, **kwargs)
