"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
from pandas.tseries.index import DatetimeIndex

from orca import (
        DB,
        DATES,
        )
from orca.utils import dateutil

from base import KMinFetcher
from kday import CaxFetcher


class IntervalFetcher(KMinFetcher):
    """Class to fetch TinySoft minute-bar data.

    :param str freq: Frequency of minute-bar data, currently only supports: ('30min', '5min', '1min')
    """

    collections = {
            '30min': DB.ts_30min,
            '5min': DB.ts_5min,
            '1min': DB.ts_1min,
            }
    intervals = {
            '30min': dateutil.generate_intervals(1800),
            '5min': dateutil.generate_intervals(300),
            '1min': dateutil.generate_intervals(60),
            }
    dnames = DB.ts_30min.distinct('dname')
    freqs = ('30min', '5min', '1min')

    def __init__(self, freq, **kwargs):
        if freq not in IntervalFetcher.freqs:
            raise ValueError('No minute-bar data of frequency {0!r} exists'.format(freq))
        self._freq = freq
        self.collection = IntervalFetcher.collections[freq]
        self.intervals = IntervalFetcher.intervals[freq]
        super(IntervalFetcher, self).__init__(**kwargs)

    @property
    def freq(self):
        """Property."""
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq not in IntervalFetcher.freqs:
            self.warning('No minute-bar data of frequency {0!r} exists. Nothing has changed'.format(freq))
            return
        self._freq = freq
        self.collection = IntervalFetcher.collections[freq]
        self.intervals = IntervalFetcher.intervals[freq]


class IntervalReturnsFetcher(KMinFetcher):
    """Class to fetch TinySoft interval returns data.

    :param str freq: Frequency of interval returns, currently only supports: ('1min', '5min', '15min', '30min', '60min', '120min')
    """

    freqs = ('1min', '5min', '15min', '30min', '60min', '120min')
    intervals = {
            '1min': dateutil.generate_intervals(60),
            '5min': dateutil.generate_intervals(300),
            '15min': dateutil.generate_intervals(15*60),
            '30min': dateutil.generate_intervals(30*60),
            '60min': dateutil.generate_intervals(60*60),
            '120min': dateutil.generate_intervals(120*60),
            }

    def __init__(self, freq, **kwargs):
        if freq is not None and freq not in IntervalReturnsFetcher.freqs:
            raise ValueError('No interval returns of frequency {0!r} exists'.format(freq))
        self._freq = freq
        self._dname = 'returns'+freq[:-3]
        self.collection = DB.ts_ret
        self.intervals = IntervalReturnsFetcher.intervals[freq]
        super(IntervalReturnsFetcher, self).__init__(**kwargs)

    @property
    def freq(self):
        """Property."""
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq not in IntervalReturnsFetcher.freqs:
            self.warning('No interval returns of frequency {0!r} exists. Nothing has changed'.format(freq))
            return
        self._freq = freq
        self._dname = 'returns'+freq[:-3]
        self.intervals = IntervalReturnsFetcher.intervals[freq]

    def fetch(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KMinFetcher.fetch`: when ``dname`` is omitted, it uses the :py:attr:`IntervalReturnsFetcher.freq` to determine which data to fetch."""
        if isinstance(args[0], str) and args[0][:7] == 'returns':    # dname is given
            return super(IntervalReturnsFetcher, self).fetch(*args, **kwargs)
        else:
            return super(IntervalReturnsFetcher, self).fetch(*((self._dname,)+args), **kwargs)

    def fetch_window(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KMinFetcher.fetch_window`: when ``dname`` is omitted, it uses the :py:attr:`IntervalReturnsFetcher.freq` to determine which data to fetch."""
        if isinstance(args[0], str):    # dname is given
            return super(IntervalReturnsFetcher, self).fetch_window(*args, **kwargs)
        else:
            return super(IntervalReturnsFetcher, self).fetch_window(*((self._dname,)+args), **kwargs)

    def fetch_history(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KMinFetcher.fetch_history`: when ``dname`` is omitted, it uses the :py:attr:`IntervalReturnsFetcher.freq` to determine which data to fetch."""
        if isinstance(args[0], str) and args[0][:7] == 'returns':    # dname is given
            return super(IntervalReturnsFetcher, self).fetch_history(*args, **kwargs)
        else:
            return super(IntervalReturnsFetcher, self).fetch_history(*((self._dname,)+args), **kwargs)

    def fetch_daily(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KMinFetcher.fetch_daily`: when ``dname`` is omitted, it uses the :py:attr:`IntervalReturnsFetcher.freq` to determine which data to fetch."""
        if isinstance(args[0], str) and args[0][:7] == 'returns':    # dname is given
            return super(IntervalReturnsFetcher, self).fetch_daily(*args, **kwargs)
        else:
            return super(IntervalReturnsFetcher, self).fetch_daily(*((self._dname,)+args), **kwargs)

    def fetch_intervals(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KMinFetcher.fetch_daily`: when ``dname`` is omitted, it uses the :py:attr:`IntervalReturnsFetcher.freq` to determine which data to fetch."""
        if isinstance(args[0], str) and args[0][:7] == 'returns':    # dname is given
            return super(IntervalReturnsFetcher, self).fetch_intervals(*args, **kwargs)
        else:
            return super(IntervalReturnsFetcher, self).fetch_intervals(*((self._dname,)+args), **kwargs)


class AdjIntervalFetcher(KMinFetcher):
    """Class to fetch adjusted interval data."""

    _prices = ('ask1', 'ask2', 'ask3', 'ask4', 'ask5',
               'bid1', 'bid2', 'bid3', 'bid4', 'bid5',
               'close', 'high', 'low', 'open', 'vwap')  # 15
    _volumes = ('aks1', 'aks2', 'aks3', 'aks4', 'aks5',
                'bds1', 'bds2', 'bds3', 'bds4', 'bds5',
                'bvolume', 'svolume', 'volume', 'iwbds', 'iwaks')   # 15
    _noadjust = ('returns', 'bamount', 'samount', 'amount') # 4

    dnames = ['adj_' + dname for dname in _prices+_volumes+_noadjust]

    def __init__(self, freq, **kwargs):
        super(AdjIntervalFetcher, self).__init__(**kwargs)
        self.quote = IntervalFetcher(freq, **kwargs)
        self.cax = CaxFetcher(**kwargs)
        self.returns = IntervalReturnsFetcher(freq, **kwargs)

    def _get_adjust_window(self, window, basedate=None, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)

        if basedate is None:
            basedate = window[-1]
            adj_window = window
        else:
            sdi, startdate = dateutil.parse_date(DATES, window[0], 1)
            edi, enddate = dateutil.parse_date(DATES, window[-1], -1)
            basedate = dateutil.compliment_datestring(str(basedate), -1, date_check)
            bdi, basedate = dateutil.parse_date(DATES, basedate, -1)
            if bdi < edi:
                raise ValueError('basedate {!r} cannot be smaller than enddate {!r}'.format(basedate, startdate))
            adj_window = DATES[sdi: bdi+1]
        return basedate, adj_window

    @staticmethod
    def _adjust_price(price, adj_factor, date):
        if isinstance(price, pd.DataFrame):
            adj_factor = adj_factor.ix[:, price.columns].fillna(1)
            adj_price = adj_factor.ix[price.index] * price
            adj_price = adj_price.div(adj_factor.ix[date], axis=1)
        else:
            adj_price = {}
            adj_factor = adj_factor.ix[:, price.minor_axis].fillna(1)
            for time, subdf in price.iteritems():
                subdf = adj_factor.ix[subdf.index] * subdf
                subdf = subdf.div(adj_factor.ix[date], axis=1)
                adj_price[time] = subdf
            adj_price = pd.Panel(adj_price).reindex(items=price.items)
        return adj_price

    @staticmethod
    def _adjust_volume(volume, adj_factor, date):
        adj_factor = 1. / adj_factor
        if isinstance(volume, pd.DataFrame):
            adj_factor = adj_factor.ix[:, volume.columns].fillna(1)
            adj_volume = adj_factor.ix[volume.index] * volume
            adj_volume = adj_volume.div(adj_factor.ix[date], axis=1)
        else:
            adj_volume = {}
            adj_factor = adj_factor.ix[:, volume.minor_axis].fillna(1)
            for time, subdf in volume.iteritems():
                subdf = adj_factor.ix[subdf.index] * subdf
                subdf = subdf.div(adj_factor.ix[date], axis=1)
                adj_volume[time] = subdf
            adj_volume = pd.Panel(adj_volume).reindex(items=volume.items)
        return adj_volume

    def fetch(self, dname, times, startdate, enddate=None, backdays=None, basedate=None, **kwargs):
        """
        :param basedate: Date on which the data adjusting is based. Default: None, defaults to ``enddate``
        :type basedate: str, None
        """
        if dname[4:] in self._noadjust:
            if dname[4:] == 'returns':
                return self.returns.fetch(times, startdate, enddate=enddate, backdays=backdays, **kwargs)
            else:
                return self.quote.fetch(dname[4:], times, startdate, enddate=enddate, backdays=backdays, **kwargs)

        date_check = kwargs.get('date_check', self.date_check)
        window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), -1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, times, window, basedate=basedate, **kwargs)

    def fetch_window(self, dname, times, window, basedate=None, **kwargs):
        """
        :param str basedate: Date on which the data adjusting is based. When it is None, it defaults to the last day in ``window``
        """
        dname = dname[4:]
        if dname in self._noadjust:
            if dname == 'returns':
                return self.returns.fetch_window(times, window, **kwargs)
            else:
                return self.quote.fetch_window(dname, times, window, **kwargs)

        basedate, adj_window = self._get_adjust_window(window, basedate=basedate, **kwargs)

        qkwargs = kwargs.copy()
        qkwargs['as_frame'] = False
        res = self.quote.fetch_window(dname, times, window, **qkwargs)
        if dname in self._prices:
            adj = self.cax.fetch_window('adjfactor', adj_window, **kwargs)
            res = self._adjust_price(res, adj, basedate)
        elif dname in self._volumes:
            adj = self.cax.fetch_window('volfactor', adj_window, **kwargs)
            res = self._adjust_volume(res, adj, basedate)
        else:
            raise ValueError('{!r} is not a valid data name'.format(dname))

        if isinstance(times, str):
            return res
        return self.to_frame(res) if kwargs.get('as_frame', False) else res

    def fetch_history(self, dname, times, date, backdays, **kwargs):
        if dname[4:] in self._noadjust:
            if dname[4:] == 'returns':
                return self.returns.fetch_history(times, date, backdays, **kwargs)
            else:
                return self.quote.fetch_history(dname[4:], times, date, backdays, **kwargs)

        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)
        date = dateutil.compliment_datestring(str(date), -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        return self.fetch_window(dname, window, basedate=date, **kwargs)

    def fetch_daily(self, *args, **kwargs):
        """Non-sensical to fetch just one day adjusted data.

        :raises: NotImplementedError
        """
        raise NotImplementedError

    def fetch_intervals(self, dname, date, time, num=None, offset=0, basedate=None, **kwargs):
        dname = dname[4:]
        if dname in self._noadjust:
            if dname == 'returns':
                return self.returns.fetch_intervals(date, time, num=num, offset=offset, **kwargs)
            else:
                return self.quote.fetch_intervals(dname, date, time, num=num, offset=offset, **kwargs)

        date_check = kwargs.get('date_check', self.date_check)
        date = dateutil.compliment_datestring(date, -1, date_check)
        if basedate is None:
            basedate = date
        res = self.quote.fetch_intervals(dname, date, time, num=1 if num is None else num, offset=offset, **kwargs)

        window = sorted(list(set([dt.strftime('%Y%m%d') for dt in res.index])))
        index = res.iloc[:, 0].resample('D', how=lambda x: x.index[0]).values
        basedate, adj_window = self._get_adjust_window(window, basedate=basedate, date_check=False)
        if dname in self._prices:
            adj = self.cax.fetch_window('adjfactor', adj_window, **kwargs)
        elif dname in self._volumes:
            adj = self.cax.fetch_window('volfactor', adj_window, **kwargs)
            adj = 1./adj
        else:
            raise ValueError('{!r} is not a valid data name'.format(dname))

        adj = adj.ix[:, res.columns].fillna(1)
        if isinstance(adj.index, DatetimeIndex):
            adj, base = adj.ix[pd.to_datetime(window)], adj.ix[pd.to_datetime(basedate)]
        else:
            adj, base = adj.ix[window], adj.ix[basedate]
        adj.index = index
        adj = adj.reindex(index=res.index).fillna(method='ffill')
        res = res * adj
        res = res.div(base, axis=1)
        return res.iloc[0] if num is None else res
