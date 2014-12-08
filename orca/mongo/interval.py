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
        """Property."""
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq not in ('5min', '1min'):
            self.warning('No minute-bar data of frequency {0!r} exists. Nothing has changed'.format(freq))
            return
        self._freq = freq
        self.collection = IntervalFetcher.collections[freq]


class IntervalReturnsFetcher(KMinFetcher):
    """Class to fetch TinySoft interval returns data.

    :param str freq: Frequency of interval returns, currently only supports: ('1min', '5min', '15min', '30min', '60min', '120min')
    """

    freqs = ('1min', '5min', '15min', '30min', '60min', '120min')

    def __init__(self, freq, **kwargs):
        if freq not in IntervalReturnsFetcher.freqs:
            raise ValueError('No interval returns of frequency {0!r} exists'.format(freq))
        self._freq = freq
        self._dname = 'returns'+freq[:-3]
        self.collection = DB.ts_ret
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

    def fetch(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KMinFetcher.fetch`: when ``dname`` is omitted, it uses the :py:attr:`IntervalReturnsFetcher.freq` to determine which data to fetch."""
        if isinstance(args[0], str):    # dname is given
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
        if isinstance(args[0], str):    # dname is given
            return super(IntervalReturnsFetcher, self).fetch_history(*args, **kwargs)
        else:
            return super(IntervalReturnsFetcher, self).fetch_history(*((self._dname,)+args), **kwargs)

    def fetch_daily(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KMinFetcher.fetch_daily`: when ``dname`` is omitted, it uses the :py:attr:`IntervalReturnsFetcher.freq` to determine which data to fetch."""
        if isinstance(args[0], str):    # dname is given
            return super(IntervalReturnsFetcher, self).fetch_daily(*args, **kwargs)
        else:
            return super(IntervalReturnsFetcher, self).fetch_daily(*((self._dname,)+args), **kwargs)
