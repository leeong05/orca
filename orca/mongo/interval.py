"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DB
from orca.utils import dateutil

from base import KMinFetcher


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
