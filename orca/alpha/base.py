"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc

import pandas as pd
import logbook
logbook.set_datetime_format('local')

from orca import DATES
from orca.utils import dateutil
from orca.operation.api import format


class AlphaBase(object):
    """Base class for alphas.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'alpha'

    def __init__(self, **kwargs):
        self.logger = logbook.Logger(AlphaBase.LOGGER_NAME)
        self.__dict__.update(kwargs)

    @abc.abstractmethod
    def generate(self, date):
        """Override (**mandatory**) to generate alpha on a particular day.

        **This is the sole interface of an alpha class**.

        :param str date: a 8-length date string like ``yyyymmdd``
        :raises: NotImplementedError
        """
        raise NotImplementedError

    @staticmethod
    def get_date(date, offset=0):
        """Convenient method to get date with offset."""
        di = dateutil.parse_date(DATES, str(date))[0]
        if di >= offset and len(DATES)-1 >= di+offset:
            return DATES[di-offset]
        raise ValueError('Can\'t get date with the specified offset')

    def debug(self, msg):
        """Logs a message with level DEBUG on the alpha logger."""
        self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the alpha logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the alpha logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the alpha logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the alpha logger."""
        self.logger.critical(msg)


class IntervalAlphaBase(AlphaBase):
    """Base class for interval alphas.

    .. note::

       This is a base class and should not be used directly
    """

    freqs = ('1min', '5min', '15min', '30min', '60min', '120min')

    def __init__(self, freq, **kwargs):
        super(IntervalAlphaBase, self).__init__(**kwargs)
        if freq not in IntervalAlphaBase.freqs:
            raise ValueError('Frequency {0!r} is currently not supported'.format(freq))
        self.freq = 60 * int(freq[:-3])
        self.times = dateutil.generate_intervals(self.freq)

    @abc.abstractmethod
    def generate(self, date, time):
        """Override (**mandatory**) to generate alpha on a particular day at particular time.

        **This is the sole interface of an interval alpha class**.

        :param str date: a 8-length date string like ``yyyymmdd``
        :param str time: a 6-length time string like ``hhmmss``
        :raises: NotImplementedError
        """
        raise NotImplementedError

    @staticmethod
    def make_datetime(date, time):
        """Convenient method to combine date and time string into datetime"""
        return pd.to_datetime(date+' '+time)


class BacktestingAlpha(AlphaBase):
    """Base class for backtesting alphas.

    .. note::

       This is a base class and should not be used directly
    """

    def __init__(self, *args, **kwargs):
        super(BacktestingAlpha, self).__init__(**kwargs)
        self.alphas = {}
        self._alphas = None

    def get_alphas(self):
        """Return the generated alphas in a DataFrame."""
        if self._alphas is not None:
            return self._alphas

        df = format(pd.DataFrame(self.alphas).T)
        self._alphas = df
        return df

    def __getitem__(self, key):
        return self.alphas[key]

    def __setitem__(self, key, value):
        if key in self.alphas:
            self.warning('{0!r} already exists as a key'.format(key))
        self.alphas[key] = value

    def dump(self, fpath, ftype='csv'):
        with open(fpath, 'w') as file:
            if ftype == 'csv':
                self.get_alphas().to_csv(file)
            elif ftype == 'pickle':
                self.get_alphas().to_pickle(file)
            elif ftype == 'msgpack':
                self.get_alphas().to_msgpack(file)
        self.info('Saved in {}'.format(fpath))

    def run(self, startdate=None, enddate=None, dates=None):
        """Main interface to an alpha.

        :param dates list: One can supply this keyword argument with a list to omit ``startdate`` and ``enddate``
        """
        if dates is None:
            startdate, enddate = str(startdate), str(enddate)
            if enddate[:5].lower() == 'today':
                enddate = DATES[-1-int(enddate[6:])]

            dates = dateutil.cut_window(
                        DATES,
                        dateutil.compliment_datestring(str(startdate), -1, True),
                        dateutil.compliment_datestring(str(enddate), 1, True))

        for date in dates:
            self.generate(date)
            self.debug('Generated alpha for {}'.format(date))


class BacktestingIntervalAlpha(IntervalAlphaBase):
    """Base class for backtesting interval alphas.

    .. note::

       This is a base class and should not be used directly
    """

    def __init__(self, freq, *args, **kwargs):
        super(BacktestingIntervalAlpha, self).__init__(freq, **kwargs)
        self.alphas = {}
        self._alphas = None

    def get_alphas(self):
        """Return the generated alphas in a DataFrame."""
        if self._alphas is not None:
            return self._alphas

        df = format(pd.DataFrame(self.alphas).T)
        self._alphas = df
        return df

    def __getitem__(self, date, time):
        return self.alphas[self.make_datetime(date, time)]

    def __setitem__(self, key, value):
        if isinstance(key, tuple):
            key = self.make_datetime(*key)
        if key in self.alphas:
            self.warning('{0!r} already exists as a key'.format(key))
        self.alphas[key] = value

    def push(self, key, value):
        if isinstance(key, tuple):
            key = self.make_datetime(*key)
        for sid, val in value.dropna.iteritems():
            self.cursor.execute("""INSERT INTO alpha VALUES (?, ?, ?)""", key, sid, val)

    def dump(self, fpath, ftype='csv'):
        with open(fpath, 'w') as file:
            if ftype == 'csv':
                self.get_alphas().to_csv(file)
            elif ftype == 'pickle':
                self.get_alphas().to_pickle(file)
            elif ftype == 'msgpack':
                self.get_alphas().to_msgpack(file)
        self.info('Saved in {}'.format(fpath))

    def run(self, startdate=None, enddate=None, dates=None):
        """Main interface to an alpha.

        :param dates list: One can supply this keyword argument with a list to omit ``startdate`` and ``enddate``
        """
        if dates is None:
            startdate, enddate = str(startdate), str(enddate)
            if enddate[:5].lower() == 'today':
                enddate = DATES[-1-int(enddate[6:])]

            dates = dateutil.cut_window(
                        DATES,
                        dateutil.compliment_datestring(str(startdate), -1, True),
                        dateutil.compliment_datestring(str(enddate), 1, True))

        for date in dates:
            for time in self.times:
                self.generate(date, time)
            self.debug('Generated alpha for {}'.format(date))


class ProductionAlpha(AlphaBase):
    """Base class for production alpha.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, *args, **kwargs):
        super(ProductionAlpha, self).__init__(**kwargs)
