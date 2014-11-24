"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc
import logging

import pandas as pd

from orca import DATES
from orca import logger
from orca.utils import dateutil
from orca.operation.api import format


class AlphaBase(object):
    """Base class for alphas.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'alpha'

    def __init__(self, debug_on=True):
        self.logger = logger.get_logger(AlphaBase.LOGGER_NAME)
        self.set_debug_mode(debug_on)

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

    def set_debug_mode(self, debug_on):
        """Enable/Disable debug level messages in the alpha.
        This is enabled by default."""
        level = logging.DEBUG if debug_on else logging.INFO
        self.logger.setLevel(level)

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


class BacktestingAlpha(AlphaBase):
    """Base class for backtesting alphas.

    .. note::

       This is a base class and should not be used directly
    """

    def __init__(self, *args, **kwargs):
        super(BacktestingAlpha, self).__init__(**kwargs)
        self.dates = None
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

    def dump(self, fpath):
        with open(fpath, 'w') as file:
            self.get_alphas.to_csv(file)

    def run(self, startdate=None, enddate=None, parallel=False, dates=None):
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

        if not parallel:
            for date in dates:
                self.generate(date)
            return
        #TODO


class ProductionAlpha(AlphaBase):
    """Base class for production alpha.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, *args, **kwargs):
        super(ProductionAlpha, self).__init__(**kwargs)
