"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc
import logging

import pandas as pd

from orca import logger

class AlphaBase(object):
    """Base class for alphas.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'alpha'

    def __init__(self, **kwargs):
        self.logger = logger.get_logger(AlphaBase.LOGGER_NAME)
        self.set_debug_mode(kwargs.get('debug_on', True))
        self.__dict__.update(kwargs)

    @abc.abstractmethod
    def generate(self, date):
        """Override (**mandatory**) to generate alpha on a particular day.

        **This is the sole interface of an alpha class**.

        :param str date: a 8-length date string like ``yyyymmdd``.
        """
        raise NotImplementedError

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
        AlphaBase.__init__(self, **kwargs)
        self.alphas = {}

    def get_alphas(self, datetime_index=True):
        """Return the generated alphas in a DataFrame.

        :param boolean datetime_index: Whether to format the DataFrame with DatetimeIndex. Default: True
        """
        df = pd.DataFrame(self.alphas).T
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df


class ProductionAlpha(AlphaBase):
    """Base class for production alpha.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, *args, **kwargs):
        AlphaBase.__init__(self, **kwargs)
