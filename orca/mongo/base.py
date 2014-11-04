"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc
import logging

import pandas as pd

from orca import logger
from orca.mongo import util

class FetcherBase(object):
    """Base class for mongo fetchers.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'mongo'

    def __init__(self, **kwargs):
        self.logger = logger.get_logger(FetcherBase.LOGGER_NAME)
        self.set_debug_mode(kwargs.get('debug_on', True))
        self.datetime_index = kwargs.get('datetime_index', True)
        self.reindex = kwargs.get('reindex', True)
        self.date_check = kwargs.get('date_check', False)
        self.delay = kwargs.get('delay', 1)
        self.__dict__.update(kwargs)

    def set_debug_mode(self, debug_on):
        """Enable/Disable debug level message in data fetchers.
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

    @staticmethod
    @abc.abstractmethod
    def fetch_window(self, dname, window, **kwargs):
        """Override (**mandatory**) to fetch data ``dname`` with a specified date ``window``.

        :param str dname: the name of the data
        :param list window: the list of dates in the window
        :rtype: DataFrame
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def fetch(self, dname, startdate, enddate=None, **kwargs):
        """Override (**mandatory**) to fetch data ``dname`` within the interval [``startdate``, ``enddate``].

        :param str dname: the name of the data
        :param startdate: the start date, fetched data must be of date >= ``startdate``
        :type startdate: str, int
        :param enddate: the end date, fetched data must be of date <= ``enddate``
        :type enddate: str, int or None. Default: None, it will be set the last date
        :rtype: DataFrame
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def fetch_history(self, dname, date, backdays, **kwargs):
        """Override (**mandatory**) to fetch data ``dname`` with ``date`` as a base point.

        :param str dname: the name of the data
        :param date: the date(with additional tweaks specified in ``kwargs``) as a base point
        :type date: str, int
        :param int backdays: number of days to look back w.r.t. the base point
        :rtype: DataFrame
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def fetch_daily(self, dname, date, offset=0):
        """Override (**mandatory**) to fetch data ``dname`` on a certain date.

        :param str dname: the name of the data
        :param date:
        :type date: str, int
        :param int offset: the offset w.r.t. the ``date``. The actual fetched date is calculated from ``date`` and ``offset``. Default: 0
        :rtype: Series
        """
        raise NotImplementedError


class KDayFetcher(FetcherBase):
    pass
