"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc
import logging

import pandas as pd

from orca import logger
from orca import (
        DATES,
        SIDS,
        )
import util

class FetcherBase(object):
    """Base class for mongo fetchers.

    :param boolean debug_on: Enable/Disable debug level messages. Default: True
    :param boolean datetime_index: Whether to use DatetimeIndex or list of date strings. Default: False
    :param boolean reindex: Whether to use full sids as columns in DataFrame. Default: False
    :param boolean date_check: Whethter to check if passed date-related parameters are valid. Default: False
    :param int delay: Delay fetched data in :py:meth:`~orca.mongo.base.FetcherBase.fetch_history`. Default: 1

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'mongo'

    def __init__(self, debug_on=True, datetime_index=False, reindex=False, date_check=False, delay=1):
        self.logger = logger.get_logger(FetcherBase.LOGGER_NAME)
        self.set_debug_mode(debug_on)
        self.datetime_index = datetime_index
        self.reindex = reindex
        self.date_check = date_check
        self.delay = delay

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
    def format(df, datetime_index, reindex):
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        if reindex:
            return df.reindex(columns=SIDS, copy=False)
        return df

    @abc.abstractmethod
    def fetch(self, dname, startdate, enddate=None, backdays=0, **kwargs):
        """Override(**mandatory**) to fetch data within two endpoints.

        :param str dname: Name of the data
        :param startdate: The **left** (may not be the actual) endpoint
        :type startdate: str, int
        :param enddate: The **right** endpoint. Default: None, defaults to the last date
        :type enddate: str, int, None
        :param int backdays: This will shift (left/right: >/< 0) the left endpoint. Default: 0
        :returns: DataFrame
        :raises: NotImplementedError

        .. seealso:: :py:func:`orca.mongo.util.cut_window`
        """
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_window(self, dname, window, **kwargs):
        """Override(**mandatory**) to fetch data for consecutive trading days.

        :param str dname: Name of the data
        :param list window: List of consecutive trading dates
        :returns: DataFrame
        :raises: NotImplementedError

        """
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_history(self, dname, date, backdays, **kwargs):
        """Override(**mandatory**) to fetch data with respect to a base point.

        :param str dname: Name of the data
        :param date: The date(with additional tweaks specified in ``kwargs`` and ``self.delay``) as a base point
        :type date: str, int
        :param int backdays: Number of days to look back w.r.t. the base point
        :returns: DataFrame
        :raises: NotImplementedError

        """
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_daily(self, dname, date, offset=0, **kwargs):
        """Override(**mandatory**) to fetch data series on a certain date.

        :param str dname: Name of the data
        :param date: The base point
        :type date: str, int
        :param int offset: The offset w.r.t. the ``date``. The actual fetched date is calculated from ``date`` and ``offset``. Default: 0
        :returns: Series
        :raises: NotImplementedError

        """
        raise NotImplementedError


class KDayFetcher(FetcherBase):
    """Base class to fetch daily data that can be formatted as DataFrame.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, **kwargs):
        super(KDayFetcher, self).__init__(**kwargs)

    def fetch(self, dname, startdate, enddate=None, backdays=0, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        date_check = kwargs.get('date_check', self.date_check)

        window = util.cut_window(
                DATES,
                util.compliment_datestring(str(startdate), -1, date_check),
                util.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, window, **kwargs)

    def fetch_window(self, dname, window, **kwargs):
        """Fetch data from a certain collection in MongoDB. For most cases, this is the **only** method that needs
        to be overridden.
        """
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        reindex = kwargs.get('reindex', self.reindex)

        query = {'dname': dname, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'dvalue': 1, 'date': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
        return self.format(df, datetime_index, reindex)

    def fetch_history(self, dname, date, backdays, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)

        date = util.compliment_datestring(date, -1, date_check)
        di, date = util.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        return self.fetch_window(dname, window, **kwargs)

    def fetch_daily(self, dname, date, offset=0, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        return self.fetch_history(dname, date, 1, delay=offset, **kwargs).iloc[0]


class KMinFetcher(FetcherBase):
    """Base class to fetch minute-bar interval data.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, **kwargs):
        super(KMinFetcher, self).__init__(**kwargs)

    def fetch(self, dname, times, startdate, enddate=None, backdays=0, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        date_check = kwargs.get('date_check', self.date_check)

        window = util.cut_window(
                DATES,
                util.compliment_datestring(str(startdate), -1, date_check),
                util.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, times, window, **kwargs)

    def fetch_window(self, dname, times, window, **kwargs):
        """Fetch minute-bar data(specified by time stamps) for a consecutive days.

        :param times: Time stamps to indicate which minute-bars should be fetched. This will affect the returned data type
        :type times: str, list
        :returns: DataFrame(if ``type(times)`` is ``str``) or Panel(with ``times`` as the item-axis)

        """
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        reindex = kwargs.get('reindex', self.reindex)

        query = {'dname': dname,
                 'date': {'$gte': window[0], '$lte': window[-1]},
                 'time': {'$in': [times] if isinstance(times, str) else times}}
        proj = {'_id': 0, 'dvalue': 1, 'date': 1, 'time': 1}
        cursor = self.collection.find(query, proj)
        dfs = pd.DataFrame({(row['date'], row['time']): row['dvalue'] for row in cursor}).T
        dfs.index.names = ['date', 'time']
        panel = dfs.to_panel().transpose(2, 1, 0)
        if datetime_index:
            panel.major_axis = pd.to_datetime(panel.major_axis)
            if reindex:
                panel = panel.reindex(major_axis=pd.to_datetime(window), minor_axis=SIDS, copy=False)
        if reindex:
            panel = panel.reindex(major_axis=window, minor_axis=SIDS)

        return panel[times] if isinstance(times, str) else panel

    def fetch_history(self, dname, times, date, backdays, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)

        date = util.compliment_datestring(date, -1, date_check)
        di, date = util.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        return self.fetch_window(dname, times, window, **kwargs)

    def fetch_daily(self, dname, times, date, offset=0, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene.

        :returns: Series(if ``type(times)`` is ``str``) or DataFrame(with ``times`` as the columns)

        """
        res = self.fetch_history(dname, times, date, 1, delay=offset, **kwargs)
        if isinstance(times, str):
            return res.iloc[0]
        return res.major_xs(res.major_axis[0])
