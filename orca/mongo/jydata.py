"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import (
        DB,
        DATES,
        )
from orca.utils import dateutil

from base import KDayFetcher


class JYDataFetcher(KDayFetcher):
    """Class to fetch derivative JYDB fundamental data."""

    def __init__(self, **kwargs):
        super(JYDataFetcher, self).__init__(**kwargs)
        self.collection = DB.jydata

    def fetch(self, dname, startdate, enddate=None, backdays=0, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        date_check = kwargs.get('date_check', self.date_check)

        window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, window, **kwargs)

    def fetch_window(self, dname, window, dtype=None, **kwargs):
        """Variant of :py:meth:`orca.mongo.base.KDayFetcher.fetch_window`, as the collection is structured differently in MongoDB.

        :param dname: Data name or a list of data names
        :type dname: str, list
        :param str dtype: A signature attached to ``dname``
        """
        assert dtype
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        reindex = kwargs.get('reindex', self.reindex)
        start = DATES[DATES.index(window[0])-120]

        query = {'date': {'$lte': window[-1], '$gte': start}, 'dtype': dtype}
        df = pd.DataFrame(list(self.collection.find(query, {'_id': 0})))
        if isinstance(dname, str):
            df = df.pivot('date', 'sid', dname)
            index = sorted(list(set(df.index) | set(window)))
            df = df.reindex(index=index).fillna(method='ffill').ix[window]
            return self.format(df, datetime_index, reindex)
        else:
            res = {}
            for _dname in dname:
                _df = df.pivot('date', 'sid', _dname)
                index = sorted(list(set(_df.index) | set(window)))
                _df = _df.reindex(index=index).fillna(method='ffill').ix[window]
                res[_dname] = self.format(_df, datetime_index, reindex)
            return pd.Panel(res)

    def fetch_history(self, dname, date, backdays, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)

        date = dateutil.compliment_datestring(date, -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        return self.fetch_window(dname, window, **kwargs)

    def fetch_daily(self, dname, date, offset=0, **kwargs):
        """Use :py:meth:`fetch_window` behind the scene."""
        return self.fetch_history(dname, date, 1, delay=offset, **kwargs).iloc[0]
