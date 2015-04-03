"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund>
"""

import pandas as pd

from orca import DATES
from orca.utils import dateutil

from orca.monitor.base import MonitorFetcherBase


class IndicatorFetcher(MonitorFetcherBase):

    def __init__(self, **kwargs):
       super(IndicatorFetcher, self).__init__(**kwargs)

    def fetch(self, indicator, startdate, enddate=None, backdays=0, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)
        window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(indicator, window, **kwargs)

    def fetch_window(self, indicator, window, **kwargs):
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        if not indicator:
            indicator = None
        try:
            cursor = self.MONITOR.cursor()
        except:
            self.connect_monitor()
            cursor = self.MONITOR.cursor()
        sql = ("SELECT * FROM indicator WHERE "
               "trading_day>='{}' AND trading_day<='{}'").format(window[0], window[-1])
        if isinstance(indicator, str) or isinstance(indicator, unicode):
            sql += " AND name={!r}".format(indicator)
        elif indicator is not None:
            sql += " AND name IN {!r}".format(tuple(indicator))
        cursor.execute(sql)
        df = pd.DataFrame(list(cursor), columns=['date', 'indicator', 'value'])
        df = df.pivot('date', 'indicator', 'value')
        df.index.name, df.columns.name = None, None

        if isinstance(indicator, str):
            df = df[indicator]
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df

    def fetch_history(self, indicator, date, backdays, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)

        date = dateutil.compliment_datestring(str(date), -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1:di+1]
        return self.fetch_window(indicator, window, **kwargs)

    def fetch_sql(self, sql):
        try:
            cursor = self.MONITOR.cursor()
        except:
            self.connect_monitor()
            cursor = self.MONITOR.cursor()
        cursor.execute(sql)
        return cursor
