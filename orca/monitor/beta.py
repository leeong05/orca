"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import DATES
from orca.utils import dateutil

from orca.monitor.base import MonitorFetcherBase


class BetaFetcher(MonitorFetcherBase):

    def __init__(self, **kwargs):
       super(BetaFetcher, self).__init__(**kwargs)

    def fetch(self, dname, startdate, enddate=None, backdays=0, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)
        window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, window, **kwargs)

    def fetch_window(self, dname, window, **kwargs):
        datetime_index = kwargs.get('datetime_index', self.datetime_index)

        cursor = self.MONITOR.cursor()
        sql = ("SELECT trading_day, industry, {} FROM industry_beta WHERE "
               "trading_day>='{}' AND trading_day<='{}'").format(dname, window[0], window[-1])
        cursor.execute(sql)
        df = pd.DataFrame(list(cursor), columns=['date', 'industry', 'value'])
        df = df.pivot('date', 'industry', 'value')
        df.index.name, df.columns.name = None, None

        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df

    def fetch_history(self, dname, date, backdays, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)

        date = dateutil.compliment_datestring(str(date), -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1:di+1]
        return self.fetch_window(dname, window, **kwargs)

    def fetch_daily(self, dname, date, offset=0, **kwargs):
        return self.fetch_history(dname, date, 1, delay=offset, **kwargs).iloc[0]
