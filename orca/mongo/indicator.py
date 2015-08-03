"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import DB

from base import KDayFetcher


class IndicatorFetcher(KDayFetcher):
    """Base class to fetch time-series indicator data.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, **kwargs):
        super(IndicatorFetcher, self).__init__(**kwargs)
        self.indicators = self.collection.distinct('indicator')

    def fetch_window(self, indicator, window, market=None, **kwargs):
        if market is None:
            market = 'ALL'
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        query = {'indicator': indicator, 'market': market, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'value': 1, 'date': 1}
        cursor = self.collection.find(query, proj)
        ser = pd.Series({row['date']: row['value'] for row in cursor})
        del cursor
        if datetime_index:
            ser.index = pd.to_datetime(ser.index)
        return ser


class BarraIndicatorFetcher(IndicatorFetcher):

    def __init__(self, **kwargs):
        self.collection = DB.barra_indicator
        super(BarraIndicatorFetcher, self).__init__(**kwargs)


class MarketIndicatorFetcher(IndicatorFetcher):

    def __init__(self, **kwargs):
        self.collection = DB.market_indicator
        super(MarketIndicatorFetcher, self).__init__(**kwargs)


class AnalystIndicatorFetcher(IndicatorFetcher):

    def __init__(self, **kwargs):
        self.collection = DB.analyst_indicator
        super(AnalystIndicatorFetcher, self).__init__(**kwargs)


class CAXIndicatorFetcher(IndicatorFetcher):

    def __init__(self, **kwargs):
        self.collection = DB.cax_indicator
        super(CAXIndicatorFetcher, self).__init__(**kwargs)


class FundIndicatorFetcher(IndicatorFetcher):

    def __init__(self, **kwargs):
        self.collection = DB.fund_indicator
        super(FundIndicatorFetcher, self).__init__(**kwargs)


class IntervalIndicatorFetcher(IndicatorFetcher):

    def __init__(self, **kwargs):
        self.collection = DB.interval_indicator
        super(IntervalIndicatorFetcher, self).__init__(**kwargs)
