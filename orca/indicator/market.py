"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from base import IndicatorBase


class MarketIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=1, **kwargs):
        super(MarketIndicatorBase, self).__init__(offset=1, **kwargs)
        self.collection = self.db.market_indicator
        self.name = name

    def upsert(self, date, value, market=None):
        if market is None:
            market = 'ALL'
        self.collection.update(
                {'market': market, 'indicator': self.name, 'date': date},
                {'$set': {'value': value}},
                upsert=True)
