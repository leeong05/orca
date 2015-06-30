"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from base import IndicatorBase


class BarraIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=1, **kwargs):
        super(BarraIndicatorBase, self).__init__(offset=offset, **kwargs)
        self.collection = self.db.barra_indicator
        self.name = name

    def upsert(self, date, value, market=None):
        if market is None:
            market = 'ALL'
        self.collection.update(
                {'market': market, 'indicator': self.name, 'date': date},
                {'$set': {'value': value}},
                upsert=True)
