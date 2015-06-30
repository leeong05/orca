"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DATES
from orca.utils.dateutil import (
        find_le,
        find_ge,
        )

from base import IndicatorBase


class CAXIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=0, **kwargs):
        super(CAXIndicatorBase, self).__init__(offset=offset, **kwargs)
        self.collection = self.db.cax_indicator
        self.name = name

    def upsert(self, date, value, market=None):
        if market is None:
            market = 'ALL'
        self.collection.update(
                {'market': market, 'indicator': self.name, 'date': date},
                {'$set': {'value': value}},
                upsert=True)

    @staticmethod
    def adjust_date(date, direction=-1):
        if date is not None:
            return direction == -1 and find_le(DATES, date)[1] or find_ge(DATES, date)[1]
        return date

    @staticmethod
    def fetch_history(df, date, n):
        pdate = DATES[DATES.index(date)-n+1]
        return df.query('date >= {!r} & date <= {!r}'.format(pdate, date))
