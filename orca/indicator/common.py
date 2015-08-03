"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from base import IndicatorBase


class AnalystIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=1, **kwargs):
        super(AnalystIndicatorBase, self).__init__(offset=offset, **kwargs)
        self.collection = self.db.analyst_indicator
        self.name = name


class BarraIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=1, **kwargs):
        super(BarraIndicatorBase, self).__init__(offset=offset, **kwargs)
        self.collection = self.db.barra_indicator
        self.name = name


class CAXIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=0, **kwargs):
        super(CAXIndicatorBase, self).__init__(offset=offset, **kwargs)
        self.collection = self.db.cax_indicator
        self.name = name


class FundIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=0, **kwargs):
        super(FundIndicatorBase, self).__init__(offset=offset, **kwargs)
        self.collection = self.db.fund_indicator
        self.name = name


class MarketIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=0, **kwargs):
        super(MarketIndicatorBase, self).__init__(offset=offset, **kwargs)
        self.collection = self.db.market_indicator
        self.name = name


class IntervalIndicatorBase(IndicatorBase):

    def __init__(self, name, offset=0, timeout=300, **kwargs):
        super(IntervalIndicatorBase, self).__init__(offset=offset, timeout=timeout, **kwargs)
        self.collection = self.db.interval_indicator
        self.name = name
