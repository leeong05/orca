"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from threading import Lock

import numpy as np
import pandas as pd

from orca.mongo.quote import QuoteFetcher
from orca.mongo.index import IndexQuoteFetcher
from orca.mongo.interval import IntervalReturnsFetcher
from orca.mongo.index import IndexIntervalReturnsFetcher
from orca.mongo.components import ComponentsFetcher
from orca.operation import api
from orca.alpha.base import AlphaBase

from analyser import (
        Analyser,
        IntAnalyser,
        )


class Performance(object):
    """Class to provide analyser to examine the performance of an alpha from different perspective.

    :param alpha: Alpha to be examined, either a well formatted DataFrame or :py:class:`orca.alpha.base.AlphaBase`
    """

    mongo_lock = Lock()

    quote = QuoteFetcher(datetime_index=True, reindex=True)
    index_quote = IndexQuoteFetcher(datetime_index=True)
    components = ComponentsFetcher(datetime_index=True, reindex=True)

    returns = None
    index_returns = {
            'HS300': None,
            }
    index_components = {
            'HS300': None,
            'CS500': None,
            'other': None
            }

    @classmethod
    def get_returns(cls, startdate):
        if cls.returns is None or startdate < cls.returns.index[0]:
            with cls.mongo_lock:
                cls.returns = cls.quote.fetch('returns', startdate=startdate.strftime('%Y%m%d'))
        return cls.returns

    @classmethod
    def get_index_returns(cls, startdate, index='HS300'):
        if index not in cls.index_returns or cls.index_returns[index] is None or startdate < cls.index_returns[index].index[0]:
            with cls.mongo_lock:
                cls.index_returns[index] = cls.quote.fetch('returns', startdate=startdate.strftime('%Y%m%d'))
        return cls.index_returns[index]

    @classmethod
    def get_index_components(cls, startdate, index):
        if cls.index_components[index] is None or startdate < cls.index_components[index].index[0]:
            with cls.mongo_lock:
                cls.index_components['HS300'] = cls.components.fetch('HS300', startdate=startdate.strftime('%Y%m%d'))
                cls.index_components['CS500'] = cls.components.fetch('CS500', startdate=startdate.strftime('%Y%m%d'))
                cls.index_components['other'] = ~(cls.index_components['HS300'] | cls.index_components['CS500'])
        return cls.index_components[index]

    @classmethod
    def set_returns(cls, returns):
        """Call this method to set returns so that for future uses, there is no need to interact with MongoDB."""
        with cls.mongo_lock:
            cls.returns = api.format(returns)

    @classmethod
    def set_index_returns(cls, index, returns):
        """Call this method to set index returns so that for future uses, there is no need to interact with MongoDB."""
        with cls.mongo_lock:
            returns.index = pd.to_datetime(returns.index)
            cls.index_returns[index] = returns

    @classmethod
    def set_index_components(cls, index, components):
        """Call this method to set index components data so that for future uses, there is no need to interact with MongoDB."""
        with cls.mongo_lock:
            cls.index_components[index] = api.format(components).fillna(False)

    def __init__(self, alpha):
        if isinstance(alpha, AlphaBase):
            self.alpha = alpha.get_alphas()
        else:
            self.alpha = api.format(alpha)
        self.alpha = self.alpha[np.isfinite(self.alpha)]
        self.startdate = self.alpha.index[0]

    def get_original(self):
        """**Be sure** to use this method when either the alpha is neutralized or you know what you are doing."""
        return Analyser(self.alpha, Performance.get_returns(self.startdate))

    def get_longshort(self):
        """Pretend the alpha can be made into a long/short portfolio."""
        return Analyser(api.neutralize(self.alpha), Performance.get_returns(self.startdate))

    def get_long(self, index=None):
        """Only analyse the long part."""
        return Analyser(self.alpha[self.alpha>0], Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index)) \
               if index is not None else \
               Analyser(self.alpha[self.alpha>0], Performance.get_returns(self.startdate))

    def get_short(self, index=None):
        """Only analyse the short part."""
        return Analyser(-self.alpha[self.alpha<0], Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index)) \
               if index is not None else \
               Analyser(-self.alpha[self.alpha<0], Performance.get_returns(self.startdate))

    def get_qtop(self, q, index=None):
        """Only analyse the top quantile as long holding."""
        return Analyser(api.qtop(self.alpha, q), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index)) \
               if index is not None else \
               Analyser(api.qtop(self.alpha, q), Performance.get_returns(self.startdate))

    def get_qbottom(self, q, index=None):
        """Only analyse the bottom quantile as long holding."""
        return Analyser(api.qbottom(self.alpha, q), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index)) \
               if index is not None else \
               Analyser(api.qbottom(self.alpha, q), Performance.get_returns(self.startdate))

    def get_ntop(self, n, index=None):
        """Only analyse the top n stocks as long holding."""
        return Analyser(api.top(self.alpha, n), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index)) \
               if index is not None else \
               Analyser(api.top(self.alpha, n), Performance.get_returns(self.startdate))

    def get_nbottom(self, n, index=None):
        """Only analyse the bottom n stocks as long holding."""
        return Analyser(api.bottom(self.alpha, n), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index)) \
               if index is not None else \
               Analyser(api.bottom(self.alpha, n), Performance.get_returns(self.startdate))

    def get_qtail(self, q):
        """Long the top quantile and at the same time short the bottom quantile."""
        return Analyser(api.qtop(self.alpha, q).astype(int) - api.qbottom(self.alpha, q).astype(int),
                Performance.get_returns(self.startdate))

    def get_ntail(self, n):
        """Long the top n stocks and at the same time short the bottom n stocks."""
        return Analyser(api.top(self.alpha, n).astype(int) - api.bottom(self.alpha, n).astype(int),
                Performance.get_returns(self.startdate))

    def get_quantiles(self, n):
        """Return a list of analysers for n quantiles."""
        return [Analyser(qt, Performance.get_returns(self.startdate)) \
                for qt in api.quantiles(self.alpha, n)]

    def get_universe(self, univ):
        """Return a performance object for alpha in this universe."""
        return Performance(api.intersect(self.alpha, univ))

    def get_bms(self):
        """Return a list of 3 performance objects for alphas in HS300, CS500 and other."""
        big = Performance.get_index_components(self.startdate, 'HS300').ix[self.alpha.index]
        mid = Performance.get_index_components(self.startdate, 'CS500').ix[self.alpha.index]
        sml = Performance.get_index_components(self.startdate, 'other').ix[self.alpha.index]

        return [self.get_universe(univ) for univ in [big, mid, sml]]


class IntPerformance(object):
    """Class to provide analyser to examine the performance of an interval alpha from different perspective.

    :param alpha: Alpha to be examined, either a well formatted DataFrame or :py:class:`orca.alpha.base.AlphaBase`
    """

    mongo_lock = Lock()

    components = ComponentsFetcher(datetime_index=True, reindex=True)

    returns = {
            '1min': None,
            '5min': None,
            '30min': None,
            }
    index_returns = {
            'HS300': {'1min': None, '5min': None, '30min': None,},
            }

    @classmethod
    def get_returns(cls, startdate, freq):
        if cls.returns[freq] is None or startdate < cls.returns[freq].index[0].date():
            with cls.mongo_lock:
                returns_fetcher = IntervalReturnsFetcher(freq, datetime_index=True, reindex=True)
                cls.returns[freq] = returns_fetcher.fetch([], startdate=startdate.strftime('%Y%m%d'), as_frame=True)
        return cls.returns[freq]

    @classmethod
    def get_index_returns(cls, startdate, freq, index='HS300'):
        if index not in cls.index_returns or cls.index_returns[index][freq] is None or startdate < cls.index_returns[index][freq].index[0].date():
            with cls.mongo_lock:
                index_returns_fetcher = IndexIntervalReturnsFetcher(freq, datetime_index=True, reindex=True)
                cls.index_returns[index] = index_returns_fetcher.fetch('returns', startdate=startdate.strftime('%Y%m%d'))
        return cls.index_returns[index]

    @classmethod
    def set_returns(cls, freq, returns):
        """Call this method to set returns so that for future uses, there is no need to interact with MongoDB."""
        with cls.mongo_lock:
            cls.returns[freq] = api.format(returns)

    @classmethod
    def set_index_returns(cls, freq, index, returns):
        """Call this method to set index returns so that for future uses, there is no need to interact with MongoDB."""
        with cls.mongo_lock:
            returns.index = pd.to_datetime(returns.index)
            cls.index_returns[index][freq] = returns

    def __init__(self, alpha):
        if isinstance(alpha, AlphaBase):
            self.alpha = alpha.get_alphas()
        else:
            self.alpha = api.format(alpha)
        self.dates = np.unique(self.alpha.index.date)
        self.startdate = self.dates[0]
        self.freq = len(self.alpha) / len(self.dates)
        self.freq = str(240 / self.freq) + 'min'

    def get_original(self):
        """**Be sure** to use this method when either the alpha is neutralized or you know what you are doing."""
        return IntAnalyser(self.alpha, IntPerformance.get_returns(self.startdate, self.freq))

    def get_longshort(self):
        """Pretend the alpha can be made into a long/short portfolio."""
        return IntAnalyser(api.neutralize(self.alpha), IntPerformance.get_returns(self.startdate, self.freq))

    def get_long(self, index=None):
        """Only analyse the long part."""
        return IntAnalyser(self.alpha[self.alpha>0], IntPerformance.get_returns(self.startdate, self.freq),
                IntPerformance.get_index_returns(self.startdate, self.freq, index=index)) \
               if index is not None else \
               IntAnalyser(self.alpha[self.alpha>0], IntPerformance.get_returns(self.startdate, self.freq))

    def get_short(self, index=None):
        """Only analyse the short part."""
        return IntAnalyser(-self.alpha[self.alpha<0], IntPerformance.get_returns(self.startdate, self.freq),
                IntPerformance.get_index_returns(self.startdate, self.freq, index=index)) \
               if index is not None else \
               IntAnalyser(-self.alpha[self.alpha<0], IntPerformance.get_returns(self.startdate, self.freq))

    def get_qtop(self, q, index=None):
        """Only analyse the top quantile as long holding."""
        return IntAnalyser(api.qtop(self.alpha, q), IntPerformance.get_returns(self.startdate, self.freq),
                IntPerformance.get_index_returns(self.startdate, self.freq, index=index)) \
               if index is not None else \
               IntAnalyser(api.qtop(self.alpha, q), IntPerformance.get_returns(self.startdate, self.freq))

    def get_qbottom(self, q, index=None):
        """Only analyse the bottom quantile as long holding."""
        return IntAnalyser(api.qbottom(self.alpha, q), IntPerformance.get_returns(self.startdate, self.freq),
                IntPerformance.get_index_returns(self.startdate, self.freq, index=index)) \
               if index is not None else \
               IntAnalyser(api.qbottom(self.alpha, q), IntPerformance.get_returns(self.startdate, self.freq))

    def get_ntop(self, n, index=None):
        """Only analyse the top n stocks as long holding."""
        return IntAnalyser(api.top(self.alpha, n), IntPerformance.get_returns(self.startdate, self.freq),
                IntPerformance.get_index_returns(self.startdate, self.freq, index=index)) \
               if index is not None else \
               IntAnalyser(api.top(self.alpha, n), IntPerformance.get_returns(self.startdate, self.freq))

    def get_nbottom(self, n, index=None):
        """Only analyse the bottom n stocks as long holding."""
        return IntAnalyser(api.bottom(self.alpha, n), IntPerformance.get_returns(self.startdate, self.freq),
                IntPerformance.get_index_returns(self.startdate, self.freq, index=index)) \
               if index is not None else \
               IntAnalyser(api.bottom(self.alpha, n), IntPerformance.get_returns(self.startdate, self.freq))

    def get_qtail(self, q):
        """Long the top quantile and at the same time short the bottom quantile."""
        return IntAnalyser(api.qtop(self.alpha, q).astype(int) - api.qbottom(self.alpha, q).astype(int),
                IntPerformance.get_returns(self.startdate, self.freq))

    def get_ntail(self, n):
        """Long the top n stocks and at the same time short the bottom n stocks."""
        return IntAnalyser(api.top(self.alpha, n).astype(int) - api.bottom(self.alpha, n).astype(int),
                IntPerformance.get_returns(self.startdate, self.freq))

    def get_quantiles(self, n):
        """Return a list of analysers for n quantiles."""
        return [IntAnalyser(qt, IntPerformance.get_returns(self.startdate, self.freq)) \
                for qt in api.quantiles(self.alpha, n)]

    def get_universe(self, univ):
        """Return a performance object for alpha in this universe."""
        return IntPerformance(api.intersect_interval(self.alpha, univ))
