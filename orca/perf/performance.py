"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from threading import Lock

import pandas as pd

from orca.mongo.quote import QuoteFetcher
from orca.mongo.index import IndexQuoteFetcher
from orca.mongo.components import ComponentsFetcher
from orca.operation import api
from orca.alpha.base import AlphaBase

from analyser import Analyser


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
            with cls.lock:
                cls.index_returns[index] = cls.quote.fetch('returns', startdate=startdate.strftime('%Y%m%d'))
        return cls.index_returns[index]

    @classmethod
    def get_index_components(cls, startdate, index):
        if cls.index_components[index] is None or startdate < cls.index_components[index].index[0]:
            with cls.lock:
                cls.index_components['HS300'] = cls.components.fetch('HS300', startdate=startdate.strftime('%Y%m%d'))
                cls.index_components['CS500'] = cls.components.fetch('CS500', startdate=startdate.strftime('%Y%m%d'))
                cls.index_components['other'] = ~(cls.index_components['HS300'] | cls.index_components['CS500'])
        return cls.index_components[index]

    @classmethod
    def set_returns(cls, returns):
        with cls.mongo_lock:
            cls.returns = api.format(returns)

    @classmethod
    def set_index_returns(cls, index, returns):
        with cls.mongo_lock:
            returns.index = pd.to_datetime(returns.index)
            cls.index_returns[index] = returns

    @classmethod
    def set_index_components(cls, index, components):
        with cls.mongo_lock:
            cls.index_components[index] = api.format(components).fillna(False)

    def __init__(self, alpha):
        if isinstance(alpha, AlphaBase):
            self.alpha = alpha.get_alphas()
        else:
            self.alpha = api.format(alpha)
        self.startdate = self.alpha.index[0]

    def get_original(self):
        """**Be sure** to use this method when either the alpha is neutralized or you know what you are doing."""
        return Analyser(self.alpha, Performance.get_returns(self.startdate))

    def get_longshort(self):
        """Pretend the alpha can be made into a long/short portfolio."""
        return Analyser(api.neutralize(self.alpha), Performance.get_returns(self.startdate))

    def get_long(self, index='HS300'):
        """Only analyse the long part."""
        return Analyser(self.alpha[self.alpha>0], Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_short(self, index='HS300'):
        """Only analyse the short part."""
        return Analyser(-self.alpha[self.alpha<0], Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_qtop(self, q, index='HS300'):
        """Only analyse the top quantile as long holding."""
        return Analyser(api.qtop(self.alpha, q), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_qbottom(self, q, index='HS300'):
        """Only analyse the bottom quantile as long holding."""
        return Analyser(api.qbottom(self.alpha, q), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_ntop(self, n, index='HS300'):
        """Only analyse the top n stocks as long holding."""
        return Analyser(api.top(self.alpha, n), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_nbottom(self, n, index='HS300'):
        """Only analyse the bottom n stocks as long holding."""
        return Analyser(api.bottom(self.alpha, n), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_qtail(self, q):
        """Long the top quantile and at the same time short the bottom quantile."""
        return Analyser(api.qtop(self.alpha, q) - api.qbottom(self.alpha, q),
                Performance.get_returns(self.startdate))

    def get_ntail(self, n):
        """Long the top n stocks and at the same time short the bottom n stocks."""
        return Analyser(api.top(self.alpha, n) - api.bottom(self.alpha, n),
                Performance.get_returns(self.startdate))

    def get_quantiles(self, n):
        """Return a list of analysers for n quantiles."""
        return [Analyser(qt, Performance.get_returns(self.startdate)) \
                for qt in api.quantiles(self.alpha, n)]

    def _universe(self, univ):
        """Return a performance object for alpha in this universe."""
        return Performance(api.intersect(self.alpha, univ))

    def get_bms(self):
        """Return a list of 3 performance objects for alphas in HS300, CS500 and other."""
        big = Performance.get_index_components(self.startdate, 'HS300').ix[self.alpha.index]
        mid = Performance.get_index_components(self.startdate, 'CS500').ix[self.alpha.index]
        sml = Performance.get_index_components(self.startdate, 'other').ix[self.alpha.index]

        return [self.get_universe(univ) for univ in [big, mid, sml]]
