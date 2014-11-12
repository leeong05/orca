"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from threading import Lock

from orca.mongo.quote import QuoteFetcher
from orca.mongo.index import IndexQuoteFetcher

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

    returns = quote.fetch('returns', startdate='20090101')
    index_returns = {
            'HS300': index_quote.fetch('returns', startdate='20090101', index='HS300'),
            }

    @classmethod
    def get_returns(cls, startdate):
        if startdate < cls.returns.index[0]:
            with cls.mongo_lock:
                cls.returns = cls.quote.fetch('returns', startdate=startdate.strftime('%Y%m%d'))
                return cls.returns
        return cls.returns

    @classmethod
    def get_index_returns(cls, startdate, index='HS300'):
        if index in cls.index_returns and startdate >= cls.index_returns[index].index[0]:
            return cls.index_returns[index]
        with cls.lock:
            cls.index_returns[index] = cls.quote.fetch('returns', startdate=startdate.strftime('%Y%m%d'))

    def __init__(self, alpha):
        if isinstance(alpha, AlphaBase):
            self.alpha = alpha.get_alphas()
        else:
            self.alpha = api.format(alpha)
        self.startdate = self.alpha.index[0]

    def get_original(self):
        """Be sure to use this method when either the alpha is neutralized or you know what you are doing."""
        return Analyser(self.alpha, Performance.get_returns(self.startdate))

    def get_long(self, index='HS300'):
        """Only analyse the long part."""
        return Analyser(self.alpha[self.alpha>0], Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_short(self, index='HS300'):
        """Only analyse the short part."""
        return Analyser(-self.alpha[self.alpha<0], Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_top(self, q, index='HS300'):
        """Only analyse the top quantile as long holding."""
        return Analyser(api.qtop(self.alpha, q), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_bottom(self, q, index='HS300'):
        """Only analyse the bottom quantile as long holding."""
        return Analyser(api.qbottom(self.alpha, q), Performance.get_returns(self.startdate),
                Performance.get_index_returns(self.startdate, index=index))

    def get_tail(self, q):
        """Long the top quantile and at the same time short the bottom quantile."""
        return Analyser(api.qtop(self.alpha, q) - api.qbottom(self.alpha, q),
                Performance.get_returns(self.startdate))

    def get_quantiles(self, n):
        """Return analyses for the ``n``-quantiles."""
        return [Analyser(qt, Performance.get_returns(self.startdate)) \
                for qt in api.quantiles(self.alpha, n)]
