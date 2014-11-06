"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import (
        SIDS,
        DAYS_IN_YEAR)
from orca.alpha import BacktestingAlpha
from orca.mongo import QuoteFetcher
from orca.utils import date
import util

class Analyser(object):
    """Class for alpha performance analysis.

    :param alpha: Alpha to be analysed
    :type alpha: DataFrame or :py:class:`orca.alpha.BacktestingAlpha`. For the latter,
    its method :py:meth:`orca.alpha.BacktestingAlpha.get_alphas` is called to return a
    DataFrame

    .. py:attribute:: IC

       A ``dict`` with key being the number of days, value being the IC time series.

    .. py:attribute:: rIC

       A ``dict`` with key being the number of days, value being the rank IC time series.

    .. py:attribute:: AC

       A ``dict`` with key being the number of days, value being the Auto-Correlation time series.

    .. py:attribute:: rAC

       A ``dict`` with key being the number of days, value being the rank Auto-Correlation time series.

    .. py:attribute:: turnover

       A ``Series`` of daily turnover.

    .. py:attribute:: returns

       A ``Series`` of daily returns without any cost considerations.
    """

    def __init__(self, alpha, **kwargs):
        if isinstance(alpha, BacktestingAlpha):
            alpha = alpha.get_alphas()
        else:
            alpha.index = date.to_datetime(alpha.index)
        self.alpha = alpha.reindex(columns=SIDS, copy=True)
        # scale alpha so that on each day, the absolute value sum is 1.0
        self.alpha = self.alpha.div(np.abs(self.alpha).sum(axis=1), axis=0)
        self.dates = date.to_datestr(self.alpha.index)
        self.quote = QuoteFetcher(datetime_index=True, reindex=True)

        self.IC, self.rIC = {}, {}
        self.AC, self.rAC = {}, {}
        self.turnover = None
        self.returns = None

        self.__dict__.update(kwargs)

    def get_turnover(self):
        if self.turnover is not None:
            return self.turnover

        change = self.alpha.fillna(0) - self.alpha.shift(1).fillna(0)
        turnover = np.abs(change).sum(axis=1)
        return turnover.iloc[1:]

    def get_ic(self, n=1, rank=False):
        if rank and n in self.rIC:
            return self.rIC[n]
        if not rank and n in self.IC:
            return self.IC[n]

        returns = self.quote.fetch_window('returnsN', n, self.dates)
        shifted = self.alpha.shift(n)
        if not rank:
            ic = returns.corr(shifted, axis=1).iloc[n:]
            self.IC[n] = ic
            return ic
        ic = returns.rank(axis=1).corr(shifted.rank(axis=1), axis=1).iloc[n:]
        self.rIC[n] = ic
        return ic

    def get_ac(self, n=1, rank=False):
        if rank and n in self.rAC:
            return self.rAC[n]
        if not rank and n in self.AC:
            return self.AC[n]

        shifted = self.alpha.shift(n)
        if not rank:
            ac = self.alpha.corr(shifted, axis=1).iloc[n:]
            self.AC[n] = ac
            return ac
        ac = self.alpha.rank(axis=1).corr(shifted.rank(axis=1), axis=1).iloc[n:]
        self.rAC[n] = ac
        return ac

    def get_returns(self, cost=0.001):
        """
        :param float cost: Trading cost. Default: 0.001, i.e. with each 1-dollar buy or sell,
        there will be a 0.001 charge
        """

        if self.returns is None:
            returns = self.quote.fetch_window('returns', self.dates)
            self.returns = (returns * self.alpha.shift(1)).sum(axis=1).iloc[1:]

        if cost == 0:
            return self.returns

        return self.returns - cost * self.get_turnover()

    def get_returns_metric(self, how, cost=0.001, by=None):
        """Calculated metrics based on the daily returns time-series.

        :param function how: Used in resampling method
        :param by: Caculation frequency
           * None: Whole period
           * 'A': Yearly
           * 'Q': Quarterly
           * 'M': Monthly
        """
        returns = self.get_returns(cost=cost)

        if by is None:
            return how(returns)

        return returns.resample(by, how=how)

    def get_drawdown(self, cost=0.001, by=None):
        return self.get_returns_metric(util.drawdown, cost=cost, by=by)

    def get_annualized_returns(self, cost=0.001, by=None):
        return self.get_returns_metric(util.annualized_returns, cost=cost, by=by)

    def get_perwin(self, cost=0.001, by=None):
        return self.get_returns_metric(util.perwin, cost=cost, by=by)

    def get_returns_ir(self, cost=0.001, by=None):
        return self.get_returns_metric(util.IR, cost=cost, by=by)

    def summary_ir(self, by=None):
        """Returns a IR-related metrics summary series(``by`` is None, default)/dataframe.

        These metrics are:
        * IR1: mean(IC(1)) / std(IC(1))
        * IR5: mean(IC(5)) / std(IC(5))
        * IR20: mean(IC(20)) / std(IC(20))
        * rIR1: mean(rank IC(1)) / std(rank IC(1))
        * rIR5: mean(rank IC(5)) / std(rank IC(5))
        * rIR20: mean(rank IC(20)) / std(rank IC(20))
        """

        index = ['IR1', 'IR5', 'IR20', 'rIR1', 'rIR5', 'rIR20']
        res = {
                'IR1': self.resample(self.get_ic(1), how='ir', by=by),
                'IR5': self.resample(self.get_ic(5), how='ir', by=by),
                'IR20': self.resample(self.get_ic(20), how='ir', by=by),
                'rIR1': self.resample(self.get_ic(1, rank=True), how='ir', by=by),
                'rIR5': self.resample(self.get_ic(5, rank=True), how='ir', by=by),
                'rIR20': self.resample(self.get_ic(20, rank=True), how='ir', by=by),
                }
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        res.index = index
        return res

    def summary_turnover(self, by=None):
        """Returns a turnover-related metrics summary series(``by`` is None, default)/dataframe.

        These metrics are:
        * turnover: average daily turnover
        * AC1: average daily 1-day auto-correlation
        * AC5: average daily 5-day auto-correlation
        * AC20: average daily 20-day auto-correlation
        * rAC1: average daily 1-day rank auto-correlation
        * rAC5: average daily 5-day rank auto-correlation
        * rAC20: average daily 20-day rank auto-correlation
        """

        index = ['turnover', 'AC1', 'AC5', 'AC20', 'rAC1', 'rAC5', 'rAC20']
        res = {
                'turnover': self.resample(self.get_turnover(), how='mean', by=by),
                'AC1': self.resample(self.get_ac(1), how='mean', by=by),
                'AC5': self.resample(self.get_ac(5), how='mean', by=by),
                'AC20': self.resample(self.get_ac(20), how='mean', by=by),
                'rAC1': self.resample(self.get_ac(1), how='mean', by=by),
                'rAC5': self.resample(self.get_ac(1), how='mean', by=by),
                'rAC20': self.resample(self.get_ac(1), how='mean', by=by),
                }
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        res.index = index
        return res

    def summary_returns(self, cost=0.001, by=None):
        """Returns a turnover-related metrics summary series(``by`` is None, default)/dataframe.

        These metrics are:
        * annualized_returns_0: annualized_returns(%) before cost
        * annualized_returns: annualized_returns(%) after cost
        * SR_0: Sharpe Ratio for daily returns series before cost
        * SR: Sharpe Ratio for daily returns series after cost
        * drawdown: drawdown within the period
        * ddstart: drawdown start date
        * ddend: drawdown end date
        * perwin: winning percentage
        """

        index = ['annualized_returns_0', 'annualized_returns', 'SR_0', 'SR',
                'drawdown', 'ddstart', 'ddend', 'perwin']
        if by is None:
            ddstart, ddend, drawdown = self.get_drawdown(cost=cost)
            ddstart, ddend = ddstart.strftime('%Y%m%d'), ddend.strftime('%Y%m%d')
        else:
            temp = self.get_drawdown(cost=cost, by=by)
            ddstart = temp.apply(lambda x: x[0].strftime('%Y%m%d'))
            ddend = temp.apply(lambda x: x[1].strftime('%Y%m%d'))
            drawdown = temp.apply(lambda x: x[2])

        res = {
                'annualized_returns_0': self.get_annualized_returns(cost=0, by=by),
                'annualized_returns': self.get_annualized_returns(cost=cost, by=by),
                'SR_0': self.resample(self.get_returns(cost=0), how='sr', by=by),
                'SR': self.resample(self.get_returns(cost=cost), how='sr', by=by),
                'drawdown': drawdown,
                'ddstart': ddstart,
                'ddend': ddend,
                }
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        res.index = index
        return res

    def summary(self, cost=0.001, by=None, group='ir'):
        """Returns a summary series(``by`` is None, default)/dataframe.

        :param str group: Which aspect to be summarized
           * 'ir': IR-related metrics
           * 'turnover': Turnover-related metrics
           * 'returns': Returns/PNL-related metrics
           * 'all': All metrics in the above 3 groups
        """

        if group == 'ir':
            return self.summary_ir(by=by)
        elif group == 'turnover':
            return self.summary_turnover(by=by)
        elif group == 'returns':
            return self.summary_returns(cost=cost, by=by)
        else:
            return pd.concat([
                self.summary_ir(by=by),
                self.summary_turnover(by=by),
                self.summary_returns(cost=cost, by=by)
                ])

    @staticmethod
    def resample(ser, how='mean', by=None):
        """Helper function.

        :param string how:
           *'mean': calculate mean
           *'ir': calculate mean/std
           *'sr': calculate mean/std*sqrt(DAYS_IN_YEAR)
        """

        if how == 'ir':
            return ser.mean() / ser.std() if by is None \
                    else ser.resample(by, how=lambda x: x.mean()/x.std())
        elif how == 'sr':
            return ser.mean() / ser.std() * np.sqrt(DAYS_IN_YEAR) if by is None \
                    else ser.resample(by, how=lambda x: x.mean()/x.std()*np.sqrt(DAYS_IN_YEAR))
        else:
            return ser.mean() if by is None \
                    else ser.resample(by, how='mean')
