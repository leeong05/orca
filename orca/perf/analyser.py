"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca.utils import dateutil
from orca.operation import api

import util


class Analyser(object):
    """Class for alpha (as portfolio) performance analysis.

    :param DataFrame alpha: Alpha to be analysed. Be sure to properly format the DataFrame as in :py:func:`orca.operation.api.format`
    :param DataFrame data: Daily returns data properly formatted
    :param Series index_data: Series of index daily returns that has the same index as ``alpha``. Default: None

    .. py:attribute:: IC

       A ``dict`` with key being the number of days, value being the IC time series.

    .. py:attribute:: rIC

       A ``dict`` with key being the number of days, value being the rank IC time series.

    .. py:attribute:: AC

       A ``dict`` with key being the number of days, value being the Auto-corrwithelation time series.

    .. py:attribute:: rAC

       A ``dict`` with key being the number of days, value being the rank Auto-corrwithelation time series.

    .. py:attribute:: turnover

       A ``Series`` of daily turnover.

    .. py:attribute:: returns

       A ``Series`` of daily returns without any cost considerations.
    """

    def __init__(self, alpha, data, index_data=None):
        self.alpha = api.scale(alpha)
        self.dates = dateutil.to_datestr(self.alpha.index)

        self.IC, self.rIC = {}, {}
        self.AC, self.rAC = {}, {}
        self.turnover = None
        self.returns = None

        self.data = data.ix[self.alpha.index]
        if index_data is not None:
            self.index_data = index_data.ix[self.alpha.index]

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

        shifted = self.alpha.shift(n)
        returns = pd.rolling_apply(self.data, n, lambda x: (1+x).cumprod()[-1] - 1.)
        if not rank:
            ic = returns.corrwith(shifted, axis=1).iloc[n:]
            self.IC[n] = ic
            return ic
        ic = returns.rank(axis=1).corrwith(shifted.rank(axis=1), axis=1).iloc[n:]
        self.rIC[n] = ic
        return ic

    def get_ir(self, n=1, rank=False, by=None):
        return util.resample(self.get_ic(1), how='ir', by=by)

    def get_ac(self, n=1, rank=False):
        if rank and n in self.rAC:
            return self.rAC[n]
        if not rank and n in self.AC:
            return self.AC[n]

        shifted = self.alpha.shift(n)
        alpha = self.alpha.copy()
        shifted[shifted.isnull() & ~alpha.isnull()] = 0
        alpha[~shifted.isnull() & alpha.isnull()] = 0
        if not rank:
            ac = alpha.corrwith(shifted, axis=1).iloc[n:]
            self.AC[n] = ac
            return ac
        ac = alpha.rank(axis=1).corrwith(shifted.rank(axis=1), axis=1).iloc[n:]
        self.rAC[n] = ac
        return ac

    def get_returns(self, cost=0, index=False):
        """
        :param float cost: Linear amount-proportion trading cost. Default: 0
        :param boolean index: Whether we measure returns against index. Default: False
        """

        if self.returns is None:
            self.returns = (self.data * self.alpha.shift(1)).sum(axis=1).iloc[1:]

        if not cost:
            return self.returns-self.index_data if index else self.returns

        return (self.returns-self.index_data if index else self.returns) - cost * self.get_turnover()

    def get_returns_metric(self, how, cost=0, by=None, index=False):
        """Calculated metrics based on the daily returns time-series.

        :param function how: Used in resampling method
        :param by: Caculation frequency. None(default): whole period; 'A': yearly; 'Q': quarterly; 'M': monthly
        :param boolean index: Whether we measure returns against index. Default: False
        """
        returns = self.get_returns(cost=cost, index=index)

        if by is None:
            return how(returns)

        return returns.resample(by, how=how)

    def get_drawdown(self, cost=0, by=None, index=False):
        """Use :py:meth:`get_returns_metric` to calculate drawdown."""
        return self.get_returns_metric(util.drawdown, cost=cost, by=by, index=index)

    def get_annualized_returns(self, cost=0, by=None, index=False):
        """Use :py:meth:`get_returns_metric` to calculate annualized returns."""
        return self.get_returns_metric(util.annualized_returns, cost=cost, by=by, index=index)

    def get_perwin(self, cost=0, by=None, index=False):
        """Use :py:meth:`get_returns_metric` to calculate winning percentage."""
        return self.get_returns_metric(util.perwin, cost=cost, by=by, index=index)

    def get_returns_sharpe(self, cost=0, by=None, index=False):
        """Use :py:meth:`get_returns_metric` to calculate Sharpe ratio."""
        return self.get_returns_metric(util.Sharpe, cost=cost, by=by, index=index)

    def summary_ir(self, by=None, freq='daily'):
        """Returns a IR-related metrics summary series(``by`` is None, default)/dataframe.

        :param str freq: Which frequency of statistics is of interest? 'daily'(default): only returns IR1, rIR1; 'weekly': returns also IR5, rIR5; 'monthly': returns also IR20, rIR20

        These metrics are:
           * IR1: mean(IC(1)) / std(IC(1))
           * IR5: mean(IC(5)) / std(IC(5))
           * IR20: mean(IC(20)) / std(IC(20))
           * rIR1: mean(rank IC(1)) / std(rank IC(1))
           * rIR5: mean(rank IC(5)) / std(rank IC(5))
           * rIR20: mean(rank IC(20)) / std(rank IC(20))
        """

        index = ['days', 'IR1', 'rIR1']
        tmp = {
                'days': util.resample(self.get_ic(1), how='count', by=by),
                'IR1': util.resample(self.get_ic(1), how='ir', by=by),
                'rIR1': util.resample(self.get_ic(1, rank=True), how='ir', by=by),
                }

        if freq == 'weekly':
            index.extend(['IR5', 'rIR5'])
            tmp.update({
                'IR5': util.resample(self.get_ic(5), how='ir', by=by),
                'rIR5': util.resample(self.get_ic(5, rank=True), how='ir', by=by),
                })
        elif freq == 'monthly':
            index.extend(['IR5', 'rIR5', 'IR20', 'rIR20'])
            tmp.update({
                'IR5': util.resample(self.get_ic(5), how='ir', by=by),
                'rIR5': util.resample(self.get_ic(5, rank=True), how='ir', by=by),
                'IR20': util.resample(self.get_ic(20), how='ir', by=by),
                'rIR20': util.resample(self.get_ic(20, rank=True), how='ir', by=by),
                })
        res = pd.Series(tmp) if by is None else pd.DataFrame(tmp).T
        res = res.reindex(index)
        return res

    def summary_turnover(self, by=None, freq='daily'):
        """Returns a turnover-related metrics summary series(``by`` is None, default)/dataframe.

        :param str freq: Which frequency of statistics is of interest? 'daily'(default): only returns turnover, AC1, rAC1; 'weekly': returns also AC5, rAC5; 'monthly': returns also AC20, rAC20

        These metrics are:
           * turnover: average daily turnover
           * AC1: average daily 1-day auto-corrwithelation
           * AC5: average daily 5-day auto-corrwithelation
           * AC20: average daily 20-day auto-corrwithelation
           * rAC1: average daily 1-day rank auto-corrwithelation
           * rAC5: average daily 5-day rank auto-corrwithelation
           * rAC20: average daily 20-day rank auto-corrwithelation
        """

        index = ['turnover', 'AC1', 'rAC1']
        tmp = {
                'turnover': util.resample(self.get_turnover(), how='mean', by=by),
                'AC1': util.resample(self.get_ac(1), how='mean', by=by),
                'rAC1': util.resample(self.get_ac(1, rank=True), how='mean', by=by),
                }
        if freq == 'weekly':
            index.extend(['AC5', 'rAC5'])
            tmp.update({
                'AC5': util.resample(self.get_ac(5), how='mean', by=by),
                'rAC5': util.resample(self.get_ac(5, rank=True), how='mean', by=by),
                })
        elif freq == 'monthly':
            index.extend(['AC5', 'rAC5', 'AC20', 'rAC20'])
            tmp.update({
                'AC5': util.resample(self.get_ac(5), how='mean', by=by),
                'rAC5': util.resample(self.get_ac(5, rank=True), how='mean', by=by),
                'AC20': util.resample(self.get_ac(20), how='mean', by=by),
                'rAC20': util.resample(self.get_ac(20, rank=True), how='mean', by=by),
                })
        res = pd.Series(tmp) if by is None else pd.DataFrame(tmp).T
        res = res.reindex(index)
        return res

    def summary_returns(self, cost=0, by=None, index=False):
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

        if by is None:
            ddstart, ddend, drawdown = self.get_drawdown(cost=cost, index=index)
            ddstart, ddend = ddstart.strftime('%Y%m%d'), ddend.strftime('%Y%m%d')
        else:
            temp = self.get_drawdown(cost=cost, by=by, index=index)
            ddstart = temp.apply(lambda x: x[0].strftime('%Y%m%d'))
            ddend = temp.apply(lambda x: x[1].strftime('%Y%m%d'))
            drawdown = temp.apply(lambda x: x[2])

        res = {
                'annualized_returns_0': self.get_annualized_returns(cost=0, by=by, index=index),
                'annualized_returns': self.get_annualized_returns(cost=cost, by=by, index=index),
                'SR_0': self.get_returns_sharpe(cost=0, by=by, index=index),
                'SR': self.get_returns_sharpe(cost=cost, by=by, index=index),
                'perwin': self.get_perwin(cost=cost, by=by, index=index),
                'drawdown': drawdown,
                'ddstart': ddstart,
                'ddend': ddend,
                }
        print res['perwin']
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        index = ['annualized_returns_0', 'annualized_returns', 'SR_0', 'SR',
                'drawdown', 'ddstart', 'ddend', 'perwin']
        res = res.reindex(index)
        return res

    def summary(self, cost=0, by=None, group='ir', freq='daily'):
        """Returns a summary series(``by`` is None, default)/dataframe.

        :param str group: Which aspect to be summarized. 'ir'(default): IR-related metrics; 'turnover': Turnover-related metrics; 'returns': Returns/PNL-related metrics; 'all': All metrics in the above 3 groups
        :param str freq: Which frequency of statistics is of interest? Default: 'daily'
        """

        if group == 'ir':
            return self.summary_ir(by=by, freq=freq)
        elif group == 'turnover':
            return self.summary_turnover(by=by, freq=freq)
        elif group == 'returns':
            return self.summary_returns(cost=cost, by=by)
        else:
            return pd.concat([
                self.summary_ir(by=by, freq=freq),
                self.summary_turnover(by=by, freq=freq),
                self.summary_returns(cost=cost, by=by)
                ])
