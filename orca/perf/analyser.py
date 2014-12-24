"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

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

       A ``dict`` with key being the number of days, value being the auto-corrwithelation time series.

    .. py:attribute:: rAC

       A ``dict`` with key being the number of days, value being the rank auto-corrwithelation time series.

    .. py:attribute:: turnover

       A ``Series`` of daily turnover.

    .. py:attribute:: returns

       A ``Series`` of daily returns without any cost considerations.
    """

    def __init__(self, alpha, data, index_data=None):
        self.alpha = api.scale(alpha)
        self.intervals = self.alpha.index.date

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
        self.turnover = turnover.iloc[1:]
        return self.turnover

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
        return util.resample(self.get_ic(n=n, rank=rank), how='ir', by=by)

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
        """Returns a IR-related metrics summary Series/Dataframe.

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
        """Returns a turnover-related metrics summary Series/Dataframe.

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
        """Returns a returns-related metrics summary Series/Dataframe.

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
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        index = ['annualized_returns_0', 'annualized_returns', 'SR_0', 'SR',
                'drawdown', 'ddstart', 'ddend', 'perwin']
        res = res.reindex(index)
        return res

    def summary(self, cost=0, by=None, group='ir', freq='daily'):
        """Returns a summary Dataframe.

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


class IntAnalyser(object):
    """Class for intraday alpha (as portfolio) performance analysis.

    :param DataFrame alpha: Alpha to be analysed. Be sure to properly format the DataFrame as in :py:func:`orca.operation.api.format`
    :param DataFrame data: Interval returns data properly formatted
    :param Series index_data: Series of index interval returns that has the same index as ``alpha``. Default: None

    .. py:attribute:: IC_t/rIC_t

       Daily average of intraday IC/rank IC

    .. py:attribute: IC_h/rIC_h

       Daily time series of overnight holding IC/rank IC

    .. py:attribute: IC_d/rIC_d

       Daily time series of sum of intraday and overnight holding IC/rank IC

    .. py:attribute:: AC_t/rAC_t

       Daily average of intraday auto-correlation/rank auto-correlation

    .. py:attribute:: AC_h/rAC_h

       Daily time series of overnight auto-correlation/rank auto-correlation

    .. py:attribute:: AC_d/rAC_d

       Daily time series of sum of intraday and overnight holding AC/rank AC

    .. py:attribute:: turnover_t

       Daily average of intraday turnover/returns

    .. py:attribute:: turnover_h

       Daily time series of overnight holding turnover/returns

    .. py:attribute:: turnover_d

       Daily time series of sum of intraday and overnight holding turnover/returns
    """

    def __init__(self, alpha, data, index_data=None):
        self.alpha = api.scale(alpha)
        self.dates = np.unique(self.alpha.index.date)
        self.intervals = len(self.alpha) / len(self.dates)

        self.IC_t, self.rIC_t = None, None
        self.IC_h, self.rIC_h = None, None
        self.IC_d, self.rIC_d = None, None
        self.AC_t, self.rAC_t = None, None
        self.AC_h, self.rAC_h = None, None
        self.AC_d, self.rAC_d = None, None
        self.turnover_t, self.turnover_h, self.turnover_d = None, None, None
        self.turnover, self.returns = None, None

        self.data = data.ix[self.alpha.index]
        if index_data is not None:
            self.index_data = index_data.ix[self.alpha.index]

    def get_turnover(self):
        if self.turnover is not None:
            return (self.turnover_t, self.turnover_h, self.turnover_d)

        change = self.alpha.fillna(0) - self.alpha.shift(1).fillna(0)
        tvr = np.abs(change).sum(axis=1)
        self.turnover = tvr.copy()
        self.turnover_d = tvr.resample('D', how='mean').ix[self.dates]
        tvr_h = tvr[::self.intervals].copy()
        tvr_h.index = tvr_h.index.date
        self.turnover_h = tvr_h.iloc[1:]
        tvr[::self.intervals] = np.nan
        self.turnover_t = tvr.resample('D', how='mean').ix[self.dates]

        self.turnover_d.index = pd.to_datetime(self.turnover_d.index)
        self.turnover_t.index = pd.to_datetime(self.turnover_t.index)
        self.turnover_h.index = pd.to_datetime(self.turnover_h.index)
        return (self.turnover_t, self.turnover_h, self.turnover_d)

    def get_ic(self, rank=False):
        if rank and self.rIC_t is not None:
            return (self.rIC_t, self.rIC_h, self.rIC_d)
        if not rank and self.IC_t is not None:
            return (self.IC_t, self.IC_h, self.IC_d)

        shifted = self.alpha.shift(1)
        returns = self.data
        if rank:
            ic = returns.rank(axis=1).corrwith(shifted.rank(axis=1), axis=1)
        else:
            ic = returns.corrwith(shifted, axis=1)
        ic_d = ic.resample('D', how='mean').ix[self.dates]
        ic_h = ic[::self.intervals].copy()
        ic_h.index = ic_h.index.date
        ic_h = ic_h.iloc[1:]

        ic[::self.intervals] = np.nan
        ic_t = ic.resample('D', how='mean').ix[self.dates]

        ic_d.index = pd.to_datetime(ic_d.index)
        ic_t.index = pd.to_datetime(ic_t.index)
        ic_h.index = pd.to_datetime(ic_h.index)
        if rank:
            self.rIC_t = ic_t
            self.rIC_h = ic_h
            self.rIC_d = ic_d
        else:
            self.IC_t = ic_t
            self.IC_h = ic_h
            self.IC_d = ic_d
        return (ic_t, ic_h, ic_d)

    def get_ir(self, rank=False, by=None):
        ic_t, ic_h, ic_d = self.get_ic(rank=rank)
        return util.resample(ic_t, how='ir', by=by), util.resample(ic_h, how='ir', by=by), util.resample(ic_d, how='ir', by=by)

    def get_ac(self, rank=False):
        if rank and self.rAC_t is not None:
            return (self.rAC_t, self.rAC_h, self.rAC_d)
        if not rank and self.AC_t is not None:
            return (self.AC_t, self.AC_h, self.AC_d)

        shifted = self.alpha.shift(1)
        alpha = self.alpha.copy()
        shifted[shifted.isnull() & ~alpha.isnull()] = 0
        alpha[~shifted.isnull() & alpha.isnull()] = 0
        if rank:
            ac = alpha.rank(axis=1).corrwith(shifted.rank(axis=1), axis=1)
        else:
            ac = alpha.corrwith(shifted, axis=1)
        ac_d = ac.resample('D', how='mean').ix[self.dates]
        ac_h = ac[::self.intervals].copy()
        ac_h.index = ac_h.index.date
        ac_h = ac_h.iloc[1:]

        ac[::self.intervals] = np.nan
        ac_t = ac.resample('D', how='mean').ix[self.dates]

        ac_d.index = pd.to_datetime(ac_d.index)
        ac_t.index = pd.to_datetime(ac_t.index)
        ac_h.index = pd.to_datetime(ac_h.index)
        if rank:
            self.rAC_t = ac_t
            self.rAC_h = ac_h
            self.rAC_d = ac_d
        else:
            self.AC_t = ac_t
            self.AC_h = ac_h
            self.AC_d = ac_d
        return (ac_t, ac_h, ac_d)

    def get_returns(self, cost=0, index=False):
        """
        :param float cost: Linear amount-proportion trading cost. Default: 0
        :param boolean index: Whether we measure returns against index. Default: False
        """

        if self.returns is None:
            self.returns = (self.data * self.alpha.shift(1)).sum(axis=1)

        if cost:
            self.get_turnover()
            ret = (self.returns-self.index_data if index else self.returns) - cost * self.turnover
        else:
            ret = self.returns-self.index_data if index else self.returns

        ret = ret.copy()
        ret_d = ret.resample('D', how='sum').ix[self.dates]
        ret_h = ret[::self.intervals].copy()
        ret_h.index = ret_h.index.date
        ret_h = ret_h.iloc[1:]
        ret[::self.intervals] = 0
        ret_t = ret.resample('D', how='sum').ix[self.dates]

        ret_d.index = pd.to_datetime(ret_d.index)
        ret_t.index = pd.to_datetime(ret_t.index)
        ret_h.index = pd.to_datetime(ret_h.index)
        return (ret_t, ret_h, ret_d)

    def get_returns_metric(self, how, cost=0, by=None, index=False):
        """Calculated metrics based on the daily returns time-series.

        :param function how: Used in resampling method
        :param by: Caculation frequency. None(default): whole period; 'A': yearly; 'Q': quarterly; 'M': monthly
        :param boolean index: Whether we measure returns against index. Default: False
        """
        ret_t, ret_h, ret_d = self.get_returns(cost=cost, index=index)

        if by is None:
            return (how(ret_t), how(ret_h), how(ret_d))

        return (ret_t.resample(by, how=how), ret_h.resample(by, how=how), ret_d.resample(by, how=how))

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

    def get_bpmargin(self, by=None):
        tvr_t = self.get_turnover()[0]
        ret_t = self.get_returns()[0]
        if by is None:
            return ret_t.mean() / ((self.intervals-1)*tvr_t.mean()) * 10000
        return ret_t.resample(by, how='mean') / ((self.intervals-1)*tvr_t.resample(by, how='mean')) * 10000

    def summary_ir(self, by=None, freq='daily'):
        """Returns a IR-related metrics summary Dataframe."""
        index = ['days', 'IR_t', 'rIR_t', 'IR_h', 'rIR_h', 'IR_d', 'rIR_d']
        ic_t, ic_h, ic_d = self.get_ic()
        ric_t, ric_h, ric_d = self.get_ic(rank=True)
        res = {
                'days': util.resample(ic_t, how='count', by=by),
                'IR_t': util.resample(ic_t, how='ir', by=by),
                'rIR_t': util.resample(ric_t, how='ir', by=by),
                'IR_h': util.resample(ic_h, how='ir', by=by),
                'rIR_h': util.resample(ric_h, how='ir', by=by),
                'IR_d': util.resample(ic_d, how='ir', by=by),
                'rIR_d': util.resample(ric_d, how='ir', by=by),
                }
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        res = res.reindex(index)
        return pd.DataFrame({'ALL': res}) if by is None else res

    def summary_turnover(self, by=None):
        """Returns a turnover-related metrics summary Dataframe."""
        index = ['turnover_t', 'turnover_h', 'turnover_d']
        tvr_t, tvr_h, tvr_d = self.get_turnover()
        res = {
                'turnover_t': util.resample(tvr_t, how='mean', by=by),
                'turnover_h': util.resample(tvr_h, how='mean', by=by),
                'turnover_d': util.resample(tvr_d, how='mean', by=by),
                }
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        res = res.reindex(index)
        return pd.DataFrame({'ALL': res}) if by is None else res

    def summary_returns(self, cost=0, by=None, index=False, which='trading'):
        """Returns a turnover-related metrics summary Dataframe.

        :param str which: Which returns is of interest? Must be one of ('trading', 'holding', 'daily'). Default: 'trading'

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

        if which == 'holding':
            which = 1
        elif which == 'daily':
            which = 2
        else:
            which = 0

        if by is None:
            ddstart, ddend, drawdown = self.get_drawdown(cost=cost, index=index)[which]
            ddstart, ddend = ddstart.strftime('%Y%m%d'), ddend.strftime('%Y%m%d')
        else:
            temp = self.get_drawdown(cost=cost, by=by, index=index)[which]
            ddstart = temp.apply(lambda x: x[0].strftime('%Y%m%d'))
            ddend = temp.apply(lambda x: x[1].strftime('%Y%m%d'))
            drawdown = temp.apply(lambda x: x[2])

        res = {
                'annualized_returns_0': self.get_annualized_returns(cost=0, by=by, index=index)[which],
                'annualized_returns': self.get_annualized_returns(cost=cost, by=by, index=index)[which],
                'SR_0': self.get_returns_sharpe(cost=0, by=by, index=index)[which],
                'SR': self.get_returns_sharpe(cost=cost, by=by, index=index)[which],
                'perwin': self.get_perwin(cost=cost, by=by, index=index)[which],
                'drawdown': drawdown,
                'ddstart': ddstart,
                'ddend': ddend,
                }
        if which == 0:
            res.update({'bp_margin': self.get_bpmargin(by=by)})
        res = pd.Series(res) if by is None else pd.DataFrame(res).T
        index = ['annualized_returns_0', 'annualized_returns', 'SR_0', 'SR',
                'drawdown', 'ddstart', 'ddend', 'perwin']
        if which == 0:
            index.append('bp_margin')
        res = res.reindex(index)
        return pd.DataFrame({'ALL': res}) if by is None else res

    def summary(self, cost=0, by=None, group='ir', which='trading'):
        """Returns a summary Dataframe.

        :param str group: Which aspect to be summarized. 'ir'(default): IR-related metrics; 'turnover': Turnover-related metrics; 'returns': Returns/PNL-related metrics; 'all': All metrics in the above 3 groups
        :param str which: Which returns is of interest? Only used when ``group`` is 'returns'; must be one of ('trading', 'holding', 'daily'). Default: 'trading'
        """

        if group == 'ir':
            return self.summary_ir(by=by)
        elif group == 'turnover':
            return self.summary_turnover(by=by)
        elif group == 'returns':
            return self.summary_returns(cost=cost, by=by, which=which)
        else:
            return pd.concat([
                self.summary_ir(by=by),
                self.summary_turnover(by=by),
                self.summary_returns(cost=cost, by=by, which=which)
                ])
