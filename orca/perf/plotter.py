"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd


class Plotter(object):
    """Class to plot time-series performance metrics.

    :param analyser: A :py:class:`orca.perf.analyser.Analyser` object

    """

    def __init__(self, analyser):
        self.analyser = analyser

    @staticmethod
    def cut(pdobj, startdate=None, enddate=None):
        if startdate:
            pdobj = pdobj.ix[pdobj.index >= str(startdate)]
        if enddate:
            pdobj = pdobj.ix[pdobj.index <= str(startdate)]
        return pdobj

    def plot_turnover(self, ma=None, startdate=None, enddate=None):
        """
        :param int ma: MA periods. Default: None, do not plot MA
        :param str startdate, enddate: Limit the time range

        """
        tvr = self.cut(self.analyser.get_turnover(), startdate, enddate)

        if ma is not None:
            ma_tvr = pd.rolling_mean(tvr, ma)
            df = pd.concat([tvr, ma_tvr], axis=1, keys=['turnover', 'MA(%s)' % ma])
            df.plot()
            return
        tvr.plot()

    def plot_ic(self, n=1, rank=False, ma=None, startdate=None, enddate=None):
        """
        :param int ma: MA periods. Default: None, do not plot MA

        """
        ic = self.cut(self.analyser.get_ic(n=n, rank=rank), startdate, enddate)

        if ma is not None:
            ma_ic = pd.rolling_mean(ic, ma)
            df = pd.concat([ic, ma_ic], axis=1, keys=['ic', 'MA(%s)' % ma])
            df.plot()
            return
        ic.plot()

    def plot_ac(self, n=1, rank=False, ma=None, startdate=None, enddate=None):
        ac = self.cut(self.analyser.get_ac(n=n, rank=rank), startdate, enddate)

        if ma is not None:
            ma_ac = pd.rolling_mean(ac, ma)
            df = pd.concat([ac, ma_ac], axis=1, keys=['ac', 'MA(%s)' % ma])
            df.plot()
            return
        ac.plot()

    def plot_pnl(self, cost=None, index=False, plot_index=False, startdate=None, enddate=None):
        """
        :param boolean plot_index: Whether to add a PNL line for index. Default: False

        """
        ret = self.analyser.get_returns(index=index)
        if plot_index:
            ret_i = self.analyser.index_data.ix[ret.index]

        if cost is not None:
            ret_c = self.analyser.get_returns(cost=cost, index=index)
            if plot_index:
                pnl = pd.concat([ret, ret_c, ret_i], axis=1, keys=['returns', 'cost(%f)' % cost, 'index'])
            else:
                pnl = pd.concat([ret, ret_c], axis=1, keys=['returns', 'cost(%f)' % cost])
            pnl = self.cut(pnl, startdate, enddate).cumsum()
            pnl.plot()
            return

        if plot_index:
            pnl = pd.concat([ret, ret_i], axis=1, keys=['returns', 'index'])
            pnl = self.cut(pnl, startdate, enddate).cumsum()
        else:
            pnl = self.cut(ret, startdate, enddate).cumsum()
        pnl.plot()
