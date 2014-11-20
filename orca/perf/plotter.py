"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.dates import DateFormatter

import util


class Plotter(object):
    """Class to plot time-series performance metrics.

    :param analyser: A :py:class:`orca.perf.analyser.Analyser` object
    """

    datefmt = DateFormatter('%Y%m%d')

    def __init__(self, analyser):
        self.analyser = analyser

    @staticmethod
    def cut(pdobj, startdate=None, enddate=None):
        if startdate:
            pdobj = pdobj.ix[pdobj.index >= str(startdate)]
        if enddate:
            pdobj = pdobj.ix[pdobj.index <= str(startdate)]
        return pdobj

    @staticmethod
    def _plot1(pdobj, title, drawdown=None):
        fig = plt.figure()
        ax = fig.add_subplot(111)

        if isinstance(pdobj, pd.Series):
            ax.plot(pdobj.index, pdobj)
            if drawdown is not None:
                start, end = drawdown
                dd_slice = pdobj.ix[start: end]
                ax.plot(dd_slice.index, dd_slice, 'r', lw=3, alpha=0.7)
        else:
            for col, ser in pdobj.iteritems():
                ax.plot(ser.index, ser, label=col)
                if col in drawdown:
                    start, end = drawdown[col]
                    dd_slice = pdobj.ix[start: end]
                    ax.plot(dd_slice.index, dd_slice, 'r', lw=3, alpha=0.7)
            ax.legend()

        ax.set_title(title)
        ax.format_xdata = Plotter.datefmt
        ax.xaxis.set_major_formatter(Plotter.datefmt)
        fig.autofmt_xdate()
        return fig

    @staticmethod
    def _plot2(pdobj, title):
        fig = plt.figure()
        ax = fig.add_subplot(111)

        if isinstance(pdobj, pd.Series):
            _pdobj = pd.DataFrame(pdobj)
        else:
            _pdobj = pdobj

        size, grps = _pdobj.shape
        ticks = np.arange(size)
        width = .9 / grps

        bars = []
        for i in range(grps):
            bars.append(ax.bar(ticks+i*width, _pdobj.iloc[:, i], width))
        for i in range(grps):
            for rect in bars[i]:
                rect.set_facecolor(cm.jet(i*1.0/grps))
                rect.set_alpha(0.6)

        ax.set_xticks(ticks + grps*width/2)
        ax.set_xticklabels([dt.strftime('%Y%m%d') for dt in pdobj.index], rotation=90)

        ax.set_title(title)
        if not isinstance(pdobj, pd.Series):
            ax.legend([bar[0] for bar in bars], list(pdobj.columns))
        return fig

    def plot_turnover(self, ma=None, startdate=None, enddate=None):
        """
        :param int ma: MA periods. Default: None, do not plot MA
        :param str startdate, enddate: Limit the time range
        """
        title = 'Turnover'
        tvr = self.cut(self.analyser.get_turnover(), startdate, enddate)

        if ma is not None:
            ma_tvr = pd.rolling_mean(tvr, ma)
            df = pd.concat([tvr, ma_tvr], axis=1, keys=['turnover', 'MA(%s)' % ma])
            return self._plot1(df, title)
        return self._plot1(tvr, title)

    def plot_ic(self, n=1, rank=False, ma=None, startdate=None, enddate=None):
        """
        :param int ma: MA periods. Default: None, do not plot MA
        """
        title = ('rIC(%s)' if rank else 'IC(%s)') % n
        ic = self.cut(self.analyser.get_ic(n=n, rank=rank), startdate, enddate)

        if ma is not None:
            ma_ic = pd.rolling_mean(ic, ma)
            df = pd.concat([ic, ma_ic], axis=1, keys=['ic', 'MA(%s)' % ma])
            return self._plot1(df, title)
        return self._plot1(ic, title)

    def plot_ac(self, n=1, rank=False, ma=None, startdate=None, enddate=None):
        """
        :param int ma: MA periods. Default: None, do not plot MA
        """
        title = ('rAC(%s)' if rank else 'AC(%s)') % n
        ac = self.cut(self.analyser.get_ac(n=n, rank=rank), startdate, enddate)

        if ma is not None:
            ma_ac = pd.rolling_mean(ac, ma)
            df = pd.concat([ac, ma_ac], axis=1, keys=['ac', 'MA(%s)' % ma])
            return self._plot1(df, title)
        return self._plot1(ac, title)

    def plot_pnl(self, cost=None, index=False, plot_index=False, drawdown=False, startdate=None, enddate=None):
        """Plot PNL line.

        :param boolean plot_index: Whether to add a PNL line for index. Default: False
        :param boolean drawdown: Whether to mark the drawdown on the PNL line. Default: False
        """
        title = 'Cumulative PNL'
        ret = self.cut(self.analyser.get_returns(index=index), startdate, enddate)
        if drawdown:
            start, end = util.drawdown(ret)[:2]
        if plot_index:
            ret_i = self.cut(self.analyser.index_data.ix[ret.index], startdate, enddate)

        if cost is not None:
            ret_c = self.cut(self.analyser.get_returns(cost=cost, index=index), startdate, enddate)
            if drawdown:
                start_c, end_c = util.drawdown(ret_c)[:2]
            if plot_index:
                pnl = pd.concat([ret, ret_c, ret_i], axis=1, keys=['returns', 'cost(%.4f)' % cost, 'index']).cumsum()
            else:
                pnl = pd.concat([ret, ret_c], axis=1, keys=['returns', 'cost(%.4f)' % cost]).cumsum()
            return self._plot1(pnl, title,
                    {pnl.columns[0]: [start, end], pnl.columns[1]: [start_c, end_c]}) if drawdown else \
                   self._plot1(pnl, title)

        if plot_index:
            pnl = pd.concat([ret, ret_i], axis=1, keys=['returns', 'index']).cumsum()
        else:
            pnl = ret.cumsum()
        return self._plot1(pnl, title, [start, end]) if drawdown else self._plot1(pnl, title)

    def plot_returns(self, by, index=False, plot_index=False, startdate=None, enddate=None):
        """Plot resampled returns using bars.

        :param str by: 'A': annually, 'Q': quarterly, 'M': monthly, 'W': weekly
        """
        mapping = {'A': 'Year', 'Q': 'Quarter', 'M': 'Month', 'W': 'Week'}
        title = 'Returns by %s' % mapping.get(by)

        ret = self.cut(self.analyser.get_returns(index=index), startdate, enddate)
        ret = ret.resample(by, how='sum')
        if plot_index:
            ret_i = self.analyser.index_data.ix[ret.index]
            ret = pd.concat([ret, ret_i], axis=1, keys=['returns', 'index'])
        return self._plot2(ret, title)


class QuantilesPlotter(object):
    """Class to plot returns distribution for quantiles.

    :param list analysers: List of :py:class:`orca.perf.analyser.Analyser` objects
    """

    datefmt = DateFormatter('%Y%m%d')

    def __init__(self, analysers):
        self.analysers = analysers

    @staticmethod
    def cut(pdobj, startdate=None, enddate=None):
        if startdate:
            pdobj = pdobj.ix[pdobj.index >= str(startdate)]
        if enddate:
            pdobj = pdobj.ix[pdobj.index <= str(startdate)]
        return pdobj

    @staticmethod
    def _plot1(df, title):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        for i, ser in df.iteritems():
            ax.plot(ser.index, ser, label='Q'+str(i+1))

        ax.legend()
        ax.set_title(title)
        ax.format_xdata = Plotter.datefmt
        ax.xaxis.set_major_formatter(QuantilesPlotter.datefmt)
        fig.autofmt_xdate()
        return fig

    @staticmethod
    def _plot2(pdobj, title):
        fig = plt.figure()

        if isinstance(pdobj, pd.Series):
            ax = fig.add_subplot(111)
            ticks = np.arange(len(pdobj))
            width = .9
            ax.bar(ticks, pdobj, width)
            ax.set_xticks(ticks + width/2)
            ax.set_xticklabels(['Q'+str(j) for j in range(1, len(pdobj)+1)])
            ax.set_title(title)
            return fig

        n, qtls = pdobj.shape
        for i, (dt, row) in enumerate(pdobj.iterrows()):
            ax = fig.add_subplot(n, 1, i+1)
            ticks = np.arange(qtls)
            width = .9
            ax.bar(ticks, row, width)
            ax.set_xticks(ticks + width/2)
            ax.set_xticklabels(['Q'+str(j) for j in range(1, qtls+1)])
            ax.set_title(dt.strftime('%Y%m%d'))

        return fig

    def plot_pnl(self, startdate=None, enddate=None):
        """Plot PNL lines for quantiles in the same plot.
        """
        rets = [self.cut(analyser.get_returns(), startdate, enddate) for analyser in self.analysers]
        pnl = pd.concat(rets, axis=1).cumsum()
        return self._plot1(pnl, 'Quantiles')

    def plot_returns(self, by=None, startdate=None, enddate=None):
        """Plot returns for each quantile, with one subplot for one period.

        :param str by: 'A': annually, 'Q': quarterly, 'M': monthly. Default: None
        """
        rets = [self.cut(analyser.get_returns(), startdate, enddate) for analyser in self.analysers]
        rets = pd.concat(rets, axis=1)
        if by is not None:
            rets = rets.resample(by, 'sum')
        else:
            rets = rets.sum()
        return self._plot2(rets, 'Quantiles')
