"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from matplotlib.backends.backend_pdf import PdfPages

from orca.perf import util as perf_util
from orca.utils import dateutil as date_util

def plot_ts(y, **kwargs):
    fig, ax = plt.subplots(figsize=(14, 7))
    if isinstance(y, pd.Series):
        ax.plot(y.index, y, **kwargs)
        if y.name:
            ax.set_ylabel(y.name)
    else:
        for l, c in y.iteritems():
            ax.plot(y.index, c, label=l, **kwargs)
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=3, fancybox=True, shadow=True)
        if y.columns.name:
            ax.set_title(y.columns.name)
    ax.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    fig.autofmt_xdate()
    return fig

def plot_pnl(y, by='A', title='', **kwargs):
    assert by in ('A', 'Q', 'M')
    fig, ax = plt.subplots(figsize=(14, 7))
    pnl = y.cumsum()
    ax.plot(y.index, pnl)
    ax.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    # summary
    colors = ['darkgray', 'silver']
    markers = date_util.get_markers(y.index, by=by, end=True)
    markers.insert(0, y.index[0])
    markers.append(y.index[-1])
    for i, (m1, m2) in enumerate(zip(markers[:-1], markers[1:])):
        ax.axvspan(m1, m2, facecolor=colors[i%2], alpha=0.3)

    summary = pd.DataFrame({
        'AAR': y.resample(by, how=perf_util.annualized_returns) * 100,
        'perwin': y.resample(by, how=perf_util.perwin),
        'Sharpe': y.resample(by, how=perf_util.Sharpe),
        })
    max_pnl, min_pnl = pnl.max(), pnl.min()
    interval = (max_pnl - min_pnl)/16
    for period, row in summary.iterrows():
        if by == 'A':
            period = period.strftime('%Y')
            period_slice = y.ix[period]
        elif by == 'Q':
            period = period.strftime('%Y%m')
            if int(period[4:]) == 12:
                period_slice = y.ix[period[:4]+str(int(period[4:])-3)+'01': str(int(period[:4])+1)+'0101']
            else:
                period_slice = y.ix[period[:4]+str(int(period[4:])-3)+'01': period[:4]+str(int(period[4:])+1)+'01']
        elif by == 'M':
            period = period.strftime('%Y%m')
            if int(period[4:]) == 12:
                period_slice = y.ix[period+'01': str(int(period[:4])+1)+'0101']
            else:
                period_slice = y.ix[period+'01': period[:4]+str(int(period[4:])+1)+'01']
        AAR, perwin, Sharpe = row['AAR'], row['perwin'], row['Sharpe']
        dd_start, dd_end, dd = perf_util.drawdown(period_slice)
        dd_slice = pnl.ix[dd_start: dd_end]
        ax.plot(dd_slice.index, dd_slice, 'r', lw=2, alpha=0.7)
        ax.text(period_slice.index[0], max_pnl-0*interval, 'Sharpe: %.1f' % Sharpe, size=12, color='darkred')
        ax.text(period_slice.index[0], max_pnl-1*interval, 'AnnRet: %.1f%%' % AAR, size=12, color='darkred')
        ax.text(period_slice.index[0], max_pnl-2*interval, 'PerWin: %.1f%%' % perwin, size=12, color='darkred')
        ax.text(period_slice.index[0], max_pnl-3*interval, 'MaxDD: %.1f%%' % dd, size=12, color='darkred')

    # drawdown
    dd_start, dd_end, dd = perf_util.drawdown(y)
    dd_slice = pnl.ix[dd_start: dd_end]
    ax.plot(dd_slice.index, dd_slice, 'k', lw=5, alpha=0.3)
    ax.text(dd_end, dd_slice.ix[dd_end], '%.1f%%' % dd, color='r')
    # title
    title_summary = 'Sharpe: %.1f, AAR: %.1f%%' % (perf_util.Sharpe(y), perf_util.annualized_returns(y)*100)
    if title:
        title = title + ' - ' + title_summary
    else:
        title = title_summary
    ax.set_title(title, color='b')
    ax.grid()
    fig.autofmt_xdate()
    return fig

def plot_twinx(y1, y2, params1={'color': 'red'}, params2={'color': 'blue'}):
    dates = y1.index.union(y2.index)
    y1, y2 = y1.reindex(index=dates), y2.reindex(index=dates)
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(dates, y1, **params1)
    ax.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    ax.set_ylabel(y1.name or 'y1', color=params1.get('color', 'red'))
    fig.autofmt_xdate()

    ax = ax.twinx()
    ax.plot(dates, y2, **params2)
    ax.set_ylabel(y2.name or 'y2', color=params2.get('color', 'blue'))
    return fig

def plot_intervals(ser, intervals):
    fig, ax = plt.subplots()
    ax.plot(ser.index, ser)
    ax.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    for interval in intervals:
        ser_slice = ser.ix[interval[0]: interval[1]]
        ax.plot(ser_slice.index, ser_slice, 'r')
    fig.autofmt_xdate()
    return fig

def save_fig(fig, pdf=None, png=None):
    if pdf:
        pp = PdfPages(pdf)
        pp.savefig(fig)
        pp.close()
    elif png:
        fig.savefig(png)

def save_figs(figs, pdf):
    pp = PdfPages(pdf)
    for fig in figs:
        pp.savefig(fig)
    pp.close()
