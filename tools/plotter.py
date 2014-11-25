"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from orca.perf.performance import Performance
from orca.perf.plotter import Plotter
from orca.operation.api import format

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('-i', '--index', type=str,
        help='Name of the index, for example: HS300. Set this only when --longonly is turned on')
    parser.add_argument('-q', '--quantile', type=float,
        help='When --longonly is turned on, this can be negative to choose the bottom quantile; when not, this sets a threshold to choose tail quantiles')
    parser.add_argument('-n', '--number', type=int,
        help='When --longonly is turned on, this can be negative to choose the bottom; when not, this sets a threshold to choose tail')
    parser.add_argument('-l', '--longonly', action='store_true',
        help='Whether to test this alpha as a longonly holding')
    parser.add_argument('-p', '--plot', default=['pnl'], nargs='+',
        help='What to plot? Could by any combination of ("pnl", "returns", "ic", "turnover", "ac")')
    parser.add_argument('-b', '--by', choices=('A', 'Q', 'M', 'W'), help='Summary period', default='A')
    parser.add_argument('-c', '--cost', type=float, default=0.001, help='Linear trading cost')
    parser.add_argument('--plot_index', help='Add index data for "pnl"/"returns" plot', action='store_true')
    parser.add_argument('--ma', type=int,
        help='For "ic"/"ac"/"turnover" plot, use simple moving average to smooth')
    parser.add_argument('--periods', help='Periods used in calculation of IC and AC', type=int, default=1)
    parser.add_argument('--pdf', action='store_true', help='Whether to save plots in a PDF file')
    parser.add_argument('-s', '--start', type=str, help='Starting date')
    parser.add_argument('-e', '--end', type=str, help='Ending date')
    args = parser.parse_args()

    alpha = format(pd.read_csv(args.alpha, parse_dates=[0], header=0, index_col=0))
    perf = Performance(alpha)
    if args.longonly:
        if args.quantile:
            if args.quantile > 0:
                analyser = perf.get_qtop(args.quantile, index=args.index)
            else:
                analyser = perf.get_qbottom(-args.quantile, index=args.index)
        elif args.number:
            if args.number > 0:
                analyser = perf.get_ntop(args.number, index=args.index)
            else:
                analyser = perf.get_nbottom(-args.number, index=args.index)
    else:
        if args.quantile:
            analyser = perf.get_qtail(args.quantile)
        elif args.number:
            analyser = perf.get_ntail(args.number)
        else:
            analyser = perf.get_longshort()

    plotter = Plotter(analyser)
    figs = []
    if 'pnl' in args.plot:
        fig = plotter.plot_pnl(cost=args.cost, index=args.index, plot_index=args.plot_index,
                drawdown=True, startdate=args.start, enddate=args.end)
        figs.append(fig)
    if 'returns' in args.plot:
        fig = plotter.plot_returns(args.by, index=args.index, plot_index=args.plot_index,
                startdate=args.start, enddate=args.end)
        figs.append(fig)
    if 'ic' in args.plot:
        fig = plotter.plot_ic(n=args.periods, rank=True, ma=args.ma,
                startdate=args.start, enddate=args.end)
        figs.append(fig)
    if 'tvr' in args.plot:
        fig = plotter.plot_turnover(ma=args.ma, startdate=args.start, enddate=args.end)
        figs.append(fig)

    if args.pdf:
        pdf = os.path.basename(args.alpha)+'.pdf'
        if os.path.exists(pdf):
            os.remove(pdf)
        pp = PdfPages(pdf)
        for fig in figs:
            pp.savefig(fig)
        pp.close()

    plt.show()
