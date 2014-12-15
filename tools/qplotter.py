"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from orca.perf.performance import Performance
from orca.perf.plotter import QuantilesPlotter
from orca.operation.api import format

def read_frame(fname, ftype='csv'):
    if ftype == 'csv':
        return format(pd.read_csv(fname, header=0, parse_dates=[0], index_col=0))
    elif ftype == 'pickle':
        return pd.read_pickle(fname)
    elif ftype == 'msgpack':
        return pd.read_msgpack(fname)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'), default='csv')
    parser.add_argument('-q', '--quantile', help='Number of quantiles', type=int, required=True)
    parser.add_argument('-p', '--plot', default=['pnl'], nargs='+',
            help='What to plot? Could by any combination of ("pnl", "returns")')
    parser.add_argument('-b', '--by', choices=('A', 'Q', 'M', 'W'), help='Summary period')
    parser.add_argument('--pdf', action='store_true', help='Whether to save plots in a PDF file')
    parser.add_argument('-s', '--start', type=str, help='Starting date')
    parser.add_argument('-e', '--end', type=str, help='Ending date')
    args = parser.parse_args()

    alpha = read_frame(args.alpha, args.ftype)
    perf = Performance(alpha)
    plotter = QuantilesPlotter(perf.get_quantiles(args.quantile))

    figs = []
    if 'pnl' in args.plot:
        fig = plotter.plot_pnl(args.startdate, args.enddate)
        figs.append(fig)
    if 'returns' in args.plot:
        fig = plotter.plot_returns(args.by, args.startdate, args.enddate)
        figs.append(fig)

    if args.pdf:
        pdf = os.path.basename(args.alpha)+'-'+str(args.quantile)+'.pdf'
        if os.path.exists(pdf):
            os.remove(pdf)
        pp = PdfPages(pdf)
        for fig in figs:
            pp.savefig(fig)
        pp.close()

    plt.show()
