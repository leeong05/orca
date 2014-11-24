"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from orca.perf.performance import Performance
from orca.perf.plotter import QuantilesPlotter
from orca.operation.api import format

if __name__ == '__main__':
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('-q', '--quantile', type=int, required=True)
    parser.add_argument('-p', '--plot', default=['pnl'], nargs='+',
            help='What to plot? Could by any combination of ("pnl", "returns")')
    parser.add_argument('-b', '--by', choices=('A', 'Q', 'M', 'W'))
    parser.add_argument('--pdf', action='store_true')
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    args = parser.parse_args()

    alpha = format(pd.read_csv(args.alpha, parse_dates=[0], header=0, index_col=0))
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
