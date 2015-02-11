"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import magic

from orca.perf.performance import Performance
from orca.perf.plotter import QuantilesPlotter
from orca.utils.io import read_frame


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
    parser.add_argument('--png', type=str, help='PNG file to save the plot')
    parser.add_argument('-s', '--start', type=str, help='Starting date')
    parser.add_argument('-e', '--end', type=str, help='Ending date')
    args = parser.parse_args()

    if args.pdf and os.path.exists(args.pdf):
        with magic.Magic() as m:
            ftype = m.id_filename(args.pdf)
            if ftype[:3] != 'PDF':
                print 'The argument --pdf if exists must be a PDF file'
                exit(0)
    if args.png and os.path.exists(args.png):
        with magic.Magic() as m:
            ftype = m.id_filename(args.png)
            if ftype[:3] != 'PNG':
                print 'The argument --png if exists must be a PNG file'
                exit(0)

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
        print 'Saved plots in {}'.format(args.pdf)
    elif args.png:
        for i, fig in enumerate(figs):
            fig.savefig(str(i)+'_'+args.png)
        print 'Saved plots in 0-{}_{}'.format(len(figs), args.pdf)
    else:
        plt.show()
