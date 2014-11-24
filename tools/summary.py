"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
pd.set_option('display.precision', 3)

from orca.perf.performance import Performance
from orca.operation.api import format

if __name__ == '__main__':
    import argparse

    parser= argparse.ArgumentParser()
    parser.add_argument('file', help='Alpha file')
    parser.add_argument('-i', '--index', default='HS300', type=str)
    parser.add_argument('-q', '--quantile', type=float)
    parser.add_argument('-l', '--longonly', action='store_true')
    parser.add_argument('-n', '--number', type=int)
    parser.add_argument('-f', '--freq', choices=('daily', 'weekly', 'monthly'), default='daily')
    parser.add_argument('-b', '--by', choices=('A', 'Q', 'M', 'W'), help='Summary period')
    parser.add_argument('-g', '--group', choices=('ir', 'turnover', 'returns', 'all'), default='ir', help='Performance metrics group')
    parser.add_argument('-c', '--cost', type=float, default=0.001)
    args = parser.parse_args()

    alpha = format(pd.read_csv(args.file, parse_dates=[0], header=0, index_col=0))
    perf = Performance(alpha)
    if args.longonly:
        if args.quantile:
            analyser = perf.get_qtop(args.quantile, index=args.index)
        elif args.number:
            analyser = perf.get_ntop(args.number, index=args.index)
    else:
        if args.quantile:
            analyser = perf.get_qtail(args.quantile)
        elif args.number:
            analyser = perf.get_ntail(args.number)
        else:
            analyser = perf.get_longshort()

    summary = analyser.summary(cost=args.cost, by=args.by, group=args.group, freq=args.freq)
    print summary
