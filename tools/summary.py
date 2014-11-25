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
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('-i', '--index', default='HS300', type=str,
        help='Index name; se this only when option --longonly is turned on')
    parser.add_argument('-q', '--quantile', type=float,
        help='When --longonly is turned on, this can be negative to choose the bottom quantile; when not, this sets a threshold to choose tail quantiles')
    parser.add_argument('-n', '--number', type=int,
        help='When --longonly is turned on, this can be negative to choose the bottom; when not, this sets a threshold to choose tail')
    parser.add_argument('-l', '--longonly', action='store_true',
        help='Whether to test this alpha as a longonly holding')
    parser.add_argument('-f', '--freq', choices=('daily', 'weekly', 'monthly'), default='daily',
        help='Which frequency of statistics to be presented? For example, "weekly" means to show IR(5) besides IR(1) if --group is set to be "ir" or "all"')
    parser.add_argument('-b', '--by', choices=('A', 'Q', 'M', 'W'), help='Summary period')
    parser.add_argument('-g', '--group', choices=('ir', 'turnover', 'returns', 'all'), default='ir', help='Performance metrics group')
    parser.add_argument('-c', '--cost', type=float, default=0.001, help='Linear trading cost')
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

    summary = analyser.summary(cost=args.cost, by=args.by, group=args.group, freq=args.freq)
    print summary
