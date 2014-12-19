"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd
pd.set_option('display.precision', 3)

from orca.perf.performance import (
        Performance,
        IntPerformance,
        )
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
    import cPickle

    parser= argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'), default='csv')
    parser.add_argument('--atype', help='Alpha type', choices=('daily', 'intraday', 'perf'))
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

    if args.atype == 'perf':
        with open(args.alpha) as file:
            perf = cPickle.load(file)
            if hasattr(perf, 'freq'):
                args.atype = 'intraday'
            else:
                args.atype = 'daily'
    else:
        alpha = read_frame(args.alpha, args.ftype)
        if args.atype is None:
            if len(alpha.index) == len(np.unique(alpha.index.date)):
                args.atype = 'daily'
            else:
                args.atype = 'intraday'

        if args.atype == 'intraday':
            perf = IntPerformance(alpha)
        else:
            perf = Performance(alpha)
        with open(args.alpha+'.pickle', 'w') as file:
            cPickle.dump(perf, file)

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

    if args.atype == 'intraday':
        summary = analyser.summary(cost=args.cost, by=args.by, group=args.group)
    else:
        summary = analyser.summary(cost=args.cost, by=args.by, group=args.group, freq=args.freq)
    print summary
