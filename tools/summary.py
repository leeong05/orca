"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd
pd.set_option('display.precision', 3)

from orca.mongo.kday import UnivFetcher
univ_fetcher = UnivFetcher(datetime_index=True, reindex=True)

from orca.perf.performance import (
        Performance,
        IntPerformance,
        )
from orca.utils.io import read_frame

import multiprocessing

def worker_i(args):
    alpha, analyser, cost, by, group = args
    summary = analyser.summary(cost=cost, by=by, group=group)
    return alpha, summary

def worker_d(args):
    alpha, analyser, cost, by, group, freq = args
    summary = analyser.summary(cost=cost, by=by, group=group, freq=freq)
    return alpha, summary


if __name__ == '__main__':
    import argparse
    import cPickle

    parser= argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file', nargs='?')
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'))
    parser.add_argument('--atype', help='Alpha type', choices=('daily', 'intraday', 'perf'))
    parser.add_argument('--alphas', nargs='*')
    parser.add_argument('-t', '--threads', type=int, default=8)
    parser.add_argument('--univ', help='Universe name', type=str)
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

    if args.alpha:
        if not args.alphas:
            args.alphas = []
        if args.alpha not in args.alphas:
            args.alphas.append(args.alpha)
    assert args.alphas

    alphas = {}
    if args.atype == 'perf':
        for alpha in args.alphas:
            try:
                with open(alpha) as file:
                    perf = cPickle.load(file)
                alphas[alpha] = perf
                if hasattr(perf, 'freq'):
                    args.atype = 'intraday'
                else:
                    args.atype = 'daily'
            except:
                print '[WARNING] Failed to parse file', alpha
    else:
        for alpha in args.alphas:
            try:
                alphadf = read_frame(alpha, args.ftype)
                if args.atype is None:
                    if len(alphadf.index) == len(np.unique(alphadf.index.date)):
                        args.atype = 'daily'
                    else:
                        args.atype = 'intraday'

                if args.atype == 'intraday':
                    perf = IntPerformance(alphadf)
                else:
                    perf = Performance(alphadf)
                alphas[alpha] = perf
            except:
                print '[WARNING] Failed to parse file', alpha

    if args.univ:
        assert args.univ in univ_fetcher.dnames
        dates = np.unique([dt.strftime('%Y%m%d') for dt in perf.alpha.index])
        univ = univ_fetcher.fetch_window(args.univ, dates)
        alphas = {alpha: perf.get_universe(univ) for alpha, perf in alphas.iteritems()}

    if args.longonly:
        if args.quantile:
            if args.quantile > 0:
                alphas = {alpha: perf.get_qtop(args.quantile, index=args.index) for alpha, perf in alphas.iteritems()}
            else:
                alphas = {alpha: perf.get_qbottom(-args.quantile, index=args.index) for alpha, perf in alphas.iteritems()}
        elif args.number:
            if args.number > 0:
                alphas = {alpha: perf.get_ntop(args.number, index=args.index) for alpha, perf in alphas.iteritems()}
            else:
                alphas = {alpha: perf.get_nbottom(-args.number, index=args.index) for alpha, perf in alphas.iteritems()}
    else:
        if args.quantile:
            alphas = {alpha: perf.get_qtail(args.quantile) for alpha, perf in alphas.iteritems()}
        elif args.number:
            alphas = {alpha: perf.get_ntail(args.number) for alpha, perf in alphas.iteritems()}
        else:
            alphas = {alpha: perf.get_longshort() for alpha, perf in alphas.iteritems()}

    pool = multiprocessing.Pool(args.threads)
    if args.atype == 'intraday':
        res = pool.imap_unordered(worker_i, ((alpha, analyser, args.cost, args.by, args.group) for alpha, analyser in alphas.iteritems()))
    else:
        res = pool.imap_unordered(worker_d, ((alpha, analyser, args.cost, args.by, args.group, args.freq) for alpha, analyser in alphas.iteritems()))
    pool.close()
    pool.join()
    for alpha, summary in res:
        print alpha
        print summary
