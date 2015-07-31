"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
pd.set_option('display.precision', 3)

from orca.mongo.sywgquote import SYWGQuoteFetcher
sywgquote_fetcher = SYWGQuoteFetcher(datetime_index=True)
from orca.operation import api
from orca.perf.analyser import Analyser
from orca.utils.io import read_frame
from orca.utils.plot import plot_pnl, save_figs

import multiprocessing


def worker(args):
    alpha, analyser, cost, by, group, freq = args
    summary = analyser.summary(cost=cost, by=by, group=group, freq=freq)
    return alpha, summary


if __name__ == '__main__':
    import argparse

    parser= argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file', nargs='?')
    parser.add_argument('--alphas', nargs='*')
    parser.add_argument('-l', '--level', type=int, choices=(1, 2, 3), default=1)
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'))
    parser.add_argument('-t', '--threads', type=int, default=8)
    parser.add_argument('-f', '--freq', choices=('daily', 'weekly', 'monthly'), default='daily',
        help='Which frequency of statistics to be presented? For example, "weekly" means to show IR(5) besides IR(1) if --group is set to be "ir" or "all"')
    parser.add_argument('-b', '--by', choices=('A', 'Q', 'M', 'W'), help='Summary period')
    parser.add_argument('-g', '--group', choices=('ir', 'turnover', 'returns', 'all'), default='ir', help='Performance metrics group')
    parser.add_argument('-c', '--cost', type=float, default=0.001, help='Linear trading cost')
    parser.add_argument('--pdf', type=str)
    args = parser.parse_args()

    if args.alpha:
        if not args.alphas:
            args.alphas = []
        if args.alpha not in args.alphas:
            args.alphas.append(args.alpha)
    assert args.alphas

    alphas, dates = {}, None
    for alpha in args.alphas:
        try:
            alphadf = read_frame(alpha, args.ftype)
            if dates is None:
                dates = alphadf.index
            else:
                dates = dates.union(alphadf.index)
            alphas[alpha] = api.scale(api.neutralize(alphadf))
        except:
            print '[WARNING] Failed to parse file', alpha

    sywg_returns = sywgquote_fetcher.fetch('returns', dates[0].strftime('%Y%m%d'), dates[-1].strftime('%Y%m%d'), level=args.level)
    for alpha, alphadf in alphas.iteritems():
        analyser = Analyser(alphadf, data=sywg_returns)
        alphas[alpha] = analyser

    pool = multiprocessing.Pool(args.threads)
    res = pool.imap_unordered(worker, ((alpha, analyser, args.cost, args.by, args.group, args.freq) for alpha, analyser in alphas.iteritems()))
    pool.close()
    pool.join()

    for alpha, summary in res:
        print alpha
        print summary

    if args.pdf:
        figs = []
        for alpha, analyser in alphas.iteritems():
            fig = plot_pnl(analyser.get_returns(), by=args.by, title=alpha)
            figs.append(fig)
        save_figs(figs, args.pdf)
        print '[INFO] Saved pnl figs in', args.pdf
