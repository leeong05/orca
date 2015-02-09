"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd
pd.set_option('display.precision', 3)

from orca.mongo.perf import PerfFetcher
perf_fetcher = PerfFetcher(datetime_index=True)
from orca.constants import DAYS_IN_YEAR
from orca.perf.performance import Performance
from orca.mongo.kday import UnivFetcher
univ_fetcher = UnivFetcher(datetime_index=True, reindex=True)
from orca.utils.dateutil import to_datestr
from orca.utils.io import read_frame

def _get_metric(analyser, metric):
    if metric == 'returns':
        return analyser.get_returns()
    elif metric == 'rIC1':
        return analyser.get_ic(1, rank=True)
    elif metric == 'rIC5':
        return analyser.get_ic(5, rank=True)
    elif metric == 'rIC20':
        return analyser.get_ic(20, rank=True)
    elif metric == 'IC1':
        return analyser.get_ic(1, rank=False)
    elif metric == 'IC5':
        return analyser.get_ic(5, rank=False)
    elif metric == 'IC20':
        return analyser.get_ic(20, rank=False)

def _get_analyser(perf, mode):
    if mode == 'longshort':
        return perf.get_longshort()
    elif mode == 'BTOP70Q':
        univ = univ_fetcher.fetch_window('BTOP70Q', to_datestr(perf.alpha.index))
        return perf.get_universe(univ).get_longshort()
    elif mode == 'quantile30':
        return perf.get_qtail(0.3)
    elif mode == 'top30':
        return perf.get_qtop(0.3)

def get_metric(perf, mode, metric):
    return _get_metric(_get_analyser(perf, mode), metric)


if __name__ == '__main__':
    import argparse
    from glob import glob

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file', nargs='*')
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'))
    parser.add_argument('--dir', help='Input directory, each file contained is assumed to be an alpha file', type=str)
    parser.add_argument('--file', help='Input file, each row in the format: name path_to_a_csv_file', type=str)
    parser.add_argument('--days', type=int, default=2*DAYS_IN_YEAR, help='How many points to be included in correlation calculation')
    parser.add_argument('--db', help='Check correlation with alphas in alphadb', action='store_true')
    parser.add_argument('--mode', choices=('longshort', 'quantile30', 'BTOP70Q', 'top30'), default='BTOP70Q')
    metrics = ('returns',) + tuple([('r' if rank else '') + 'IC' + str(n) for rank in [True, False] for n in (1, 5, 20)])
    parser.add_argument('--metric', choices=metrics, default='rIC1', help='What type of correlations is of interest?')
    parser.add_argument('--limit', type=int, default=10)
    args = parser.parse_args()

    alpha_metric, dates = {}, None
    if args.alpha:
        for path in args.alpha:
            for name in glob(path):
                df = read_frame(name, args.ftype)
                perf = Performance(df)
                name = os.path.basename(name)
                alpha_metric[name] = get_metric(perf, args.mode, args.metric)
                if dates is None:
                    dates = alpha_metric[name].index
                else:
                    dates = dates.union(alpha_metric[name].index)

    ext_alphas = {}
    if args.file:
        with open(args.file) as file:
            for line in file:
                name, fpath = line.strip().split()
                ext_alphas[name] = read_frame(fpath, args.ftype)
    if args.dir:
        assert os.path.exists(args.dir)
        for name in os.listdir(args.dir):
            ext_alphas[name] = read_frame(os.path.join(args.dir, name), args.ftype)

    extalpha_metric = {}
    if args.db:
        assert args.alpha
        db_metrics = perf_fetcher.fetch_window(args.metric, to_datestr(dates), mode=args.mode)
        for name, metric in db_metrics.iteritems():
            extalpha_metric[name] = metric

    for name, alpha in ext_alphas.iteritems():
        perf = Performance(alpha)
        extalpha_metric[name] = get_metric(perf, args.mode, args.metric)

    extmetric_df = pd.DataFrame(extalpha_metric)
    if not args.alpha:
        if len(extmetric_df) > args.days:
            extmetric_df = extmetric_df.iloc[-args.days:]
        print extmetric_df.corr()
    else:
        if len(extmetric_df) > 0:
            extmetric_df = extmetric_df.ix[dates]
            if len(extmetric_df) > args.days:
                extmetric_df = extmetric_df.iloc[-args.days:]
            for name, metric in alpha_metric.iteritems():
                corr = extmetric_df.corrwith(metric)
                corr.sort(ascending=False)
                if len(corr) > args.limit:
                    corr = corr.ix[:args.limit]
                print name
                print corr
        else:
            metric_df = pd.DataFrame(alpha_metric)
            if len(metric_df) > args.days:
                metric_df = metric_df.iloc[-args.days:]
            print metric_df.corr()
