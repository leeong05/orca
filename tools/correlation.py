"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd
pd.set_option('display.precision', 3)

from orca.constants import DAYS_IN_YEAR
from orca.operation.api import format
from orca.perf.performance import Performance

def corr(s1, s2, days):
    index = s1.index.intersection(s2.index)
    if len(index) > days:
        index = index[-days:]
    return s1.ix[index].corr(s2.ix[index])

def get_pairwise_corr(name_series, days):
    df = pd.DataFrame(name_series)
    index = df.index
    for name, series in df.iteritems():
        index = index.intersection(series.index)
    if len(index) > days:
        index = index[-days:]
    df = df.ix[index]
    return df.corr()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file', nargs='*')
    parser.add_argument('-q', '--quantile', help='Sets threshold for tail quantiles to calculate returns', type=float)
    parser.add_argument('--db', help='Check correlation with alphas in alphadb', action='store_true')
    parser.add_argument('-m', '--method', choices=('ic', 'returns', 'both'), default='both', help='What type of correlations is of interest?')
    parser.add_argument('--dir', help='Input directory, each file contained is assumed to be an alpha file', type=str)
    parser.add_argument('--file', help='Input file, each row in the format: name path_to_a_csv_file', type=str)
    parser.add_argument('--days', type=int, default=2*DAYS_IN_YEAR, help='How many points to be included in correlation calculation')
    args = parser.parse_args()

    ext_alphas = {}
    if args.file:
        with open(args.file) as file:
            for line in file:
                name, fpath = line.split('\s+')
                ext_alphas[name] = format(pd.read_csv(fpath, header=0, parse_dates=[0], index_col=0))
    if args.dir:
        assert os.path.exists(args.dir)
        for name in os.listdir(args.dir):
            ext_alphas[name] = format(pd.read_csv(os.path.join(args.dir, name), header=0, parse_dates=[0], index_col=0))
    for name, alpha in ext_alphas:
        perf = Performance(alpha)
        if args.quantile:
            ext_alphas[name] = perf.get_qtail(args.quantile)
        else:
            ext_alphas[name] = perf.get_longshort()

    if args.alpha is None:
        if args.method == 'ic':
            ics_dict = {name: analyser.get_ic(rank=True) for name, analyser in ext_alphas.iteritems()}
            ics_corr = get_pairwise_corr(ics_dict, args.days)
            print ics_corr
        elif args.method == 'returns':
            rets_dict = {name: analyser.get_returns() for name, analyser in ext_alphas.iteritems()}
            rets_corr = get_pairwise_corr(rets_dict, args.days)
            print rets_corr
        else:
            ics_dict = {name: analyser.get_ic(rank=True) for name, analyser in ext_alphas.iteritems()}
            rets_dict = {name: analyser.get_returns() for name, analyser in ext_alphas.iteritems()}
            ics_corr = get_pairwise_corr(ics_dict, args.days)
            rets_corr = get_pairwise_corr(rets_dict, args.days)
            print ics_corr
            print ''
            print rets_corr
    else:
        alpha_analyser = None
        for i, name in enumerate(args.alpha):
            df = pd.read_csv(name, header=0, parse_dates=[0], index_col=0)
            perf = Performance(format(df))
            if args.quantile:
                analyser = perf.get_qtail(args.quantile)
            else:
                analyser = perf.get_longshort()
            if i == 0:
                alpha_analyser = analyser
            else:
                ext_alphas[name] = analyser

        if args.method == 'ic':
            ic = alpha_analyser.get_ic(rank=True)
            ics_dict = {name: analyser.get_ic(rank=True) for name, analyser in ext_alphas.iteritems()}
            ic_corr = pd.Series({name: corr(ic, ic_, args.days) for name, ic_ in ics_dict.iteritems()})
            print ic_corr.sort(ascending=False)
        elif args.method == 'ir':
            ret = alpha_analyser.get_returns()
            rets_dict = {name: analyser.get_returns() for name, analyser in ext_alphas.iteritems()}
            ret_corr = pd.Series({name: corr(ret, ret_, args.days) for name, ret_ in rets_dict.iteritems()})
            print ret_corr.sort(ascending=False)
        else:
            ic = alpha_analyser.get_ic(rank=True)
            ics_dict = {name: analyser.get_ic(rank=True) for name, analyser in ext_alphas.iteritems()}
            ic_corr = pd.Series({name: corr(ic, ic_, args.days) for name, ic_ in ics_dict.iteritems()})
            ret = alpha_analyser.get_returns()
            rets_dict = {name: analyser.get_returns() for name, analyser in ext_alphas.iteritems()}
            ret_corr = pd.Series({name: corr(ret, ret_, args.days) for name, ret_ in rets_dict.iteritems()})
            res = pd.concat([ic_corr, ret_corr], axis=1, keys=['ic', 'returns'])
            print res.sort(['ic', 'returns'], ascending=False)
