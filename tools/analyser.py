"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca.utils.misc import (
        fetch_returns,
        fetch_dates,
        )
from orca.mongo.barra import BarraExposureFetcher
barra_exposure_fetcher = BarraExposureFetcher('short')
from orca.utils.plot import (
        plot_ser_bar,
        save_fig,
        )


if __name__ == '__main__':
    import argparse
    from orca.utils.io import read_frame

    parser = argparse.ArgumentParser()
    parser.add_argument('univ', type=str)
    parser.add_argument('-e', '--ext', type=str, nargs='*')
    parser.add_argument('-l', '--label', type=str, nargs='*')
    parser.add_argument('--lshift', type=int, default=-1)
    parser.add_argument('--rshift', type=int, default=20)
    parser.add_argument('--pdf', type=str, required=True)
    args = parser.parse_args()

    if len(args.label) != len(args.ext):
        args.label = ['factor_'+str(i) for i in range(len(args.ext))]

    univ = read_frame(args.univ).astype(bool)
    exts = {}
    for label, ext in zip(args.label, args.ext):
        exts[label] = read_frame(ext)

    returns = fetch_returns(univ.index, args.rshift, args.lshift)
    univ = univ.ix[returns.index]

    for k, v in exts.iteritems():
        exts[k] = fetch_dates(v, univ.index)

    exps = barra_exposure_fetcher.fetch_dates('style', univ.index)
    exps.major_axis = [m[6:] for m in exps.major_axis]
    for factor in exps.major_axis:
        exts[factor] = exps.major_xs(factor)

    irs = {}
    for k, v in exts.iteritems():
        v_univ = v[univ]
        v_ic = v_univ.corrwith(returns, axis=1)
        irs[k] = v_ic.mean()/v_ic.std()
    irs = pd.Series(irs)
    irs.sort(ascending=False)
    fig = plot_ser_bar(irs)
    save_fig(fig, args.pdf)
