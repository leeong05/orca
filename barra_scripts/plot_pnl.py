"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from string import Template

import numpy as np
import pandas as pd
import logbook
logbook.set_datetime_format('local')
logger = logbook.Logger('plotter')

from orca.mongo.quote import QuoteFetcher
quote_fetcher = QuoteFetcher(datetime_index=True)
from orca.mongo.index import IndexQuoteFetcher
index_quote_fetcher = IndexQuoteFetcher(datetime_index=True)

def generate_path(path_pattern, sid, date):
    return Template(path_pattern).substitute(sid=sid,
                YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8])

def get_returns(sid, pattern=None):
    try:
        returns = index_quote_fetcher.fetch_window('returns', shift_dates, index=sid)
    except:
        assert pattern is not None
        path0 = generate_path(pattern, sid, dates[0])
        path1 = generate_path(pattern, sid, dates[-1])
        if path0 == path1:
            df = read_frame(path0)
            df.index = pd.to_datetime(shift_dates)
            returns = (df * stock_returns[df.columns]).sum(axis=1)
        else:
            returns = {}
            for date, shift_date in zip(dates, shift_dates):
                path = generate_path(pattern, sid, date)
                df = pd.read_csv(path, header=0, dtype={0: str})
                df.index = df['sid']
                returns[shift_date] = (df['weight'] * stock_returns.ix[shift_date].ix[df.index]).sum()
            returns = pd.Series(returns)
            returns.index = pd.to_datetime(shift_dates)
    return returns


if __name__ == '__main__':
    import argparse
    from orca.utils.io import read_frame
    from orca import DATES, SIDS
    from orca.utils.plot import (
            plot_pnl,
            save_figs,
            )

    parser = argparse.ArgumentParser()
    parser.add_argument('position', type=str, nargs='+')
    parser.add_argument('--composite', type=str)
    parser.add_argument('--cost', type=float, default=0.001)
    parser.add_argument('--pdf', type=str, required=True)
    parser.add_argument('--legend', type=str, nargs='*')
    args = parser.parse_args()

    if not args.legend or len(args.legend) != len(args.position):
        args.legend = range(len(args.position))

    positions= []
    dates, sids = None, None
    for fname in args.position:
        position = read_frame(fname)
        if dates is None:
            dates = position.index
        else:
            dates = dates.intersection(position.index)
        if sids is None:
            sids = set(position.columns)
        else:
            sids = sids | set(position.columns)
        positions.append(position)

    for i, position in enumerate(positions):
        position = position.ix[dates]
        positions[i] = position
    dates = [date.strftime('%Y%m%d') for date in dates]

    di = DATES.index(dates[0])
    shift_dates = DATES[di+1:di+len(dates)+1]
    for i, position in enumerate(positions):
        position.index = pd.to_datetime(shift_dates)
        positions[i] = position

    stock_returns = quote_fetcher.fetch_window('returns', shift_dates)

    regular_sids = [sid for sid in sids if sid in SIDS]
    composite_sids = [sid for sid in sids if sid not in SIDS]
    composite_returns = {}
    for sid in composite_sids:
        composite_returns[sid] = get_returns(sid, args.composite)
    composite_returns = pd.DataFrame(composite_returns)

    returns = pd.concat([stock_returns, composite_returns], axis=1)

    figs = []
    for legend, position in zip(args.legend, positions):
        turnover = np.abs(position.shift(1).fillna(0)-position.fillna(0)).sum(axis=1)
        turnover.iloc[0] = 0
        pnl = (position * returns[position.columns]).sum(axis=1)
        cost_pnl = pnl - args.cost*turnover
        fig = plot_pnl(cost_pnl, title=legend)
        figs.append(fig)
    save_figs(figs, pdf=args.pdf)
