"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
from string import Template

import pandas as pd
import logbook
logbook.set_datetime_format('local')
logger = logbook.Logger('init_portfolio')

from orca.mongo.kday import UnivFetcher
univ_fetcher = UnivFetcher(as_list=True)
from orca.mongo.barra import BarraFetcher
barra_fetcher = BarraFetcher('short')
bid_sid = barra_fetcher.fetch_idmaps()

def generate_path(path_pattern, date):
    return Template(path_pattern).substitute(YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8])

def prep_init_portfolio_lance(account, date, remove_suspension, output, output_suspension):
    path = os.path.join('/home/liulc/trade_'+account, 'barra', date[:4], date[4:6], date[6:8], 'init_portfolio.'+date)
    df = pd.read_csv(path, header=0, dtype={0: str})
    df.columns = ['bid', 'weight']
    df['sid'] = [bid_sid.get(bid, bid) for bid in df.bid]
    df = df.reindex(columns=['sid', 'bid', 'weight'])
    is_cash = df['sid'].apply(lambda x: x.upper() == 'CASH')
    df = df.ix[~is_cash]
    is_regular = df['sid'].apply(lambda x: len(x) == 6 and x[:2] in ('00', '30', '60'))
    df['type'] = 'Regular'
    df.loc[~is_regular, 'type'] = 'Composite'
    if remove_suspension:
        suspension = univ_fetcher.fetch_daily('SUSPENSION', date)
        is_suspended = df['sid'].apply(lambda x: x in suspension)
        suspended = df.ix[is_suspended]
        if len(suspended) > 0:
            logger.info('Removed {} sids with total weight {} in suspension', len(suspended), suspended['weight'].sum())
            output_suspension = generate_path(output_suspension, date)
            suspended.to_csv(output_suspension, index=False, float_format='%.6f')

            original_long_regular = df.ix[is_regular & df['weight'] > 0]
            cleaned_long_regular = df.ix[is_regular & df['weight'] > 0 & ~is_suspended]
            cleaned_long_regular['weight'] *= original_long_regular['weight'].sum()/cleaned_long_regular['weight'].sum()

            original_short_regular = df.ix[is_regular & df['weight'] > 0]
            cleaned_short_regular = df.ix[is_regular & df['weight'] < 0 & ~is_suspended]
            cleaned_short_regular['weight'] *= original_short_regular['weight'].sum()/cleaned_short_regular['weight'].sum()
            df = pd.concat([df.ix[~is_regular], cleaned_long_regular, cleaned_short_regular])

    output = generate_path(output, date)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    df.to_csv(output, index=False, float_format='%.6f')
    logger.info('Generated file: {}', output)


if __name__ == '__main__':
    import argparse
    from datetime import datetime
    from orca import DATES

    parser = argparse.ArgumentParser()
    parser.add_argument('date', default=datetime.now().strftime('%Y%m%d'), nargs='?')
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('--shift', type=int, default=0)
    parser.add_argument('--source', type=str, choices=('lance', 'alpha', 'other'))
    parser.add_argument('-a', '--account', type=str)
    parser.add_argument('-i', '--input', type=str)
    parser.add_argument('-r', '--remove_suspension', action='store_true')
    parser.add_argument('-o', '--output', type=str, default='init_portfolio.${YYYYMMDD}')
    parser.add_argument('--output_suspension', type=str)
    args = parser.parse_args()

    dates = args.date in DATES and [args.date] or []
    if args.start:
        dates = [date for date in DATES if date >= args.start]
    if args.end:
        dates = [date for date in dates if date <= args.end]
    if args.shift:
        dates = [DATES[DATES.index(date)-args.shift] for date in dates]

    if args.account:
        args.source = 'lance'
    if args.input:
        args.source = 'other'
    assert args.source

    if args.output_suspension is None:
        args.output_suspension = os.path.join(os.path.dirname(args.output), 'suspension.${YYYYMMDD}') if os.path.dirname(args.output) else 'suspension.${YYYYMMDD}'

    if args.source == 'lance':
        for date in dates:
            prep_init_portfolio_lance(args.account, date, args.remove_suspension, args.output, args.output_suspension)
