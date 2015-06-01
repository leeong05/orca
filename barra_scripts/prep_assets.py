"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
from string import Template

import pandas as pd
import logbook
logbook.set_datetime_format('local')
logger = logbook.Logger('assets')
from orca.mongo.barra import BarraFetcher
barra_fetcher = BarraFetcher('short')

def generate_path(path_pattern, date):
    return Template(path_pattern).substitute(YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8])

def prep_assets_lance(account, date, output):
    path = os.path.join('/home/liulc/trade_'+account, 'barra', date[:4], date[4:6], date[6:8], 'assets.'+date)
    df = pd.read_csv(path, header=0, dtype={0: str}).iloc[:, :3]
    df.columns = ['sid', 'bid', 'alpha']

    output = generate_path(output, date)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    df.to_csv(output, index=False, float_format='%.6f')
    logger.info('Generated file: {}', output)

def prep_assets_alpha(alpha, date, output):
    df = pd.DataFrame({'alpha': alpha.ix[date]})
    df['sid'] = df.index
    bid_sid = barra_fetcher.fetch_idmaps(date=DATES[DATES.index(date)-1])
    sid_bid = {sid: bid for bid, sid in bid_sid.iteritems()}
    df = df.ix[df['sid'].apply(lambda x: x in sid_bid)]
    df['bid'] = df['sid'].apply(lambda x: sid_bid[x])
    df = df.reindex(columns=['sid', 'bid', 'alpha'])
    df = df.ix[df['alpha'].notnull()]

    output = generate_path(output, date)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    df.to_csv(output, index=False, float_format='%.6f')
    logger.info('Generated file: {}', output)


if __name__ == '__main__':
    import argparse
    from datetime import datetime
    from orca import DATES
    from orca.utils.io import read_frame

    parser = argparse.ArgumentParser()
    parser.add_argument('date', default=datetime.now().strftime('%Y%m%d'), nargs='?')
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('--shift', type=int, default=0)
    parser.add_argument('--source', type=str, choices=('lance', 'alpha', 'other'))
    parser.add_argument('-a', '--account', type=str)
    parser.add_argument('-i', '--input', type=str)
    parser.add_argument('-o', '--output', type=str, default='assets.${YYYYMMDD}')
    parser.add_argument('-p', '--price', type=str, choices=('prev_close', 'close', 'open', 'latest'), default='close')
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

    if args.source == 'lance':
        for date in dates:
            prep_assets_lance(args.account, date, args.output)
    elif args.source == 'alpha':
        alpha = read_frame('/home/liulc/model/product/ols/alpha.univ0')
        for date in dates:
            prep_assets_alpha(alpha, date, args.output)
