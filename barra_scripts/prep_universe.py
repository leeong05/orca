"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
from string import Template

import pandas as pd
import logbook
logbook.set_datetime_format('local')
logger = logbook.Logger('universe')

from orca.mongo.barra import BarraFetcher
barra_fetcher = BarraFetcher('short')
bid_sid = barra_fetcher.fetch_idmaps()

def generate_path(path_pattern, date):
    return Template(path_pattern).substitute(YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8])

def prep_universe_lance(account, date, output):
    path = os.path.join('/home/liulc/trade_'+account, 'barra', date[:4], date[4:6], date[6:8], 'universe.'+date)
    bid = pd.read_csv(path, header=0, dtype={0: str}).iloc[:,0]
    sid = bid.apply(lambda x: bid_sid.get(x, x))
    is_regular = sid.apply(lambda x: len(x) == 6 and x[:2] in ('00', '30', '60'))
    df = pd.concat([sid.ix[is_regular], bid.ix[is_regular]], axis=1)
    df.columns = ['sid', 'bid']

    output = generate_path(output, date)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    df.to_csv(output, index=False)
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
    parser.add_argument('-o', '--output', type=str, default='universe.${YYYYMMDD}')
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
            prep_universe_lance(args.account, date, args.output)
