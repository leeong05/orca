"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
from string import Template

import pandas as pd
import logbook
logbook.set_datetime_format('local')
logger = logbook.Logger('composite')

from orca.mongo.barra import BarraFetcher
barra_fetcher = BarraFetcher('short')
from orca.mongo.components import ComponentsFetcher
components_fetcher = ComponentsFetcher(as_bool=False)


def generate_path(path_pattern, date, sid):
    return Template(path_pattern).substitute(YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8], sid=sid)

def prep_composite_lance(account, date, sid, output):
    bid_sid = barra_fetcher.fetch_idmaps(date)
    path = os.path.join('/home/liulc/trade_'+account, 'barra', date[:4], date[4:6], date[6:8], 'benchmark.'+date)
    df = pd.read_csv(path, header=0, dtype={0: str})
    df.columns = ['bid', 'weight']
    df['sid'] = df['bid'].map(bid_sid)
    df = df.reindex(columns=['sid', 'bid', 'weight'])

    output = generate_path(output, date, sid)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    df.to_csv(output, index=False, float_format='%.6f')
    logger.info('Generated file: {}', output)

def prep_composite_mongo(date, sid, output):
    bid_sid = barra_fetcher.fetch_idmaps(date)
    sid_bid = {sid: bid for bid, sid in bid_sid.iteritems()}
    df = pd.DataFrame(components_fetcher.fetch_daily(sid, date))
    df.columns = ['weight']
    df['sid'] = df.index
    df['bid'] = df['sid'].map(sid_bid)
    df = df.reindex(columns=['sid', 'bid', 'weight'])
    df = df.ix[df['weight'] > 0]
    df['weight'] /= df['weight'].sum()

    output = generate_path(output, date, sid)
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
    parser.add_argument('--source', type=str, choices=('lance', 'mongo'), default='mongo')
    parser.add_argument('-a', '--account', type=str)
    parser.add_argument('-i', '--input', type=str)
    parser.add_argument('--index', nargs='+')
    parser.add_argument('-o', '--output', type=str, default='${sid}.${YYYYMMDD}')
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
            prep_composite_lance(args.account, date, args.index[0], args.output)
    elif args.source == 'mongo':
        for date in dates:
            for index in args.index:
                prep_composite_mongo(date, index, args.output)
