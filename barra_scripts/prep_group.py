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
from orca.mongo.industry import IndustryFetcher
industry_fetcher = IndustryFetcher()
from orca.mongo.components import ComponentsFetcher
components_fetcher = ComponentsFetcher()

def generate_path(path_pattern, date):
    return Template(path_pattern).substitute(YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8])

def prep_group(group, name, date, output, add):
    bid_sid = barra_fetcher.fetch_idmaps()
    sid_bid = {sid: bid for bid, sid in bid_sid.iteritems()}
    output = generate_path(output, date)
    if add and os.path.exists(output):
        exist = pd.read_csv(output, dtype={0: str})
        exist.index = exist['sid']
        if name in exist.columns:
            logger.info('Group already exists in file: {}', output)
            return
        exist[name] = group
        exist[name] = exist[name].fillna('Unknown')
        exist.to_csv(output, index=False)
    else:
        df = pd.DataFrame(group)
        df.columns = [name]
        sids = [sid for sid in df.index if sid in sid_bid]
        df = df.ix[sids]
        df['sid'] = df.index
        df['bid'] = df['sid'].map(sid_bid)
        df = df.reindex(columns=['sid', 'bid', name])
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
    parser.add_argument('--which', choices=('industry', 'components', 'other'), required=True)
    parser.add_argument('-a', '--add', action='store_true')
    parser.add_argument('-i', '--input', type=str)
    parser.add_argument('-o', '--output', type=str, default='group.${YYYYMMDD}')
    args = parser.parse_args()

    dates = args.date in DATES and [args.date] or []
    if args.start:
        dates = [date for date in DATES if date >= args.start]
    if args.end:
        dates = [date for date in dates if date <= args.end]
    if args.shift:
        dates = [DATES[DATES.index(date)-args.shift] for date in dates]

    for date in dates:
        if args.which == 'industry':
            group = industry_fetcher.fetch_daily('level1', date)
            prep_group(group, args.which, date, args.output, args.add)
        elif args.which == 'components':
            hs300 = components_fetcher.fetch_daily('HS300', date)
            hs300[:] = 'HS300'
            cs500 = components_fetcher.fetch_daily('CS500', date)
            cs500[:] = 'CS500'
            group = pd.concat([hs300, cs500])
            prep_group(group, args.which, date, args.output, args.add)
        elif args.which == 'other':
            assert args.input
            df = pd.read_csv(generate_path(args.input, date), header=0, dtype={0: str}, index_col=0)
            for name, group in df.iteritems():
                if name not in ('sid', 'bid'):
                    prep_group(group, name, date, args.output, args.add)
