"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=pd.core.common.SettingWithCopyWarning)
from lxml import etree

from orca import DATES
from orca.barra.base import BarraOptimizerBase
from orca.mongo.barra import BarraFetcher
barra_fetcher = BarraFetcher('short')
from orca.mongo.quote import QuoteFetcher
quote_fetcher = QuoteFetcher()
from orca.mongo.index import IndexQuoteFetcher
index_quote_fetcher = IndexQuoteFetcher()
from orca.mongo.components import ComponentsFetcher
components_fetcher = ComponentsFetcher(as_bool=False)

config = etree.XML("""<Optimize>
<Assets><Composite/></Assets>
<InitPortfolio/>
<Universe/>
<RiskModel path="/home/SambaServer/extend_data/Barra/short/${YYYY}/${MM}/${DD}" name="CNE5S"/>
<Case>
  <Utility/>
  <Constraints>
    <HedgeConstraints>
      <Leverage>
        <Net lower="1" upper="1"/>
      </Leverage>
    </HedgeConstraints>
    <LinearConstraints>
      <AssetRange>
        <Composite/>
      </AssetRange>
    </LinearConstraints>
  </Constraints>
</Case>
<Solver/>
</Optimize>""")

pattern = '/home/liulc/trade_{account}/tradelog/{yyyy}/{mm}/{filename}.{date}'
assets_pattern = '/home/liulc/trade_{account}/barra/{yyyy}/{mm}/{dd}/assets.{date}'

futures_index = {
    'IF': 'HS300',
    'IC': 'CS500',
    'IH': 'SH50',
    }

futures_multiplier = {
    'HS300': 300,
    'CS500': 200,
    'SH50': 300,
    }

def get_idmaps(date):
    bid_sid = barra_fetcher.fetch_idmaps(date)
    sid_bid = {sid: bid for bid, sid in bid_sid.iteritems()}
    return bid_sid, sid_bid

def read_regulars(date, account, alpha=None):
    regulars = pattern.format(account=account, date=date, yyyy=date[:4], mm=date[4:6], filename='start_shares')
    regulars = pd.read_csv(regulars, sep='\s+', header=0, dtype={0: str})
    regulars.columns = ['sid', 'shares']
    regulars.index = regulars['sid']
    del regulars['sid']
    regulars['close'] = quote_fetcher.fetch_daily('fclose', date, offset=1).ix[regulars.index]
    regulars['weight'] = regulars['close']*regulars['shares']
    regulars['weight'] /= regulars['weight'].sum()
    if alpha is None:
        path = assets_pattern.format(account=account, date=date, yyyy=date[:4], mm=date[4:6], dd=date[6:8])
        assets = pd.read_csv(path, header=0, dtype={0:str})
        assets.columns = ['sid', 'bid', 'alpha', 'industry', 'size', 'special']
        assets.index = assets.sid
        regulars['alpha'] = assets['alpha'].ix[regulars.index].fillna(0)
    else:
        regulars['alpha'] = alpha.ix[regulars.index].fillna(0)
    return regulars

def read_composites(date, account):
    date = DATES[DATES.index(date)-1]
    composites = pattern.format(account=account, date=date, yyyy=date[:4], mm=date[4:6], filename='end_ifs')
    composites = pd.read_csv(composites, sep='\s+', header=0, index_col=0)
    composites.index = [futures_index[sid[:2]] for sid in composites.index]
    composites.index.name, composites.columns = 'sid', ['shares']
    composites['close'], composites['weight'] = 0, 0
    for sid in composites.index:
        close = index_quote_fetcher.fetch_daily('close', date, index=sid)
        composites.loc[sid, 'close'] = close
        composites.loc[sid, 'weight'] = close * composites.loc[sid, 'shares'] * futures_multiplier[sid]
    composites['weight'] /= composites['weight'].sum()
    return composites

def dump_assets(regulars, dir, config):
    path = os.path.join(dir, 'assets')
    assets_config = config.xpath('//Assets')[0]
    assets_config.attrib['path'] = path
    df = regulars[['bid', 'alpha']]
    df.to_csv(path, index=True)

def dump_composite(date, sid, dir, config):
    path = os.path.join(dir, sid)
    composite_config = config.xpath('//Assets/Composite')[0]
    element = etree.XML('<{sid} path="{path}"/>'.format(sid=sid, path=path))
    composite_config.append(element)
    df = pd.DataFrame(components_fetcher.fetch_daily(sid, date))
    df.index.name, df.columns = 'sid', ['weight']
    df['weight'] /= df['weight'].sum()
    df['bid'] = {sid_bid[sid] for sid in df.index}
    df = df.reindex(columns=['bid', 'weight'])
    df.to_csv(path, index=True, float_format='%.6f')

def dump_init_portfolio(regulars, composites, dir, config):
    path = os.path.join(dir, 'init_portfolio')
    init_portfolio_config = config.xpath('InitPortfolio')[0]
    init_portfolio_config.attrib['path'] = path
    df = regulars[['bid', 'weight']]
    df['type'] = 'eREGULAR'
    df = df.reindex(columns=['bid', 'type', 'weight'])
    for sid, weight in composites['weight'].iteritems():
        df.ix[sid] = [sid, 'eCOMPOSITE', -weight]
    df.to_csv(path, index=True, float_format='%.6f')

def dump_universe(regulars, dir, config):
    path = os.path.join(dir, 'universe')
    universe_config = config.xpath('Universe')[0]
    universe_config.attrib['path'] = path
    df = regulars[['bid']]
    df.to_csv(path, index=True, float_format='%.6f')

def set_risk_target(risk, config):
    case_config = config.xpath('//Case')[0]
    case_config.attrib['risk_target'] = str(risk)

def set_utility(lambda_f, lambda_d, config):
    utility_config = config.xpath('//Case/Utility')[0]
    utility_config.attrib['lambda_f'] = str(lambda_f)
    utility_config.attrib['lambda_d'] = str(lambda_d)

def set_regulars_range(regulars, dir, multiplier, config):
    path = os.path.join(dir, 'asset_range')
    asset_range_config = config.xpath('//LinearConstraints/AssetRange')[0]
    asset_range_config.attrib['path'] = path
    asset_range_config.attrib['upper'], asset_range_config.attrib['lower'] = str(0), str(0)
    asset_range_config.attrib['relative'] = 'eABSOLUTE'
    df = regulars[['bid', 'weight']]
    df.columns = ['bid', 'upper']
    df['upper'] *= multiplier
    df['lower'], df['relative'] = 0, 'eABSOLUTE'
    df = df.reindex(columns=['bid', 'lower', 'upper', 'relative'])
    df.to_csv(path, index=True, float_format='%.6f')

def set_composite_range(sid, weight, config):
    composite_config = config.xpath('//LinearConstraints/AssetRange/Composite')[0]
    element = etree.XML('<{sid} lower="-{weight}" upper="-{weight}"/>'.format(sid=sid, weight=weight))
    composite_config.append(element)

def set_solver(dir, config):
    solver_config = config.xpath('//Solver')[0]
    solver_config.attrib['input'] = os.path.join(dir, 'input')
    solver_config.attrib['output'] = os.path.join(dir, 'output')
    solver_config.attrib['weight'] = os.path.join(dir, 'weight')


if __name__ == '__main__':
    import argparse
    from orca.utils.io import read_frame

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--account', type=str, required=True)
    parser.add_argument('-d', '--date', type=str, required=True)
    parser.add_argument('-o', '--output', type=str, required=True)
    parser.add_argument('-r', '--risk', type=float)
    parser.add_argument('--lambda_f', type=float, default=0.05)
    parser.add_argument('--lambda_d', type=float, default=0.025)
    parser.add_argument('-x', '--exclude', type=str, required=True)
    parser.add_argument('--alpha', type=str)
    args = parser.parse_args()

    if args.date not in DATES:
        exit(1)

    if args.alpha:
        args.alpha = read_frame(args.alpha).ix[args.date]

    if not os.path.exists(args.output):
        os.makedirs(args.output)

    bid_sid, sid_bid = get_idmaps(args.date)

    regulars = read_regulars(args.date, args.account, args.alpha)
    regulars['bid'] = [sid_bid[sid] for sid in regulars.index]

    composites = read_composites(args.date, args.account)
    assert args.exclude in composites.index and len(composites.index) > 1

    dump_assets(regulars, args.output, config)
    for sid in composites.index:
        dump_composite(args.date, sid, args.output, config)
    dump_init_portfolio(regulars, composites, args.output, config)
    dump_universe(regulars, args.output, config)
    if args.risk:
        set_risk_target(args.risk, config)
    set_utility(args.lambda_f, args.lambda_d, config)

    multiplier = 1./(1-composites['weight'].ix[args.exclude])
    set_regulars_range(regulars, args.output, multiplier, config)
    for sid in composites.index:
        if sid != args.exclude:
            set_composite_range(sid, composites['weight'].ix[sid]*multiplier, config)
        else:
            set_composite_range(sid, 0, config)
    set_solver(args.output, config)

    optimizer = BarraOptimizerBase(config)
    optimizer.run(args.date)

    remainder = optimizer.output_portfolio_df.query('type == "eREGULAR"')
    remainder['weight'] /= multiplier
    remainder['weight'] /= regulars['weight'].ix[remainder.index]
    remainder = (regulars['shares'].ix[remainder.index]*remainder['weight']).astype(int)
    remainder_full = remainder.reindex(index=regulars.index).fillna(0)
    trade_full = regulars['shares'] - remainder_full

    trade_full = (trade_full/100).astype(int)*100
    remainder_full = regulars['shares'] - trade_full

    remainder = remainder_full[remainder_full > 0]
    trade = trade_full[trade_full > 0]

    remainder.to_csv(os.path.join(args.output, 'remainder'), index=True)
    trade.to_csv(os.path.join(args.output, 'trade'), index=True)
    optimizer.logger.info('Trade: {}, Remainder: {}', len(trade), len(remainder))
