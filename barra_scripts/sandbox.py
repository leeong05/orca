"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca.barra.base import BarraOptimizerBase
from orca.barra import util


class BarraOptimizer(BarraOptimizerBase):

    def __init__(self, config, alpha, univ, dates):
        super(BarraOptimizerBase, self).__init__(config)
        self.alpha = alpha
        self.univ = univ
        self.dates = dates
        self.positions = {}

    def before(self, date):
        alpha, univ = self.alpha.ix[date], self.univ.ix[date].astype(bool)

        alpha = pd.DataFrame({'weight': alpha}).dropna()
        alpha['sid'] = alpha.index
        alpha['bid'] = alpha['sid'].map(self.sid_bid)
        alpha = alpha.reindex(['sid', 'bid', 'weight'])
        config = self.config.xpath('Assets')[0]
        path = util.generate_path(config.attrib['path'], date)
        alpha.to_csv(path, index=False, float_format='%.6f')

        univ = pd.DataFrame({'sid': univ.ix[univ].index})
        univ['bid'] = univ['sid'].map(self.sid_bid)
        config = self.config.xpath('Univ')[0]
        path = util.generate_path(config.attrib['path'], date)
        univ.to_csv(path, index=False)

    def after(self, date):
        self.positions[date] = self.output_portfolio_df['weight']
        if date == self.dates[-1]:
            self.positions = pd.DataFrame(self.positions)
            self.positions.to_csv('positions', index=True, float_format='%.6f')
            return
        ndate = self.dates[self.dates.index(date)+1]
        config = self.config.xpath('InitPortfolio')[0]
        path = util.generate_path(config.attrib['path'], ndate)
        self.output_portfolio_df.to_csv(path, index=True, float_format='%.6f')

if __name__ == '__main__':
    import argparse
    from orca import DATES
    from orca.utils.io import read_frame
    import os
    import shutil
    from lxml import etree

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--alpha', required=True, type=str)
    parser.add_argument('-c', '--config', required=True, type=str)
    parser.add_argument('-d', '--dir', required=True, type=str)
    parser.add_argument('-u', '--univ', required=True, type=str)
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('-f', '--freq', default=1, type=int)
    parser.add_argument('-o', '--offset', default=0, type=int)
    args = parser.parse_args()

    if args.start:
        dates = [date for date in DATES if date >= args.start]
    if args.end:
        dates = [date for date in dates if date <= args.end]
    dates = dates[args.offset::args.freq]

    if not os.path.exists(args.dir):
        os.makedirs(args.dir)
    shutil.copy(args.alpha, args.dir)
    shutil.copy(args.config, args.dir)
    shutil.copy(args.univ, args.dir)

    os.chdir(args.dir)
    alpha, univ = read_frame(args.alpha), read_frame(args.univ)
    optimizer = BarraOptimizer(etree.parse(args.config), alpha, univ, dates)

    for date in dates:
        optimizer.run(date)
