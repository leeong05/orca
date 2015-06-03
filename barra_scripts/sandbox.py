"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca.barra.base import BarraOptimizerBase
from orca.barra import util


class BarraOptimizer(BarraOptimizerBase):

    def __init__(self, config, debug_on, alpha, univ, dates):
        super(BarraOptimizer, self).__init__(config, debug_on=debug_on)
        self.alpha = alpha
        self.univ = univ
        self.dates = dates
        self.positions = {}
        self.returns_ser, self.ir_ser = {}, {}
        self.factor_risk_ser, self.specific_risk_ser = {}, {}
        self.turnover_ser, self.risk_ser = {}, {}

    def before(self, date):
        alpha, univ = self.alpha.ix[date], self.univ.ix[date].astype(bool)

        alpha = pd.DataFrame({'alpha': alpha}).dropna()
        alpha['sid'] = alpha.index
        alpha['bid'] = alpha['sid'].map(self.sid_bid)
        alpha = alpha.reindex(columns=['sid', 'bid', 'alpha'])
        alpha = alpha.dropna()
        config = self.config.xpath('Assets')[0]
        path = util.generate_path(config.attrib['path'], date)
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        alpha.to_csv(path, index=False, float_format='%.6f')

        univ = pd.DataFrame({'sid': univ.ix[univ].index})
        univ['bid'] = univ['sid'].map(self.sid_bid)
        univ = univ.dropna()
        config = self.config.xpath('Universe')[0]
        path = util.generate_path(config.attrib['path'], date)
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        univ.to_csv(path, index=False)

    def after(self, date):
        self.returns_ser[date], self.ir_ser[date] = self.returns, self.ir
        self.factor_risk_ser[date], self.specific_risk_ser[date] = self.factor_risk, self.specific_risk
        self.turnover_ser[date], self.risk_ser[date] = self.turnover, self.risk
        if date == self.dates[-1]:
            self.returns_ser, self.ir_ser = pd.Series(self.returns_ser), pd.Series(self.ir_ser)
            self.factor_risk_ser, self.specific_risk_ser = pd.Series(self.factor_risk_ser), pd.Series(self.specific_risk_ser)
            self.turnover_ser, self.risk_ser = pd.Series(self.turnover_ser), pd.Series(self.risk_ser)
            df = pd.concat([self.returns_ser, self.ir_ser, 
                    self.factor_risk_ser, self.specific_risk_ser,
                    self.turnover_ser, self.risk_ser], axis=1)
            df.columns = ['returns', 'ir', 'factor_risk', 'specific_risk', 'turnover', 'risk']
            df.index = pd.to_datetime(df.index)
            df.to_csv('metrics', index=True, float_format='%.4f')

        self.positions[date] = self.output_portfolio_df['weight']
        if date == self.dates[-1]:
            self.positions = pd.DataFrame(self.positions)
            self.positions.to_csv('positions', index=True, float_format='%.6f')
            return
        ndate = self.dates[self.dates.index(date)+1]
        config = self.config.xpath('InitPortfolio')[0]
        path = util.generate_path(config.attrib['path'], ndate)
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
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
    parser.add_argument('--debug_on', action='store_true')
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
    optimizer = BarraOptimizer(etree.parse(args.config), args.debug_on, alpha, univ, dates)

    for date in dates:
        optimizer.run(date)
