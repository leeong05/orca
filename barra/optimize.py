"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
from string import Template
from datetime import datetime
import argparse
import json

from lxml import etree
import numpy as np
import pandas as pd
import logbook
logbook.set_datetime_format('local')

import barraopt

from orca import DATES
from orca.mongo.quote import QuoteFetcher
quote_fetcher = QuoteFetcher(delay=1)
from orca.mongo.barra import BarraFetcher
barra_fetcher = BarraFetcher('short')
bid_sid = barra_fetcher.fetch_idmaps()
sid_bid = {sid: bid for bid, sid in bid_sid.iteritems()}

STYLES = ['BETA', 'BTOP', 'EARNYILD', 'GROWTH', 'LEVERAGE', 'LIQUIDTY', 'MOMENTUM', 'RESVOL', 'SIZE', 'SIZENL']

class BarraOptimizerBase(object):

    def __init__(self):
        self.logger = logbook.Logger('optimizer')

    @staticmethod
    def parse_bool(text):
        if text in (True, False):
            return text
        if text.lower() in ('true', 'false'):
            return text.lower() == 'true'
        return bool(int(text))

    def parse_config(self, config):
        self.config = etree.parse(config)
        # assets
        assets = self.config.xpath('//Assets')[0]
        self.assets_path = Template(assets.attrib['path'])
        self.group_path = Template(assets.attrib.get('group_path', ''))
        if assets.attrib.get('price', None):
            self.assets_price = assets.attrib['price']
        if assets.attrib.get('price_path', None):
            self.assets_price_path = Template(assets.attrib['price_path'])
        self.assets_lotsize = assets.attrib.get('lotsize', None)
        self.assets_bcost = assets.attrib.get('bcost', None)
        self.assets_scost = assets.attrib.get('scost', None)
        # composite_portfolio
        composite = assets.xpath('Composite')[0]
        self.composites = [cfg.tag for cfg in composite]
        # init_portfolio
        init_portfolio = self.config.xpath('//InitPortfolio')[0]
        self.init_portfolio_path = Template(init_portfolio.attrib['path'])
        # universe
        univ = self.config.xpath('Universe')[0]
        self.universe_path = Template(univ.attrib['path'])
        # risk model
        risk_model = self.config.xpath('RiskModel')[0]
        self.risk_model_path = Template(risk_model.attrib['path'])
        self.risk_model_name = risk_model.attrib['name']

        self.type_map = {
            'Regular': barraopt.eREGULAR,
            'Composite': barraopt.eCOMPOSITE,
            'Cash': barraopt.eCASH,
            }

        self.relative_map = {
            'Absolute': barraopt.eABSOLUTE, 'Abs': barraopt.eABSOLUTE, 'A': barraopt.eABSOLUTE,
            'Multiple': barraopt.eMULTIPLE, 'Mul': barraopt.eMULTIPLE, 'M': barraopt.eMULTIPLE,
            'Plus': barraopt.ePLUS, 'P': barraopt.ePLUS,
            }

        # case
        case = self.config.xpath('//Case')[0]
        try:
            self.booksize = float(case.attrib['booksize'])
            self.cashflow = float(case.attrib.get('cashflow', 0))
        except:
            self.booksize, self.cashflow = 1, 0
        try:
            self.risk_target = float(case.attrib['risk_target'])
        except:
            self.risk_target = None
        # utility
        utility = case.xpath('Utility')[0]
        self.lambda_d = float(utility.attrib.get('lambda_d', 0.0075))
        self.lambda_f = float(utility.attrib.get('lambda_f', 0.0075))
        self.lambda_alpha = float(utility.attrib.get('lambda_alpha', 1))
        self.lambda_cost = float(utility.attrib.get('lambda_cost', 1))
        self.lambda_penalty = float(utility.attrib.get('lambda_penalty', 1))
        self.lambda_residual = float(utility.attrib.get('lambda_residual', 1))

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('config')
        parser.add_argument('-d', '--date', type=str, default=datetime.now().strftime('%Y%m%d'))
        parser.add_argument('-s', '--start', type=str)
        parser.add_argument('-e', '--end', type=str)
        parser.add_argument('-f', '--freq', type=int, default=1)
        args = parser.parse_args()

        self.parse_config(args.config)
        if args.date:
            self.dates = args.date in DATES and [args.date] or []
        if args.start:
            self.dates = [date for date in DATES if date >= args.start]
        if args.end:
            self.dates = [date for date in self.dates if date <= args.end]
        self.dates = self.dates[::args.freq]

    def generate_path(self, template, date, **kwargs):
        return template.substitute(YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8], **kwargs)

    def add_assets(self, date):
        self.workspace.CreateAsset('CASH', barraopt.eCASH)
        path = self.generate_path(self.assets_path, date)
        self.assets = pd.read_csv(path, header=0, dtype={0: str}, index_col=0)
        fclose = quote_fetcher.fetch_daily('fclose', date, offset=1)
        if hasattr(self, 'assets_price'):
            price = quote_fetcher.fetch_daily(self.assets_price, date, offset=1)
        elif hasattr(self, 'assets_price_path'):
            price = pd.read_csv(self.generate_path(self.assets_price_path, date), header=0, dtype={0: str}, index_col=0)
        else:
            price = None
        self.regulars = self.workspace.CreateIDSet()
        for sid, row in self.assets.iterrows():
            asset = self.workspace.CreateAsset(row['bid'], barraopt.eREGULAR)
            self.regulars.Add(row['bid'])
            if 'lotsize' in row.index:
                asset.SetRoundLotSize(int(row['lotsize']))
            elif self.assets_lotsize is not None:
                asset.SetRoundLotSize(int(self.assets_lotsize))
            if 'bcost' in row.index:
                asset.AddPWLinearBuyCost(float(row['bcost']))
            elif self.assets_bcost is not None:
                asset.AddPWLinearBuyCost(float(self.assets_bcost))
            if 'scost' in row.index:
                asset.AddPWLinearSellCost(float(row['scost']))
            elif self.assets_scost is not None:
                asset.AddPWLinearSellCost(float(self.assets_scost))
            if 'alpha' in row.index:
                asset.SetAlpha(float(row['alpha']))
            if 'weight' in row.index:
                asset.SetResidualAlphaWeight(float(row['weight']))
            if price is not None:
                if np.isfinite(price.ix[sid]):
                    asset.SetPrice(price.ix[sid])
                elif np.isfinite(fclose.ix[sid]):
                    asset.SetPrice(fclose.ix[sid])

        self.groups = []
        path = self.generate_path(self.group_path, date)
        if os.path.exists(path):
            self.group_df = pd.read_csv(path, header=0, dtype={0: str})
            self.group_df = pd.read_csv(path, header=0, dtype={i: str for i in range(len(self.group_df.columns))})
            self.group_df.index = self.group_df['sid']
            self.groups = [col for col in self.group_df.columns if col not in ['sid', 'bid']]
            for _, row in self.group_df.iterrows():
                asset = self.workspace.GetAsset(row['bid'])
                if not asset:
                    asset = self.workspace.CreateAsset(row['bid'], barraopt.eREGULAR)
                for group in self.groups:
                    asset.SetGroupAttribute(group, str(row[group]))

        for sid in self.composites:
            self.workspace.CreateAsset(sid, barraopt.eCOMPOSITE)

        self.composite_portfolios, self.composite_portfolio_dfs = {}, {}
        for sid in self.composites:
            path = self.config.xpath('//Assets/Composite/%s' % sid)[0].attrib['path']
            path = self.generate_path(Template(path), date)
            portfolio = self.workspace.CreatePortfolio(sid)
            df = pd.read_csv(path, header=0, dtype={0: str})
            self.composite_portfolio_dfs[sid] = df
            for bid, weight in zip(df['bid'], df['weight']):
                portfolio.AddAsset(bid, float(weight))
                self.regulars.Add(bid)
            self.composite_portfolios[sid] = portfolio
            self.logger.debug('Composite Porfolio: {}({})', sid, len(df))

        self.logger.debug('Assets: Regular({}), Composite({})', len(self.assets), len(self.composites))

    def construct_init_portfolio(self, date):
        path = self.generate_path(self.init_portfolio_path, date)
        self.init_portfolio = self.workspace.CreatePortfolio('init_portfolio')
        if not os.path.exists(path):
            self.logger.warning('No such file exists: {}', path)
            self.init_portfolio.AddAsset('CASH', 1)
            return
        df = pd.read_csv(path, header=0, dtype={0: str})
        for bid, weight in zip(df['bid'], df['weight']):
            self.init_portfolio.AddAsset(bid, float(weight))

        debugs = []
        for key in sorted(self.type_map.keys()):
            w = df.query('type == "%s"' % key)['weight'].sum()
            if abs(w) > 0:
                debugs.append('%s(%.2f)' % (key, w))
        self.logger.debug('Initial Portfolio: ' + ', '.join(debugs))

    def construct_universe(self, date):
        path = self.generate_path(self.universe_path, date)
        self.universe = self.workspace.CreatePortfolio('universe')
        df = pd.read_csv(path, header=0, dtype={0: str})
        for bid in df['bid']:
            self.workspace.CreateAsset(bid, barraopt.eREGULAR)
            self.universe.AddAsset(bid)
        for sid in self.composites:
            self.universe.AddAsset(sid)
        self.logger.debug('Trade Universe: Regular({}), Composite({})', len(df), len(self.composites))

    def define_risk_model(self, date):
        pdate = DATES[DATES.index(date)-1]
        path = self.generate_path(self.risk_model_path, pdate)
        self.risk_model = self.workspace.CreateRiskModel(self.risk_model_name)
        status = self.risk_model.LoadModelsDirectData(str(path), int(pdate), self.regulars)
        assert status == barraopt.eSUCCESS
        self.factors = []
        factors = self.risk_model.GetFactorIDSet()
        for i in range(factors.GetCount()):
            factor = i == 0 and factors.GetFirst() or factors.GetNext()
            self.factors.append(factor)
        self.logger.debug('Load Risk Model Data: {}', pdate)

    def construct_composite_portfolios(self, date):
        bids = self.risk_model.GetAssetIDSet()
        for sid, portfolio in self.composite_portfolios.iteritems():
            for factor in self.factors:
                exposure = self.risk_model.ComputePortExposure(portfolio, factor)
                self.risk_model.SetFactorExposure(sid, factor, exposure)

            variance = self.risk_model.ComputePortSpecificVariance(portfolio)
            self.risk_model.SetSpecificCovariance(sid, sid, variance)

            for i in range(bids.GetCount()):
                bid = i == 0 and bids.GetFirst() or bids.GetNext()
                covariance = self.risk_model.ComputePortAssetSpecificCovariance(portfolio, bid)
                self.risk_model.SetSpecificCovariance(sid, bid, covariance)
            self.logger.debug('FactorExposure, SpecificCovariance for {}', sid)

        if len(self.composite_portfolios) > 1:
            for i, sid1 in enumerate(self.composites):
                for sid2 in self.composites[i+1:]:
                    p1, p2 = self.composite_portfolios[sid1], self.composite_portfolios[sid2]
                    covariance = self.risk_model.ComputePortSpecificCovariance(p1, p2)
                    self.risk_model.SetSpecificCovariance(sid1, sid2, covariance)
                    self.logger.debug('PortSpecificCovariance for {}, {}', sid1, sid2)

    def prepare_case(self, date):
        self.case = self.workspace.CreateCase('optimize', self.init_portfolio, self.universe, self.booksize, self.cashflow/self.booksize)
        if self.risk_target:
            self.case.SetRiskTarget(self.risk_target)
        self.case.SetPrimaryRiskModel(self.workspace.GetRiskModel(self.risk_model_name))
        self.setup_utility(date)
        self.setup_constraints(date)
        if self.config.xpath('//Case/Frontier'):
            self.setup_frontier(date)

    def run_optimization(self, date):
        self.logger.debug('Optimize >>>')
        solver = self.config.xpath('//Solver')[0]
        self.solver = self.workspace.CreateSolver(self.case)
        self.solver.SetOptimalityToleranceMultiplier(float(solver.attrib.get('tolerance_multiplier', 1)))
        self.solver.SetCountCrossoverTradeAsOne(self.parse_bool(solver.attrib.get('crossover_as_one', False)))
        if solver.attrib.get('timeout', None):
            self.solver.SetOption('MAX_OPTIMAL_TIME', float(solver.attrib['timeout']))
        if solver.attrib.get('input', None):
            path = self.generate_path(Template(solver.attrib['input']), date)
            self.solver.WriteInputToFile(self.generate_path(Template(solver.attrib['input']), date))
            self.logger.debug('Write Input File: {}', path)
        self.status = self.solver.Optimize()
        self.status_code = self.status.GetStatusCode()
        try:
            assert self.status_code == barraopt.eOK
            self.logger.debug('Message: {}', self.status.GetMessage())
            self.logger.debug('AdditionalInfo: {}', self.status.GetAdditionalInfo())
        except AssertionError:
            self.logger.warning('Message: {}', self.status.GetMessage())
            self.logger.warning('AdditionalInfo: {}', self.status.GetAdditionalInfo())
            raise
        self.output = self.solver.GetPortfolioOutput()
        self.output_portfolio = self.output.GetPortfolio()
        path = self.generate_path(Template(solver.attrib['output']), date)
        self.output.WriteToFile(path)
        self.logger.debug('Write Output File: {}', path)
        self.final_portfolio = []
        bids = self.output_portfolio.GetAssetIDSet()
        self.output_long_portfolio, self.output_short_portfolio = self.workspace.CreatePortfolio('long'), self.workspace.CreatePortfolio('short')
        for i in range(bids.GetCount()):
            bid = i == 0 and bids.GetFirst() or bids.GetNext()
            if bid == 'CASH':
                continue
            weight = self.output_portfolio.GetAssetWeight(bid)
            if np.abs(weight) > 0:
                if self.workspace.GetAsset(bid).GetType() == barraopt.eCOMPOSITE:
                    self.final_portfolio.append([bid, weight, 'Composite'])
                else:
                    self.final_portfolio.append([bid, weight, 'Regular'])
                if weight > 0:
                    self.output_long_portfolio.AddAsset(bid, weight)
                else:
                    self.output_short_portfolio.AddAsset(bid, -weight)
            else:
                self.output_portfolio.RemoveAsset(bid)
        self.final_portfolio = pd.DataFrame(self.final_portfolio)
        self.final_portfolio.columns = ['bid', 'weight', 'type']
        self.final_portfolio['sid'] = self.final_portfolio['bid'].apply(lambda x: bid_sid.get(x, x))
        self.final_portfolio = self.final_portfolio.reindex(columns=['sid', 'bid', 'weight', 'type'])
        path = self.generate_path(Template(solver.attrib['weight']), date)
        self.final_portfolio.to_csv(path, index=False, float_format='%.6f')
        self.logger.debug('Ideal Weight File: {}', path)

        infos = []
        infos.append('=' * 45)
        infos.append('%-14s:%15.4f%15.4f' % ('Return', self.solver.Evaluate(barraopt.eRETURN), self.solver.Evaluate(barraopt.eRETURN, self.output_portfolio)))
        infos.append('%-14s:%15.4f%15.4f' % ('Factor_Risk', self.solver.Evaluate(barraopt.eFACTOR_RISK), self.solver.Evaluate(barraopt.eFACTOR_RISK, self.output_portfolio)))
        infos.append('%-14s:%15.4f%15.4f' % ('Specific_Risk', self.solver.Evaluate(barraopt.eSPECIFIC_RISK), self.solver.Evaluate(barraopt.eSPECIFIC_RISK, self.output_portfolio)))
        infos.append('%-14s:%15.4f%15.4f' % ('IR', self.solver.Evaluate(barraopt.eINFO_RATIO), self.solver.Evaluate(barraopt.eINFO_RATIO, self.output_portfolio)))
        infos.append('-' * 45)
        infos.append('%-14s:%15.4f' % ('Turnover', self.output.GetTurnover()))
        infos.append('%-14s:%15.4f' % ('Risk', self.output.GetRisk()))
        infos.append('=' * 45)
        self.logger.info('Portfolio Statistics: Long(Regular: {}, Composite: {}), Short(Regular: {}, Composite: {})\n{}',
                len(self.final_portfolio.query('type == "Regular" & weight > 0')),
                len(self.final_portfolio.query('type == "Composite" & weight > 0')),
                len(self.final_portfolio.query('type == "Regular" & weight < 0')),
                len(self.final_portfolio.query('type == "Composite" & weight < 0')),
                '\n'.join(infos))
        self.logger.debug('<<< Optimize')

    def pro_optimization(self, date):
        self.calculate_factor_exposure(date)
        self.calculate_group_exposure(date)

    def calculate_factor_exposure(self, date):
        infos, writes = [], []
        infos.append('=' * 45)
        infos.append('%-15s%10s%10s%10s' % ('Factor', 'Portfolio', 'Long', 'Short'))
        style_factors = [self.risk_model_name+'_'+style for style in STYLES]
        styles, industries = [], []
        styles.append('%19s %5s %19s' % ('-'*19, 'STYLE', '-'*19))
        industries.append('%18s %8s %17s' % ('-'*16, 'INDUSTRY(%)', '-'*16))
        for factor in self.factors:
            exposure = self.risk_model.ComputePortExposure(self.output_portfolio, factor)
            long_exposure = self.risk_model.ComputePortExposure(self.output_long_portfolio, factor)
            short_exposure = self.risk_model.ComputePortExposure(self.output_short_portfolio, factor)
            if factor in style_factors:
                styles.append('%-15s%10.2f%10.2f%10.2f' % (factor, exposure, long_exposure, short_exposure))
            elif factor != self.risk_model_name+'_'+'COUNTRY':
                industries.append('%-15s%10.2f%10.2f%10.2f' % (factor, exposure*100, long_exposure*100, short_exposure*100))
        infos.extend(styles)
        infos.extend(industries)
        infos.append('=' * 45)
        writes.extend(styles[1:])
        writes.extend(industries[1:])
        path = self.config.xpath('//Solver/Exposure')[0].attrib['factor']
        path = self.generate_path(Template(path), date)
        with open(path, 'w') as file:
            file.write('\n'.join(writes))
        self.logger.debug('Factor Exposure: saved in file {}\n{}', path, '\n'.join(infos))

    def calculate_group_exposure(self, date):
        config = self.config.xpath('//Solver/Exposure')[0]
        if config.attrib.get('group', None):
            self.expand_portfolio()
            path = self.generate_path(Template(config.attrib['expanded']), date)
            self.expanded_full_portfolio.to_csv(path, index=False, float_format='%.6f')
            infos, writes = [], []
            infos.append('=' * 45)
            infos.append('%-15s%10s%10s%10s' % ('Attribute', 'Portfolio', 'Long', 'Short'))
            for group in self.groups:
                left = ((45-2-len(group))+1)/2
                right = (45-2-len(group))/2
                format = '%'+str(left)+'s %'+str(len(group))+'s %'+str(right)+'s'
                infos.append(format % ('-'*left, group, '-'*right))
                df = pd.concat([
                            self.expanded_portfolio['weight'].groupby(self.group_df[group]).sum(),
                            self.expanded_long_portfolio['weight'].groupby(self.group_df[group]).sum(),
                            self.expanded_short_portfolio['weight'].groupby(self.group_df[group]).sum(),
                            ], axis=1).fillna(0)
                for attr, row in df.iterrows():
                    infos.append('%-15s%10.2f%10.2f%10.2f' % (attr, row[0]*100, row[1]*100, row[2]*100))
                    writes.append('%-15s%10.2f%10.2f%10.2f' % (attr, row[0]*100, row[1]*100, row[2]*100))
            infos.append('=' * 45)
            path = self.generate_path(Template(config.attrib['group']), date)
            with open(path, 'w') as file:
                file.write('\n'.join(writes))
            self.logger.debug('Group Exposure: saved in file {}\n{}', path, '\n'.join(infos))

    def expand_portfolio(self):
        long = self.final_portfolio.query('type == "Regular" & weight > 0')[['sid', 'bid', 'weight']].copy()
        short = self.final_portfolio.query('type == "Regular" & weight < 0')[['sid', 'bid', 'weight']].copy()
        long.index, short.index = long['sid'], short['sid']
        composites = self.final_portfolio.query('type == "Composite"')
        long_weight, short_weight = long['weight'].copy(), short['weight'].copy()
        for sid, w in zip(composites['sid'], composites['weight']):
            composite = self.composite_portfolio_dfs[sid].copy()
            composite.index = composite['sid']
            composite['weight'] *= w
            if w > 0:
                sids = long_weight.index.union(composite.index)
                long_weight = long_weight.reindex(index=sids).fillna(0) + composite['weight'].reindex(index=sids).fillna(0)
            else:
                sids = short_weight.index.union(composite.index)
                short_weight = short_weight.reindex(index=sids).fillna(0) + composite['weight'].reindex(index=sids).fillna(0)
        sids = short_weight.index.union(long_weight.index)
        weight = short_weight.reindex(index=sids).fillna(0) + long_weight.reindex(index=sids).fillna(0)
        self.expanded_portfolio, self.expanded_long_portfolio, self.expanded_short_portfolio = pd.DataFrame(weight), pd.DataFrame(long_weight), pd.DataFrame(short_weight)
        self.expanded_portfolio['sid'] = self.expanded_portfolio.index
        self.expanded_portfolio['bid'] = self.expanded_portfolio['sid'].map(sid_bid)
        self.expanded_portfolio = self.expanded_portfolio.reindex(columns=['sid', 'bid', 'weight'])
        self.expanded_long_portfolio['sid'] = self.expanded_long_portfolio.index
        self.expanded_long_portfolio['bid'] = self.expanded_long_portfolio['sid'].map(sid_bid)
        self.expanded_long_portfolio = self.expanded_long_portfolio.reindex(columns=['sid', 'bid', 'weight'])
        self.expanded_short_portfolio['sid'] = self.expanded_short_portfolio.index
        self.expanded_short_portfolio['bid'] = self.expanded_short_portfolio['sid'].map(sid_bid)
        self.expanded_short_portfolio = self.expanded_short_portfolio.reindex(columns=['sid', 'bid', 'weight'])
        self.expanded_full_portfolio = pd.concat([self.expanded_long_portfolio, self.expanded_short_portfolio])
        self.expanded_short_portfolio['weight'] *= -1

    def setup_utility(self, date):
        self.logger.debug('Utility >>>')
        self.utility = self.case.InitUtility()
        self.utility.SetPrimaryRiskTerm(None, self.lambda_d, self.lambda_f)
        self.utility.SetAlphaTerm(self.lambda_alpha)
        self.utility.SetTranxCostTerm(self.lambda_cost)
        self.utility.SetPenaltyTerm(self.lambda_penalty)
        self.utility.SetResidualAlphaTerm(self.lambda_residual)
        if self.config.xpath('//Utility/FactorCovTerm'):
            attributes = self.workspace.CreateAttributeSet()
            factors = self.risk_model.GetFactorIDSet()
            config_factors = []
            factor_term = self.config.xpath('//Utility/FactorCovTerm')[0]
            for config in factor_term:
                factor = self.risk_model_name + '_' + config.attrib['value']
                if factors.Contains(factor):
                    attributes.Set(factor, float(config.attrib['weight']))
                    config_factors.append(factor)
            for i in range(factors.GetCount()):
                factor = i == 0 and factors.GetFirst() or factors.GetNext()
                if factor not in config_factors:
                    attributes.Set(factor, float(factor_term.attrib['default']))
            self.utility.AddCovarianceTerm(float(factor_term.attrib['lambda_f']), barraopt.eXWFWX, None, attributes)
            self.logger.debug('... AddCovarianceTerm ...')
        self.logger.debug('<<< Utility')

    def setup_constraints(self, date):
        self.constraints = self.case.InitConstraints()

        if self.config.xpath('//Constraints/TransactionCost'):
            transaction_cost_constraint = self.config.xpath('//Constraints/TransactionCost')[0]
            constraint = self.constraints.SetTransactionCostConstraint()
            constraint.SetUpperBound(float(transaction_cost_constraint.attrib['upper']), barraopt.eABSOLUTE)
            constraint.SetSoft(self.parse_bool(transaction_cost_constraint.attrib['soft']))
            self.logger.debug('... SetTransactionCostConstraint ...')

        if self.config.xpath('//Constraints/ParingConstraints') and self.risk_target is None:
            self.setup_paring_constraints(date)
        if self.config.xpath('//Constraints/HedgeConstraints'):
            self.setup_hedge_constraints(date)
        if self.config.xpath('//Constraints/LinearConstraints'):
            self.setup_linear_constraints(date)
        if self.config.xpath('//Constraints/TurnoverConstraints'):
            self.setup_turnover_constraints(date)
        if self.config.xpath('//Constraints/RiskConstraints'):
            self.setup_risk_constraints(date)

    def setup_frontier(self, date):
        self.logger.debug('Frontier >>>')
        frontier_type_map = {
            'risk_return': barraopt.eRISK_RETURN,
            'return_risk': barraopt.eRETURN_RISK,
            'utility_turnover': barraopt.eUTILITY_TURNOVER,
            'utility_tax': barraopt.eUTILITY_TAX,
            'utility_factor': barraopt.eUTILITY_FACTOR,
            'utility_general_linear': barraopt.eUTILITY_GENERAL_LINEAR,
            'utility_transaction': barraopt.eUTILITY_TRANSACTION,
            }
        bound_type_map = {
            'upper': barraopt.eUPPER_BOUND,
            'lower': barraopt.eLOWER_BOUND,
            }
        frontier = self.config.xpath('//Case/Frontier')[0]
        self.frontier = self.case.InitFrontier(frontier_type_map[frontier.attrib['type']])
        if frontier.attrib.get('id', None):
            self.frontier.SetFrontierConstraintID(str(frontier.attrib['id']))
            self.logger.debug('... SetFrontierConstraintID ...')
        if frontier.attrib.get('include_cost', None):
            self.frontier.SetIncludeTransactionCost(self.parse_bool(frontier.attrib['include_cost']))
            self.logger.debug('... SetIncludeTransactionCost ...')
        if frontier.attrib.get('bound', None):
            self.frontier.SetFrontierBoundType(bound_type_map[frontier.attrib['bound']])
            self.logger.debug('... SetFrontierBoundType ...')
        if frontier.attrib.get('lower', None) and frontier.attrib.get('upper', None):
            self.frontier.SetFrontierRange(float(frontier.attrib['lower']), float(frontier.attrib['upper']))
            self.logger.debug('... SetFrontierRange ...')
        if frontier.attrib.get('max_points', None):
            self.frontier.SetMaxNumDataPoints(int(frontier.attrib['max_points']))
            self.logger.debug('... SetMaxNumDataPoints ...')
        self.logger.debug('<<< Frontier')

    def setup_paring_constraints(self, date):
        self.logger.debug('ParingConstraints >>>')
        paring_constraint = self.config.xpath('//Constraints/ParingConstraints')[0]
        self.paring_constraints = self.constraints.InitParingConstraints()
        holding_map = {
            'Holding': ('SetMinNumAssets', 'SetMaxNumAssets'),
            'LongNum': ('SetMinNumLongs', 'SetMaxNumLongs'),
            'ShortNum': ('SetMinNumShorts', 'SetMaxNumShorts'),
            'LongLevel': ('SetMinHoldingLevel4Long',),
            'ShortLevel': ('SetMinHoldingLevel4Short',),
            }
        trading_map = {
            'Trading': ('SetMinNumTrades', 'SetMaxNumTrades'),
            'BuyNum': ('SetMinNumBuys', 'SetMaxNumBuys'),
            'SellNum': ('SetMinNumSells', 'SetMaxNumSells'),
            'LongNum': ('SetMinNumLongTrades', 'SetMaxNumLongTrades'),
            'ShortNum': ('SetMinNumShortTrades', 'SetMaxNumShortTrades'),
            'LongLevel': ('SetMinTranxLevel4Long',),
            'ShortLevel': ('SetMinTranxLevel4Short',),
            }
        level_paring_type_map = {
            'hLong': barraopt.eMIN_HOLDING_LONG,
            'hShort': barraopt.eMIN_HOLDING_SHORT,
            'tLong': barraopt.eMIN_TRANX_LONG,
            'tShort': barraopt.eMIN_TRANX_SHORT,
            'tBuy': barraopt.eMIN_TRANX_BUY,
            'tSell': barraopt.eMIN_TRANX_SELL,
            }
        trade_paring_type_map = {
            'nAssets': barraopt.eNUM_ASSETS,
            'nLong': barraopt.eNUM_ASSETS_LONG,
            'nShort': barraopt.eNUM_ASSETS_SHORT,
            'nTrades': barraopt.eNUM_TRADES,
            'ntLong': barraopt.eNUM_TRADES_LONG,
            'ntShort': barraopt.eNUM_TRADES_SHORT,
            'nBuy': barraopt.eNUM_BUYS,
            'nSell': barraopt.eNUM_SELLS,
            }
        def set_constraint(config, tag_map):
            if config.tag not in tag_map:
                return
            attrs = tag_map[config.tag]
            if config.attrib.get('min', None):
                getattr(self.paring_constraints, attrs[0])(int(config.attrib['min']), self.parse_bool(config.attrib.get('min_soft', config.attrib.get('soft', False))))
                self.logger.debug('... ' + attrs[0] + ' ...')
            if len(attrs) == 1:
                return
            if config.attrib.get('max', None):
                getattr(self.paring_constraints, attrs[1])(int(config.attrib['max']), self.parse_bool(config.attrib.get('max_soft', config.attrib.get('soft', False))))
                self.logger.debug('... ' + attrs[1] + ' ...')

        self.paring_constraints.SetAllowCloseOut(self.parse_bool(paring_constraint.attrib.get('closeout', True)))
        self.logger.debug('... SetAllowCloseOut ...')
        for config in paring_constraint:
            if config.tag == 'Holding':
                set_constraint(config, holding_map)
                for subconfig in config:
                    set_constraint(subconfig, holding_map)
            elif config.tag == 'Trading':
                set_constraint(config, trading_map)
                for subconfig in config:
                    set_constraint(subconfig, trading_map)
            elif config.tag == 'Asset':
                paring_assets = pd.read_csv(self.generate_path(Template(config.attrib['path']), date), header=0, dtype={0: str})
                paring_assets['paring_type'] = paring_assets['paring_type'].map(level_paring_type_map)
                if not 'soft' in paring_assets.columns:
                    paring_assets['soft'] = False
                paring_assets['soft'] = paring_assets['soft'].fillna(False)
                for _, row in paring_assets.iterrows():
                    self.paring_constraints.AddLevelParingByAsset(row['paring_type'], str(row['bid']), float(row['threshold']), self.parse_bool(row['soft']))
                self.logger.debug('... AddLevelParingByAsset ...')
            elif config.tag == 'Group':
                if config.attrib.get('level_path', None):
                    group_level = pd.read_csv(self.generate_path(Template(config.attrib['level_path']), date), header=0, dtype={0: str})
                    group_level['paring_type'] = group_level['paring_type'].map(level_paring_type_map)
                    if not 'soft' in group_level:
                        group_level['soft'] = False
                    group_level['soft'] = group_level['soft'].fillna(False)
                    for _, row in group_level.iterrows():
                        self.paring_constraints.AddLevelParingByGroup(row['paring_type'], str(row['group']), str(row['attribute']), float(row['threshold']), self.parse_bool(row['soft']))
                    self.logger.debug('... AddLevelParingByGroup ...')
                if config.attrib.get('trade_path', None):
                    group_trade = pd.read_csv(self.generate_path(Template(config.attrib['trade_path']), date), header=0, dtype={0: str})
                    group_trade['paring_type'] = group_trade['paring_type'].map(trade_paring_type_map)
                    if not 'min_soft' in group_trade:
                        group_trade['min_soft'] = False
                    group_trade['min_soft'] = group_trade['min_soft'].fillna(False)
                    if not 'max_soft' in group_trade:
                        group_trade['max_soft'] = False
                    group_trade['max_soft'] = group_trade['max_soft'].fillna(False)
                    for _, row in group_level.iterrows():
                        range = self.paring_constraints.AddAssetTradeParingByGroup(row['paring_type'], str(row['group']), str(row['attribute']))
                        range.SetMin(int(row['min']), self.parse_bool(row['min_soft']))
                        range.SetMax(int(row['max']), self.parse_bool(row['max_soft']))
                    self.logger.debug('... AddAssetTradeParingByGroup ...')

        self.logger.debug('<<< ParingConstraints')

    def setup_hedge_constraints(self, date):
        self.logger.debug('Hedge Constraints >>>')
        self.hedge_constraints = self.constraints.InitHedgeConstraints()
        config = self.config.xpath('//Constraints/HedgeConstraints')[0]
        for hedge_constraint in config:
            if hedge_constraint.tag == 'Leverage':
                self.setup_leverage_range(hedge_constraint)
            elif hedge_constraint.tag == 'LeverageRatio':
                self.setup_leverage_ratio_range(hedge_constraint)
        self.logger.debug('<<< Hedge Constraints')

    def setup_linear_constraints(self, date):
        self.logger.debug('Linear Constraints >>>')
        self.linear_constraints = self.constraints.InitLinearConstraints()
        config = self.config.xpath('//Constraints/LinearConstraints')[0]
        self.linear_constraints.EnableCrossovers(self.parse_bool(config.attrib.get('crossover', True)))
        for linear_constraint in config:
            if linear_constraint.tag == 'AssetRange':
                self.setup_asset_range_constraint(linear_constraint, date)
            elif linear_constraint.tag == 'FactorRange':
                self.setup_factor_range_constraint(linear_constraint, date)
            elif linear_constraint.tag == 'Beta':
                self.setup_beta_constraint(linear_constraint)
            elif linear_constraint.tag == 'Group':
                self.setup_group_constraint(linear_constraint, date)
        self.logger.debug('<<< Linear Constraints')

    def setup_turnover_constraints(self, date):
        self.logger.debug('Turnover Constraints >>>')
        self.turnover_constraints = self.constraints.InitTurnoverConstraints()
        config = self.config.xpath('//Constraints/TurnoverConstraints')[0]
        for turnover_constraint in config:
            if turnover_constraint.tag == 'Long':
                self.setup_long_constraint(turnover_constraint)
            elif turnover_constraint.tag == 'Short':
                self.setup_short_constraint(turnover_constraint)
            elif turnover_constraint.tag == 'Buy':
                self.setup_buy_constraint(turnover_constraint)
            elif turnover_constraint.tag == 'Sell':
                self.setup_sell_constraint(turnover_constraint)
            elif turnover_constraint.tag == 'Net':
                self.setup_net_constraint(turnover_constraint)
        self.logger.debug('<<< Turnover Constraints')

    def setup_risk_constraints(self, date):
        self.logger.debug('Risk Constraints >>>')
        self.risk_contraints = self.constraints.InitRiskConstraints()
        config = self.config.xpath('//Constraints/RiskConstraints')[0]
        for risk_constraint in config:
            if risk_constraint.tag == 'Factor':
                self.setup_factor_constraint(risk_constraint, date)
            elif risk_constraint.tag == 'Specific':
                self.setup_specific_constraint(risk_constraint, date)
            elif risk_constraint.tag == 'Total':
                self.setup_total_constraint(risk_constraint, date)
        self.logger.debug('<<< Risk Constraints')

    def setup_leverage_range(self, config):
        def get_relative(name):
            res = subconfig.attrib.get(name, config.attrib.get(name, subconfig.attrib.get('relative', config.attrib.get('relative', 'Abs')))).capitalize()
            return self.relative_map[res]

        for subconfig in config:
            if subconfig.tag == 'Long':
                constraint = self.hedge_constraints.SetLongSideLeverageRange(self.parse_bool(subconfig.attrib.get('nochange', False)))
                if subconfig.attrib.get('reference', config.attrib.get('reference', None)):
                    constraint.SetReference(subconfig.attrib.get('reference', config.attrib['reference']))
                constraint.SetSoft(self.parse_bool(subconfig.attrib.get('soft', config.attrib.get('soft', False))))
                constraint.SetUpperBound(float(subconfig.attrib['upper']), get_relative('u_relative'))
                constraint.SetLowerBound(float(subconfig.attrib['lower']), get_relative('l_relative'))
                self.logger.debug('... SetLongSideLeverageRange ...')
            elif subconfig.tag == 'Short':
                constraint = self.hedge_constraints.SetShortSideLeverageRange(self.parse_bool(subconfig.attrib.get('nochange', False)))
                if subconfig.attrib.get('reference', config.attrib.get('reference', None)):
                    constraint.SetReference(subconfig.attrib.get('reference', config.attrib['reference']))
                constraint.SetSoft(self.parse_bool(subconfig.attrib.get('soft', config.attrib.get('soft', False))))
                constraint.SetUpperBound(float(subconfig.attrib['upper']), get_relative('u_relative'))
                constraint.SetLowerBound(float(subconfig.attrib['lower']), get_relative('l_relative'))
                self.logger.debug('... SetShortSideLeverageRange ...')
            elif subconfig.tag == 'Total':
                constraint = self.hedge_constraints.SetTotalLeverageRange(self.parse_bool(subconfig.attrib.get('nochange', False)))
                constraint.SetSoft(self.parse_bool(subconfig.attrib.get('soft', config.attrib.get('soft', False))))
                constraint.SetUpperBound(float(subconfig.attrib['upper']), get_relative('u_relative'))
                constraint.SetLowerBound(float(subconfig.attrib['lower']), get_relative('l_relative'))
                self.logger.debug('... SetTotalLeverageRange ...')

    def setup_asset_range_constraint(self, config, date):
        constraint = self.linear_constraints.SetAssetRange('CASH')
        constraint.SetUpperBound(1, barraopt.eABSOLUTE)
        constraint.SetLowerBound(1, barraopt.eABSOLUTE)
        path = self.generate_path(Template(config.attrib['path']), date)
        white = self.generate_path(Template(config.attrib['white']), date)
        black = self.generate_path(Template(config.attrib['black']), date)
        file_assets = []
        if os.path.exists(path):
            asset_range = pd.read_csv(path, header=0, dtype={0: str})
            asset_range['relative'] = asset_range['relative'].map(self.relative_map)
            if 'u_relative' in asset_range.columns:
                asset_range['u_relative'] = asset_range['u_relative'].map(self.relative_map)
            if 'l_relative' in asset_range.columns:
                asset_range['l_relative'] = asset_range['l_relative'].map(self.relative_map)

            for _, row in asset_range.iterrows():
                constraint = self.linear_constraints.SetAssetRange(row['bid'])
                file_assets.append(row['bid'])
                if 'u_relative' in row.index and row.notnull()['u_relative']:
                    constraint.SetUpperBound(float(row['upper']), row['u_relative'])
                else:
                    constraint.SetUpperBound(float(row['upper']), row['relative'])
                if 'l_relative' in row.index and row.notnull()['l_relative']:
                    constraint.SetLowerBound(float(row['lower']), row['l_relative'])
                else:
                    constraint.SetLowerBound(float(row['lower']), row['relative'])
        def get_relative(name, config):
            return self.relative_map[config.attrib.get(name, config.attrib.get('relative', 'Abs')).capitalize()]
        regular = config.xpath('Regular')[0]
        composites = config.xpath('Composite')[0]
        if os.path.exists(white):
            whites = json.load(open(white))
            for sid in whites:
                if sid not in self.assets.index:
                    continue
                bid = self.assets['bid'].ix[sid]
                constraint = self.linear_constraints.SetAssetRange(bid)
                file_assets.append(bid)
                w = self.booksize/(self.booksize+self.cashflow*1.0)
                constraint.SetLowerBound(self.assets['weight'].ix[sid]*w, barraopt.eABSOLUTE)
                constraint.SetUpperBound(float(regular.attrib['upper']), get_relative('u_relative', regular))
        if os.path.exists(black):
            blacks = json.load(open(black))
            for sid in blacks:
                if sid not in self.assets.index:
                    continue
                bid = self.assets['bid'].ix[sid]
                constraint = self.linear_constraints.SetAssetRange(bid)
                file_assets.append(bid)
                constraint.SetUpperBound(0, barraopt.eABSOLUTE)
                constraint.SetLowerBound(0, barraopt.eABSOLUTE)
        bids = self.workspace.GetAssetIDSet()
        for i in range(bids.GetCount()):
            bid = i == 0 and bids.GetFirst() or bids.GetNext()
            if bid in file_assets:
                continue
            if self.workspace.GetAsset(bid).GetType() == barraopt.eREGULAR:
                constraint = self.linear_constraints.SetAssetRange(bid)
                constraint.SetUpperBound(float(regular.attrib['upper']), get_relative('u_relative', regular))
                constraint.SetLowerBound(float(regular.attrib['lower']), get_relative('l_relative', regular))
            elif self.workspace.GetAsset(bid).GetType() == barraopt.eCOMPOSITE and composites.xpath(bid):
                composite = composites.xpath(bid)[0]
                constraint = self.linear_constraints.SetAssetRange(bid)
                constraint.SetUpperBound(float(composite.attrib['upper']),
                        self.relative_map[composite.attrib.get('u_relative', composites.attrib.get('u_relative', composite.attrib.get('relative', composites.attrib.get('relative', 'Abs')))).capitalize()])
                constraint.SetLowerBound(float(composite.attrib['lower']),
                        self.relative_map[composite.attrib.get('l_relative', composites.attrib.get('l_relative', composite.attrib.get('relative', composites.attrib.get('relative', 'Abs')))).capitalize()])
        self.logger.debug('... SetAssetRange ...')

    def setup_factor_range_constraint(self, config, date):
        path = self.generate_path(Template(config.attrib['path']), date) if config.attrib.get('path', '') else ''
        file_factors = []
        if os.path.exists(path):
            factor_range = pd.read_csv(path, header=0)
            factor_range['relative'] = factor_range['relative'].map(self.relative_map)
            if 'u_relative' in factor_range.columns:
                factor_range['u_relative'] = factor_range['u_relative'].map(self.relative_map)
            if 'l_relative' in factor_range.columns:
                factor_range['l_relative'] = factor_range['l_relative'].map(self.relative_map)
            if 'soft' in factor_range.columns:
                factor_range['soft'] = factor_range['soft'].fillna(False).astype(bool)
            for _, row in factor_range.iterrows():
                constraint = self.linear_constraints.SetFactorRange(row['factor'])
                file_factors.append(row['factor'])
                if 'u_relative' in row.index and row.notnull()['u_relative']:
                    constraint.SetUpperBound(float(row['upper']), row['u_relative'])
                else:
                    constraint.SetUpperBound(float(row['upper']), row['relative'])
                if 'l_relative' in row.index and row.notnull()['l_relative']:
                    constraint.SetLowerBound(float(row['lower']), row['l_relative'])
                else:
                    constraint.SetLowerBound(float(row['lower']), row['relative'])
                if 'soft' in row.index:
                    constraint.SetSoft(row['soft'])
        for factor in config:
            factor_name = self.risk_model_name+'_'+factor.attrib['value']
            if factor_name in file_factors:
                continue
            constraint = self.linear_constraints.SetFactorRange(factor_name)
            if factor.attrib.get('reference', config.attrib.get('reference', None)):
                constraint.SetReference(self.composite_portfolios[config.attrib['reference']])
            constraint.SetUpperBound(float(factor.attrib['upper']),
                    self.relative_map[factor.attrib.get('u_relative', config.attrib.get('u_relative', factor.attrib.get('relative', config.attrib['relative']))).capitalize()])
            constraint.SetLowerBound(float(factor.attrib['upper']),
                    self.relative_map[factor.attrib.get('l_relative', config.attrib.get('l_relative', factor.attrib.get('relative', config.attrib['relative']))).capitalize()])
            constraint.SetSoft(self.parse_bool(factor.attrib.get('soft', config.attrib.get('soft', False))))
        self.logger.debug('... SetFactorRange ...')

    def setup_beta_constraint(self, config):
        constraint = self.linear_constraints.SetBetaConstraint()
        constraint.SetUpperBound(float(config.attrib['upper']))
        constraint.SetLowerBound(float(config.attrib['lower']))
        constraint.SetSoft(self.parse_bool(config.attrib.get('soft', False)))

    def setup_group_constraint(self, config, date):
        path = self.generate_path(Template(config.attrib['path']), date) if config.attrib.get('path', '') else ''
        file_groups = []
        if os.path.exists(path):
            group = pd.read_csv(path, header=0)
            if 'soft' in group.columns:
                group['soft'] = group['soft'].fillna(False).astype(bool)
            for _, row in group.iterrows():
                constraint = self.linear_constraints.AddGroupConstraint(row['group'], row['attribute'])
                file_groups.append((row['group'], row['attribute']))
                if row.notnull()['reference']:
                    constraint.SetReference(self.composite_portfolios[row['reference']])
                if 'u_relative' in row.index and row.notnull()['u_relative']:
                    constraint.SetUpperBound(float(row['upper']), row['u_relative'])
                else:
                    constraint.SetUpperBound(float(row['upper']), row['relative'])
                if 'l_relative' in row.index and row.notnull()['l_relative']:
                    constraint.SetLowerBound(float(row['lower']), row['l_relative'])
                else:
                    constraint.SetLowerBound(float(row['lower']), row['relative'])
                if 'soft' in row.index:
                    constraint.SetSoft(row['soft'])
        for group in self.groups:
            if not config.xpath(group.capitalize()):
                continue
            gconfig = config.xpath(group.capitalize())[0]
            gattribs = set()
            for attrib in gconfig:
                gattribs.add(attrib.attrib['value'])
                if (group, attrib.attrib['value']) in file_groups:
                    continue
                constraint = self.linear_constraints.AddGroupConstraint(group, attrib.attrib['value'])
                if attrib.attrib.get('reference', gconfig.attrib.get('reference', None)):
                    constraint.SetReference(self.composite_portfolios[attrib.attrib.get('reference', gconfig.attrib['reference'])])
                constraint.SetUpperBound(float(attrib.attrib['upper']),
                        self.relative_map[attrib.attrib.get('u_relative', gconfig.attrib.get('u_relative', attrib.attrib.get('relative', gconfig.attrib['relative']))).capitalize()])
                constraint.SetLowerBound(float(attrib.attrib['lower']),
                        self.relative_map[attrib.attrib.get('l_relative', gconfig.attrib.get('l_relative', attrib.attrib.get('relative', gconfig.attrib['relative']))).capitalize()])
                constraint.SetSoft(self.parse_bool(attrib.attrib.get('soft', gconfig.attrib.get('soft', False))))
                self.logger.debug('AddGroupConstraint for {}@{}', group, attrib.attrib['value'])
            attribs = self.group_df[group].unique()
            if gconfig.attrib.get('lower', None) and gconfig.attrib.get('upper', None):
                for attrib in attribs:
                    if attrib not in gattribs:
                        constraint = self.linear_constraints.AddGroupConstraint(group, str(attrib))
                        constraint.SetUpperBound(float(gconfig.attrib['upper']),
                                self.relative_map[gconfig.attrib.get('u_relative', gconfig.attrib['relative']).capitalize()])
                        constraint.SetLowerBound(float(gconfig.attrib['lower']),
                                self.relative_map[gconfig.attrib.get('l_relative', gconfig.attrib['relative']).capitalize()])
                        constraint.SetSoft(self.parse_bool(gconfig.attrib.get('soft', False)))
                        self.logger.debug('AddGroupConstraint for {}@{}', group, attrib)
        self.logger.debug('... AddGroupConstraint ...')

    def setup_turnover_constraint(self, config, category):
        category_map = {
            'long': 'SetLongSideConstraint',
            'short': 'SetShortSideConstraint',
            'buy': 'SetBuySideConstraint',
            'sell': 'SetSellSideConstraint',
            'net': 'SetNetConstraint',
            }
        constraint = getattr(self.turnover_constraints, category_map[category])()
        constraint.SetUpperBound(float(config.attrib['upper']))
        constraint.SetSoft(self.parse_bool(config.attrib.get('soft', False)))
        self.logger.debug('... ' + category_map[category] + ' ...')

    def setup_long_constraint(self, config):
        self.setup_turnover_constraint(config, 'long')

    def setup_short_constraint(self, config):
        self.setup_turnover_constraint(config, 'short')

    def setup_buy_constraint(self, config):
        self.setup_turnover_constraint(config, 'buy')

    def setup_sell_constraint(self, config):
        self.setup_turnover_constraint(config, 'sell')

    def setup_net_constraint(self, config):
        self.setup_turnover_constraint(config, 'net')

    def setup_factor_constraint(self, config, date):
        for subconfig in config:
            id_path = self.generate_path(Template(subconfig.attrib['id_path']), date)
            ids = pd.read_csv(id_path, header=0, dtype={0: str})
            factor_path = self.generate_path(Template(subconfig.attrib['factor_path']), date)
            factors_ = pd.read_csv(factor_path, header=0, dtype={0: str})
            bids, factors = self.workspace.CreateIDSet(), self.workspace.CreateIDSet()
            for _, row in ids.iterrows():
                bids.Add(str(row['bid']))
            for _, row in factors_.iterrows():
                factors.Add(str(row['factor']))

            benchmark = subconfig.attrib.get('benchmark', None)
            if benchmark and benchmark in self.composite_portfolios:
                benchmark = self.composite_portfolios[benchmark]
            else:
                benchmark = None
            relative = self.parse_bool(subconfig.attrib.get('relative', False))
            soft = self.parse_bool(subconfig.attrib.get('soft', False))
            constraint = self.risk_contraints.AddFactorConstraint(bids, factors, True, benchmark, relative, False, soft)
            constraint.SetUpperBound(float(subconfig.attrib['upper']))
        self.logger.debug('... AddFactorConstraint ...')

    def setup_specific_constraint(self, config, date):
        for subconfig in config:
            id_path = self.generate_path(Template(subconfig.attrib['id_path']), date)
            ids = pd.read_csv(id_path, header=0, dtype={0: str})
            bids = self.workspace.CreateIDSet()
            for _, row in ids.iterrows():
                bids.Add(str(row['bid']))
            benchmark = subconfig.attrib.get('benchmark', None)
            if benchmark in self.composite_portfolios:
                benchmark = self.composite_portfolios[benchmark]
            else:
                benchmark = None
            relative = self.parse_bool(subconfig.attrib.get('relative', False))
            soft = self.parse_bool(subconfig.attrib.get('soft', False))
            constraint = self.risk_contraints.AddSpecificConstraint(bids, True, benchmark, relative, False, soft)
            constraint.SetUpperBound(float(subconfig.attrib['upper']))
        self.logger.debug('... AddSpecificConstraint ...')

    def setup_total_constraint(self, config, date):
        for subconfig in config:
            id_path = self.generate_path(Template(subconfig.attrib['id_path']), date)
            ids = pd.read_csv(id_path, header=0, dtype={0: str})
            factor_path = self.generate_path(Template(subconfig.attrib['factor_path']), date)
            factors_ = pd.read_csv(factor_path, header=0, dtype={0: str})
            bids, factors = self.workspace.CreateIDSet(), self.workspace.CreateIDSet()
            for _, row in ids.iterrows():
                bids.Add(str(row['bid']))
            for _, row in factors_.iterrows():
                factors.Add(str(row['factor']))
            benchmark = subconfig.attrib.get('benchmark', None)
            if benchmark in self.composite_portfolios:
                benchmark = self.composite_portfolios[benchmark]
            else:
                benchmark = None
            relative = self.parse_bool(subconfig.attrib.get('relative', False))
            soft = self.parse_bool(subconfig.attrib.get('soft', False))
            constraint = self.risk_contraints.AddTotalConstraint(bids, factors, True, benchmark, relative, False, soft)
            constraint.SetUpperBound(float(subconfig.attrib['upper']))
        self.logger.debug('... AddTotalConstraint ...')

    def run(self):
        self.parse_args()
        self.workspace = barraopt.CWorkSpace.CreateInstance()
        for date in self.dates:
            self.add_assets(date)
            self.construct_init_portfolio(date)
            self.construct_universe(date)
            self.define_risk_model(date)
            self.construct_composite_portfolios(date)
            self.prepare_case(date)
            self.run_optimization(date)
            self.pro_optimization(date)
            self.workspace.Release()


if __name__ == '__main__':

    optimizer = BarraOptimizerBase()
    optimizer.run()
