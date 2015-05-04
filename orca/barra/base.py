"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
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
from orca.barra import str2enum
from orca.barra import util

STYLE_FACTORS = ['BETA', 'BTOP', 'EARNYILD', 'GROWTH', 'LEVERAGE', 'LIQUIDTY', 'MOMENTUM', 'RESVOL', 'SIZE', 'SIZENL']

class BarraOptimizerBase(object):

    def __init__(self, config):
        self.logger = logbook.Logger('optimizer')
        self.config = etree.parse(config)

    def to_portfolio(self, df, name):
        portfolio = self.workspace.CreatePortfolio(name)
        for bid, weight in zip(df['bid'], df['weight']):
            portfolio.AddAsset(bid, float(weight))
        return portfolio

    def to_idset(self, seq):
        idset = self.workspace.CreateIDSet()
        for e in seq:
            idset.Add(e)
        return idset

    def before(self, date):
        pass

    def initialize(self, date):
        prev_date = DATES[DATES.index(date)-1]
        self.workspace = barraopt.CWorkSpace.CreateInstance()
        self.regular_bids = set()
        self.bid_sid = barra_fetcher.fetch_idmaps(date=prev_date)
        self.sid_bid = {sid: bid for bid, sid in self.bid_sid.iteritems()}

    def add_assets(self, date):
        # add cash asset
        self.workspace.CreateAsset('CASH', barraopt.eCASH)
        # add asset from file: alpha, bcost, scost, weight
        config = self.config.xpath('Assets')[0]
        path = util.generate_path(config.attrib['path'], date)
        self.assets_df = util.read_csv(path, header=0, dtype={0: str})
        for sid, row in self.assets_df.iterrows():
            asset = self.workspace.CreateAsset(row['bid'], barraopt.eREGULAR)
            row_not_null = row.notnull()
            self.regular_bids.add(row['bid'])
            if 'alpha' in row.index and row_not_null['alpha']:
                util.set_asset_attribute(asset.SetAlpha, row['alpha'], float, np.isfinite)
            if 'weight' in row.index and row_not_null['weight']:
                util.set_asset_attribute(asset.SetResidualAlphaWeight, row['weight'], float, np.isfinite)
            if 'bcost' in row.index and row_not_null['bcost']:
                util.set_asset_attribute(asset.AddPWLinearBuyCost, row['bcost'], float, np.isfinite)
            elif config.attrib.get('bcost', None):
                util.set_asset_attribute(asset.AddPWLinearBuyCost, config.attrib['bcost'], float, np.isfinite)
            if 'scost' in row.index and row_not_null['scost']:
                util.set_asset_attribute(asset.AddPWLinearSellCost, row['scost'], float, np.isfinite)
            elif config.attrib.get('scost', None):
                util.set_asset_attribute(asset.AddPWLinearBuyCost, config.attrib['scost'], float, np.isfinite)
        # add group/attribute for assets
        self.group_names = []
        path = util.generate_path(config.attrib['group_path'], date)
        if os.path.exists(path):
            self.groups_df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
            self.group_names = [col for col in self.groups_df.columns if col not in ['sid', 'bid']]
            for group in self.group_names:
                self.groups_df[group] = self.groups_df[group].astype(str)
            for _, row in self.groups_df.iterrows():
                asset = self.workspace.GetAsset(row['bid'])
                if not asset:
                    asset = self.workspace.CreateAsset(row['bid'], barraopt.eREGULAR)
                row_not_null = row.notnull()
                for group in self.group_names:
                    if row_not_null[group]:
                        util.set_asset_attribute(asset.SetGroupAttribute, row[group], str)
        # add composites
        self.composite_bids = []
        self.composite_portfolios, self.composite_dfs = {}, {}
        if config.xpath('Composite'):
            composite_config = config.xpath('Composite')[0]
            for elem in composite_config:
                bid = elem.tag
                self.workspace.CreateAsset(bid, barraopt.eCOMPOSITE)
                self.composite_bids.append(bid)

                path = util.generate_path(elem.attrib['path'], date)
                df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
                self.composite_dfs[bid] = df

                portfolio = self.workspace.CreatePortfolio(bid)
                for sbid, weight in zip(df['bid'], df['weight']):
                    portfolio.AddAsset(sbid, float(weight))
                    self.regular_bids.add(sbid)
                self.composite_portfolios[bid] = portfolio
        self.logger.debug('Assets: Regular({}), Composite({})', len(self.assets_df), len(self.composite_bids))

    def construct_init_portfolio(self, date):
        config = self.config.xpath('InitPortfolio')[0]
        path = util.generate_path(config.attrib['path'], date)
        self.init_portfolio = self.workspace.CreatePortfolio('init_portfolio')
        self.init_portfolio.AddAsset('CASH', 1)
        if not os.path.exists(path):
            # add CASH asset
            self.logger.warning('No such file exists: {}', path)
            self.init_portfolio_df = pd.DataFrame(columns=['bid', 'type', 'weight'])
            self.init_portfolio_df.index.name = 'sid'
            return
        self.init_portfolio_df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
        for _, row in self.init_portfolio_df.iterrows():
            self.workspace.CreateAsset(row['bid'], str2enum[row['type']])
            self.init_portfolio.AddAsset(row['bid'], float(row['weight']))
            if row['type'] == 'eREGULAR':
                self.regular_bids.add(row['bid'])

        regular = self.init_portfolio_df.query('type == "eREGULAR"')
        composite = self.init_portfolio_df.query('type == "eCOMPOSITE"')
        self.logger.debug('Initial Portfolio: Regular({:.2f}, {:.2f}), Composite({:.2f}, {:.2f})',
                regular.query('weight > 0')['weight'].sum(),
                regular.query('weight < 0')['weight'].sum(),
                composite.query('weight > 0')['weight'].sum(),
                composite.query('weight < 0')['weight'].sum()
                )

    def construct_universe(self, date):
        config = self.config.xpath('Universe')[0]
        alpha_only = config.attrib.get('alpha_only', False)
        path = util.generate_path(config.attrib['path'], date)
        self.universe = self.workspace.CreatePortfolio('universe')
        self.universe_df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
        self.universe_bids = set()
        for bid in self.universe_df['bid']:
            self.workspace.CreateAsset(bid, barraopt.eREGULAR)
            if not alpha_only or bid in self.assets_df['bid']:
                self.universe.AddAsset(bid)
                self.regular_bids.add(bid)
                self.universe_bids.add(bid)
        # do not forget to add composites into trading universe
        for bid in self.composite_bids:
            self.universe.AddAsset(bid)
        self.logger.debug('Trade Universe: Regular({}), Composite({})', len(self.universe_bids), len(self.composite_bids))

    def define_risk_model(self, date):
        config = self.config.xpath('RiskModel')[0]
        prev_date = DATES[DATES.index(date)-1]
        path = util.generate_path(config.attrib['path'], prev_date)
        self.risk_model_name = config.attrib['name']
        self.risk_model = self.workspace.CreateRiskModel(self.risk_model_name)
        status = self.risk_model.LoadModelsDirectData(str(path), int(prev_date), self.to_idset(self.regular_bids))
        assert status == barraopt.eSUCCESS
        self.risk_factors = []
        factors = self.risk_model.GetFactorIDSet()
        for i in range(factors.GetCount()):
            factor = i == 0 and factors.GetFirst() or factors.GetNext()
            self.risk_factors.append(factor)
        self.risk_bids = []
        bids = self.risk_model.GetAssetIDSet()
        for i in range(bids.GetCount()):
            bid = i == 0 and bids.GetFirst() or bids.GetNext()
            self.risk_bids.append(bid)
        self.logger.debug('Load Risk Model Data: {}', prev_date)

    def construct_composite_portfolios(self, date):
        for composite_bid, portfolio in self.composite_portfolios.iteritems():
            for factor in self.risk_factors:
                exposure = self.risk_model.ComputePortExposure(portfolio, factor)
                self.risk_model.SetFactorExposure(composite_bid, factor, exposure)

            variance = self.risk_model.ComputePortSpecificVariance(portfolio)
            self.risk_model.SetSpecificCovariance(composite_bid, composite_bid, variance)

            for bid in self.risk_bids:
                covariance = self.risk_model.ComputePortAssetSpecificCovariance(portfolio, bid)
                self.risk_model.SetSpecificCovariance(composite_bid, bid, covariance)
            self.logger.debug('FactorExposure, SpecificCovariance for {}', composite_bid)

        if len(self.composite_portfolios) > 1:
            for i, bid1 in enumerate(self.composite_bids):
                for bid2 in self.composite_bids[i+1:]:
                    p1, p2 = self.composite_portfolios[bid1], self.composite_portfolios[bid2]
                    covariance = self.risk_model.ComputePortSpecificCovariance(p1, p2)
                    self.risk_model.SetSpecificCovariance(bid1, bid2, covariance)
                    self.logger.debug('PortSpecificCovariance for {}, {}', bid1, bid2)
        self.logger.debug('Construct Composite Portfolio: {}', ', '.join(self.composite_bids))

    def prepare_case(self, date):
        config = self.config.xpath('Case')[0]
        try:
            self.booksize = float(config.attrib.get('booksize', 1))
        except:
            self.booksize = 1
        try:
            self.cashflow = float(config.attrib.get('cashflow', 0))
        except:
            self.cashflow = 0
        self.case = self.workspace.CreateCase('optimize', self.init_portfolio, self.universe, self.booksize, 0)
        try:
            self.risk_target = float(config.attrib['risk_target'])
        except:
            self.risk_target = None
        if self.risk_target is not None and self.risk_target > 0:
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
        self.solver.SetCountCrossoverTradeAsOne(util.parse_bool(solver.attrib.get('crossover_as_one', False)))
        if solver.attrib.get('timeout', None):
            self.solver.SetOption('MAX_OPTIMAL_TIME', float(solver.attrib['timeout']))
        if solver.attrib.get('input', None):
            path = util.generate_path(solver.attrib['input'], date)
            self.solver.WriteInputToFile(path)
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
        path = util.generate_path(solver.attrib['output'], date)
        self.output.WriteToFile(path)
        self.logger.debug('Write Output File: {}', path)

        self.output_portfolio_df = []
        bids = self.output_portfolio.GetAssetIDSet()
        for i in range(bids.GetCount()):
            bid = i == 0 and bids.GetFirst() or bids.GetNext()
            weight = self.output_portfolio.GetAssetWeight(bid)
            if bid == 'CASH':
                continue
            if np.abs(weight) > 0:
                if bid in self.composite_bids:
                    self.output_portfolio_df.append([bid, weight, 'eCOMPOSITE'])
                else:
                    self.output_portfolio_df.append([bid, weight, 'eREGULAR'])
        self.output_portfolio_df = pd.DataFrame(self.output_portfolio_df)
        self.output_portfolio_df.columns = ['bid', 'weight', 'type']
        self.output_portfolio_df.index = self.output_portfolio_df['bid'].apply(lambda x: self.bid_sid.get(x, x))
        self.output_portfolio_df.index.name = 'sid'
        path = util.generate_path(solver.attrib['weight'], date)
        self.output_portfolio_df.to_csv(path, index=True, float_format='%.6f')
        self.logger.debug('Ideal Weight File: {}', path)

        infos = []
        infos.append('=' * 45)
        infos.append('%-14s:%15.4f%15.4f' % ('Return', self.solver.Evaluate(barraopt.eRETURN), self.solver.Evaluate(barraopt.eRETURN, self.output_portfolio)))
        infos.append('%-14s:%15.4f%15.4f' % ('Factor_Risk', self.solver.Evaluate(barraopt.eFACTOR_RISK), self.solver.Evaluate(barraopt.eFACTOR_RISK, self.output_portfolio)))
        infos.append('%-14s:%15.4f%15.4f' % ('Specific_Risk', self.solver.Evaluate(barraopt.eSPECIFIC_RISK), self.solver.Evaluate(barraopt.eSPECIFIC_RISK, self.output_portfolio)))
        init_ir = self.solver.Evaluate(barraopt.eINFO_RATIO)
        infos.append('%-14s:%15.4f%15.4f' % ('IR', init_ir if np.abs(init_ir) < 100 else np.nan, self.solver.Evaluate(barraopt.eINFO_RATIO, self.output_portfolio)))
        infos.append('-' * 45)
        infos.append('%-14s:%15.4f' % ('Turnover', self.output.GetTurnover()))
        infos.append('%-14s:%15.4f' % ('Risk', self.output.GetRisk()))
        infos.append('=' * 45)
        self.logger.info('Portfolio Statistics: Long(Regular: {}, Composite: {}), Short(Regular: {}, Composite: {})\n{}',
                len(self.output_portfolio_df.query('type == "eREGULAR" & weight > 0')),
                len(self.output_portfolio_df.query('type == "eCOMPOSITE" & weight > 0')),
                len(self.output_portfolio_df.query('type == "eREGULAR" & weight < 0')),
                len(self.output_portfolio_df.query('type == "eCOMPOSITE" & weight < 0')),
                '\n'.join(infos))
        self.logger.debug('<<< Optimize')
        self.generate_report(date)

    def after(self, date):
        pass

#-----------

    def setup_utility(self, date):
        self.logger.debug('Utility >>>')
        config = self.config.xpath('//Utility')[0]
        self.lambda_d = float(config.attrib['lambda_d'])
        self.lambda_f = float(config.attrib['lambda_f'])
        self.lambda_alpha = float(config.attrib.get('lambda_alpha', 1))
        self.lambda_cost = float(config.attrib.get('lambda_cost', 1))
        self.lambda_penalty = float(config.attrib.get('lambda_penalty', 1))
        self.lambda_residual = float(config.attrib.get('lambda_residual', 1))

        self.utility = self.case.InitUtility()
        self.utility.SetPrimaryRiskTerm(None, self.lambda_d, self.lambda_f)
        self.utility.SetAlphaTerm(self.lambda_alpha)
        self.utility.SetTranxCostTerm(self.lambda_cost)
        self.utility.SetPenaltyTerm(self.lambda_penalty)
        self.utility.SetResidualAlphaTerm(self.lambda_residual)
        if config.xpath('FactorCovTerm'):
            config = config.xpath('FactorCovTerm')[0]
            attributes = self.workspace.CreateAttributeSet()
            config_factors = []
            for elem in config:
                factor = self.risk_model_name + '_' + elem.attrib['value']
                if factor in self.risk_factors:
                    attributes.Set(factor, float(config.attrib['weight']))
                    config_factors.append(factor)
                else:
                    self.logger.warning('{} not a valid risk factor?', factor)
            for factor in self.risk_factors:
                if factor not in config_factors:
                    attributes.Set(factor, float(config.attrib['default']))
            self.utility.AddCovarianceTerm(float(config.attrib['lambda_f']), barraopt.eXWFWX, None, attributes)
            self.logger.debug('... AddCovarianceTerm ...')
        self.logger.debug('<<< Utility')

    def setup_constraints(self, date):
        self.logger.debug('Constraints >>>')
        self.constraints = self.case.InitConstraints()
        if self.config.xpath('//Constraints/TransactionCost'):
            config = self.config.xpath('//Constraints/TransactionCost')[0]
            constraint = self.constraints.SetTransactionCostConstraint()
            constraint.SetUpperBound(float(config.attrib['upper']), barraopt.eABSOLUTE)
            constraint.SetSoft(util.parse_bool(config.attrib['soft']))
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
        self.logger.debug('<<< Constraints')

    def setup_frontier(self, date):
        self.logger.debug('Frontier >>>')
        config = self.config.xpath('Frontier')[0]
        self.frontier = self.case.InitFrontier(str2enum[config.attrib['type']])
        if config.attrib.get('id', None):
            self.frontier.SetFrontierConstraintID(str(config.attrib['id']))
            self.logger.debug('... SetFrontierConstraintID ...')
        if config.attrib.get('include_cost', None):
            self.frontier.SetIncludeTransactionCost(util.parse_bool(config.attrib['include_cost']))
            self.logger.debug('... SetIncludeTransactionCost ...')
        if config.attrib.get('bound', None):
            self.frontier.SetFrontierBoundType(str2enum[config.attrib['bound']])
            self.logger.debug('... SetFrontierBoundType ...')
        if config.attrib.get('lower', None) and config.attrib.get('upper', None):
            self.frontier.SetFrontierRange(float(config.attrib['lower']), float(config.attrib['upper']))
            self.logger.debug('... SetFrontierRange ...')
        if config.attrib.get('max_points', None):
            self.frontier.SetMaxNumDataPoints(int(config.attrib['max_points']))
            self.logger.debug('... SetMaxNumDataPoints ...')
        self.logger.debug('<<< Frontier')

    # paring constraints >>>
    def setup_paring_constraints(self, date):
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
        def set_constraint(config, tag_map):
            if config.tag not in tag_map:
                return
            attrs = tag_map[config.tag]
            if config.attrib.get('min', None):
                getattr(self.paring_constraints, attrs[0])(int(config.attrib['min']), util.parse_bool(config.attrib.get('min_soft', config.attrib.get('soft', False))))
                self.logger.debug('... ' + attrs[0] + ' ...')
            if len(attrs) == 1:
                return
            if config.attrib.get('max', None):
                getattr(self.paring_constraints, attrs[1])(int(config.attrib['max']), util.parse_bool(config.attrib.get('max_soft', config.attrib.get('soft', False))))
                self.logger.debug('... ' + attrs[1] + ' ...')

        self.logger.debug('ParingConstraints >>>')
        config = self.config.xpath('//Constraints/ParingConstraints')[0]
        self.paring_constraints = self.constraints.InitParingConstraints()
        self.paring_constraints.SetAllowCloseOut(util.parse_bool(config.attrib.get('closeout', True)))
        self.logger.debug('... SetAllowCloseOut ...')
        for elem in config:
            if elem.tag == 'Holding':
                set_constraint(elem, holding_map)
                for sub_elem in elem:
                    set_constraint(sub_elem, holding_map)
            elif elem.tag == 'Trading':
                set_constraint(elem, trading_map)
                for sub_elem in elem:
                    set_constraint(sub_elem, trading_map)
            elif elem.tag == 'Asset':
                self.setup_level_paring_by_asset(elem, date)
            elif elem.tag == 'Group':
                if elem.attrib.get('level_path', None):
                    self.setup_group_level_paring(elem, date)
                if elem.attrib.get('trade_path', None):
                    self.setup_group_trade_paring(elem, date)
        self.logger.debug('<<< ParingConstraints')

    def setup_asset_level_paring(self, config, date):
        path = util.generate_path(config.attrib['path'], date)
        self.asset_level_paring_df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
        self.asset_level_paring_df['level_paring_type'] = self.asset_level_paring_df['level_paring_type'].map(str2enum)
        if not 'soft' in self.asset_level_paring_df.columns:
            self.asset_level_paring_df['soft'] = util.parse_bool(config.attrib.get('soft', False))
        self.asset_level_paring_df['soft'] = self.asset_level_paring_df['soft'].fillna(False).apply(util.parse_bool)
        for _, row in self.asset_level_paring_df.iterrows():
            if row.notnull()['threshold']:
                self.paring_constraints.AddLevelParingByAsset(row['paring_type'], str(row['bid']), float(row['threshold']), row['soft'])
        self.logger.debug('... AddLevelParingByAsset ...')

    def setup_group_level_paring(self, config, date):
        path = util.generate_path(config.attrib['level_path'], date)
        self.group_level_paring_df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
        self.group_level_paring_df['paring_type'] = self.group_level_paring_df['paring_type'].map(str2enum)
        if not 'soft' in self.group_level_paring_df.columns:
            self.group_level_paring_df['soft'] = util.parse_bool(config.attrib.get('level_soft', False))
        self.group_level_paring_df['soft'] = self.group_level_paring_df['soft'].fillna(False).apply(util.parse_bool)
        for _, row in self.group_level_paring_df.iterrows():
            if row.notnull()['threshold']:
                self.paring_constraints.AddLevelParingByGroup(row['paring_type'], str(row['group']), str(row['attribute']), float(row['threshold']), row['soft'])
        self.logger.debug('... AddLevelParingByGroup ...')

    def setup_group_trade_paring(self, config, date):
        path = util.generate_path(config.attrib['trade_path'], date)
        self.group_trade_paring_df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
        self.group_trade_paring_df['paring_type'] = self.group_trade_paring_df['paring_type'].map(str2enum)
        if not 'min_soft' in self.group_trade_paring_df.columns:
            self.group_trade_paring_df['min_soft'] = util.parse_bool(config.attrib.get('trade_min_soft', config.attrib.get('trade_soft', False)))
        self.group_trade_paring_df['min_soft'] = self.group_trade_paring_df['min_soft'].fillna(False).apply(util.parse_bool)
        if not 'max_soft' in self.group_trade_paring_df.columns:
            self.group_trade_paring_df['max_soft'] = util.parse_bool(config.attrib.get('trade_max_soft', config.attrib.get('trade_soft', False)))
        self.group_trade_paring_df['max_soft'] = self.group_trade_paring_df['max_soft'].fillna(False).apply(util.parse_bool)
        for _, row in self.group_level_paring_df.iterrows():
            if row.notnull()['min'] and row.notnull()['max']:
                range = self.paring_constraints.AddAssetTradeParingByGroup(row['paring_type'], str(row['group']), str(row['attribute']))
                range.SetMin(int(row['min']), row['min_soft'])
                range.SetMax(int(row['max']), row['max_soft'])
        self.logger.debug('... AddAssetTradeParingByGroup ...')
    # <<< paring constraints

    # hedge constraints >>>
    def setup_hedge_constraints(self, date):
        self.logger.debug('Hedge Constraints >>>')
        config = self.config.xpath('//Constraints/HedgeConstraints')[0]
        self.hedge_constraints = self.constraints.InitHedgeConstraints()
        for elem in config:
            if elem.tag == 'Leverage':
                self.setup_leverage_range(elem, date)
            elif elem.tag == 'LeverageRatio':
                self.setup_leverage_ratio_range(elem, date)
        self.logger.debug('<<< Hedge Constraints')

    def setup_leverage_range(self, config, date):
        def get_relative(name):
            attr = elem.attrib.get(name, config.attrib.get(name, elem.attrib.get('relative', config.attrib.get('relative', 'eABSOLUTE'))))
            return str2enum[attr]

        for elem in config:
            if elem.tag == 'Long':
                constraint = self.hedge_constraints.SetLongSideLeverageRange(util.parse_bool(elem.attrib.get('nochange', False)))
                if elem.attrib.get('reference', config.attrib.get('reference', None)):
                    constraint.SetReference(elem.attrib.get('reference', config.attrib['reference']))
                constraint.SetSoft(util.parse_bool(elem.attrib.get('soft', config.attrib.get('soft', False))))
                constraint.SetUpperBound(float(elem.attrib['upper']), get_relative('u_relative'))
                constraint.SetLowerBound(float(elem.attrib['lower']), get_relative('l_relative'))
                self.logger.debug('... SetLongSideLeverageRange ...')
            elif elem.tag == 'Short':
                constraint = self.hedge_constraints.SetShortSideLeverageRange(util.parse_bool(elem.attrib.get('nochange', False)))
                if elem.attrib.get('reference', config.attrib.get('reference', None)):
                    constraint.SetReference(elem.attrib.get('reference', config.attrib['reference']))
                constraint.SetSoft(util.parse_bool(elem.attrib.get('soft', config.attrib.get('soft', False))))
                constraint.SetUpperBound(float(elem.attrib['upper']), get_relative('u_relative'))
                constraint.SetLowerBound(float(elem.attrib['lower']), get_relative('l_relative'))
                self.logger.debug('... SetShortSideLeverageRange ...')
            elif elem.tag == 'Total':
                constraint = self.hedge_constraints.SetTotalLeverageRange(util.parse_bool(elem.attrib.get('nochange', False)))
                constraint.SetSoft(util.parse_bool(elem.attrib.get('soft', config.attrib.get('soft', False))))
                constraint.SetUpperBound(float(elem.attrib['upper']), get_relative('u_relative'))
                constraint.SetLowerBound(float(elem.attrib['lower']), get_relative('l_relative'))
                self.logger.debug('... SetTotalLeverageRange ...')

    def setup_leverage_ratio_range(self, config, date):
        pass
    # <<< hedge constraints

    # linear constraints >>>
    def setup_linear_constraints(self, date):
        self.logger.debug('Linear Constraints >>>')
        config = self.config.xpath('//Constraints/LinearConstraints')[0]
        self.linear_constraints = self.constraints.InitLinearConstraints()
        self.linear_constraints.EnableCrossovers(util.parse_bool(config.attrib.get('crossover', True)))
        for elem in config:
            if elem.tag == 'AssetRange':
                self.setup_asset_range_constraint(elem, date)
            elif elem.tag == 'FactorRange':
                self.setup_factor_range_constraint(elem, date)
            elif elem.tag == 'Beta':
                self.setup_beta_constraint(elem, date)
            elif elem.tag == 'Group':
                self.setup_group_constraint(elem, date)
        self.logger.debug('<<< Linear Constraints')

    def setup_asset_range_constraint(self, config, date):
        # first: cash asset
        constraint = self.linear_constraints.SetAssetRange('CASH')
        constraint.SetUpperBound(1, barraopt.eABSOLUTE)
        constraint.SetLowerBound(1, barraopt.eABSOLUTE)

        # second: regular assets
        path = util.generate_path(config.attrib.get('path', ''), date)
        white = util.generate_path(config.attrib.get('white', ''), date)
        black = util.generate_path(config.attrib.get('black', ''), date)
        file_sids = set()
        if os.path.exists(path):
            self.asset_range_df = util.read_csv(path, header=0, dtype={0: str}, index_col=0)
            if 'relative' in self.asset_range_df.columns:
                self.asset_range_df['relative'] = self.asset_range_df['relative'].map(str2enum)
            if 'l_relative' in self.asset_range_df.columns:
                self.asset_range_df['l_relative'] = self.asset_range_df['l_relative'].map(str2enum)
            if 'u_relative' in self.asset_range_df.columns:
                self.asset_range_df['u_relative'] = self.asset_range_df['u_relative'].map(str2enum)

            for sid, row in self.asset_range_df.iterrows():
                if not sid in self.assets_df.index:
                    continue
                file_sids.add(sid)
                constraint = self.linear_constraints.SetAssetRange(row['bid'])
                if 'u_relative' in row.index:
                    constraint.SetUpperBound(float(row['upper']), row['u_relative'])
                else:
                    constraint.SetUpperBound(float(row['upper']), row['relative'])
                if 'l_relative' in row.index:
                    constraint.SetLowerBound(float(row['lower']), row['l_relative'])
                else:
                    constraint.SetLowerBound(float(row['lower']), row['relative'])

        def get_relative(name, config, default='eABSOLUTE'):
            return str2enum[config.attrib.get(name, config.attrib.get('relative', default))]

        if os.path.exists(white):
            whites = json.load(open(white))
            for sid in whites:
                if sid not in self.assets_df.index:
                    continue
                bid = self.assets_df['bid'][sid]
                if sid in file_sids:
                    self.warning('Asset range already set for {} in {}', sid, path)
                    continue
                constraint = self.linear_constraints.SetAssetRange(bid)
                file_sids.add(sid)
                w = self.booksize/(self.booksize+self.cashflow*1.0)
                constraint.SetLowerBound(self.assets_df['weight'].ix[sid]*w, barraopt.eABSOLUTE)
                constraint.SetUpperBound(float(config.attrib['upper']), get_relative('u_relative', config))
        if os.path.exists(black):
            blacks = json.load(open(black))
            for sid in blacks:
                if sid not in self.assets_df.index:
                    continue
                bid = self.assets_df['bid'].ix[sid]
                if sid in file_sids:
                    self.warning('Asset range already set for {} in {}', sid, path)
                    continue
                constraint = self.linear_constraints.SetAssetRange(bid)
                file_sids.add(sid)
                constraint.SetUpperBound(0, barraopt.eABSOLUTE)
                constraint.SetLowerBound(0, barraopt.eABSOLUTE)

        for bid in self.regular_bids:
            if self.bid_sid[bid] in file_sids:
                continue
            constraint = self.linear_constraints.SetAssetRange(bid)
            constraint.SetUpperBound(float(config.attrib['upper']), get_relative('u_relative', config))
            constraint.SetLowerBound(float(config.attrib['lower']), get_relative('l_relative', config))

        # third: composite assets
        composite_config = config.xpath('Composite')[0]
        for bid in self.composite_bids:
            if composite_config.xpath(bid):
                constraint = self.linear_constraints.SetAssetRange(bid)
                config = composite_config.xpath(bid)[0]
                try:
                    l_relative = get_relative('l_relative', config, default=None)
                except:
                    l_relative = get_relative('l_relative', composite_config)
                try:
                    u_relative = get_relative('u_relative', config, default=None)
                except:
                    u_relative = get_relative('u_relative', composite_config)
                constraint.SetLowerBound(float(config.attrib['lower']), l_relative)
                constraint.SetUpperBound(float(config.attrib['upper']), u_relative)
            elif  composite_config.attrib.get('lower', None) and composite_config.attrib.get('upper', None):
                constraint = self.linear_constraints.SetAssetRange(bid)
                constraint.SetLowerBound(float(composite_config.attrib['lower']), get_relative('l_relative', composite_config))
                constraint.SetUpperBound(float(composite_config.attrib['upper']), get_relative('u_relative', composite_config))
        self.logger.debug('... SetAssetRange ...')

    def setup_factor_range_constraint(self, config, date):
        path = util.generate_path(config.attrib.get('path', ''), date)
        file_factors = set()
        if os.path.exists(path):
            self.factor_range_df = util.read_csv(path, header=0, index_col=0)
            if 'relative' in self.factor_range_df.columns:
                self.factor_range_df['relative'] = self.factor_range_df['relative'].map(str2enum)
            if 'l_relative' in self.factor_range_df.columns:
                self.factor_range_df['l_relative'] = self.factor_range_df['l_relative'].map(str2enum)
            if 'u_relative' in self.factor_range_df.columns:
                self.factor_range_df['u_relative'] = self.factor_range_df['u_relative'].map(str2enum)
            if 'soft' in self.factor_range_df.columns:
                self.factor_range_df['soft'] = self.factor_range_df['soft'].fillna(False).apply(util.parse_bool)
            else:
                self.factor_range_df['soft'] = util.parse_bool(config.attrib.get('soft', False))
            for factor, row in self.factor_range_df.iterrows():
                if not factor in self.risk_factors:
                    continue
                file_factors.add(factor)
                constraint = self.linear_constraints.SetFactorRange(row['factor'])
                constraint.SetSoft(row['soft'])
                if 'reference' in row.index:
                    constraint.SetReference(self.composite_portfolios[row['reference']])
                elif config.attrib.get('reference', None):
                    constraint.SetReference(config.attrib['reference'])
                if 'u_relative' in row.index:
                    constraint.SetUpperBound(float(row['upper']), row['u_relative'])
                else:
                    constraint.SetUpperBound(float(row['upper']), row['relative'])
                if 'l_relative' in row.index:
                    constraint.SetLowerBound(float(row['lower']), row['l_relative'])
                else:
                    constraint.SetLowerBound(float(row['lower']), row['relative'])

        def get_relative(name):
            attr = elem.attrib.get(name, elem.attrib.get('relative', config.attrib.get(name, config.attrib['relative'])))
            return str2enum[attr]

        for elem in config:
            factor = self.risk_model_name+'_'+elem.attrib['value']
            if factor not in self.risk_factors:
                self.logger.warning('Factor {} not a valid risk factor name?', factor)
                continue
            if factor in file_factors:
                self.warning('Factor range already set for {} in {}', factor, path)
                continue
            constraint = self.linear_constraints.SetFactorRange(factor)
            constraint.SetSoft(util.parse_bool(elem.attrib.get('soft', config.attrib.get('soft', False))))
            if elem.attrib.get('reference', config.attrib.get('reference', None)):
                constraint.SetReference(self.composite_portfolios[elem.attrib.get('reference', config.attrib['reference'])])
            constraint.SetLowerBound(float(elem.attrib['lower']), get_relative('l_relative'))
            constraint.SetUpperBound(float(elem.attrib['upper']), get_relative('u_relative'))
        self.logger.debug('... SetFactorRange ...')

    def setup_group_constraint(self, config, date):
        path = util.generate_path(config.attrib.get('path', ''), date)
        file_group_attrs = set()
        if os.path.exists(path):
            self.group_range_df = util.read_csv(path, header=0)
            if 'relative' in self.group_range_df.columns:
                self.group_range_df['relative'] = self.group_range_df['relative'].map(str2enum)
            if 'l_relative' in self.group_range_df.columns:
                self.group_range_df['l_relative'] = self.group_range_df['l_relative'].map(str2enum)
            if 'u_relative' in self.group_range_df.columns:
                self.group_range_df['u_relative'] = self.group_range_df['u_relative'].map(str2enum)
            if 'soft' in self.group_range_df.columns:
                self.group_range_df['soft'] = self.group_range_df['soft'].fillna(False).apply(util.parse_bool)
            for _, row in self.group_range_df.iterrows():
                file_group_attrs.add((row['group'], row['attribute']))
                constraint = self.linear_constraints.AddGroupConstraint(row['group'], row['attribute'])
                constraint.SetSoft(row['soft'])
                if 'reference' in row.index:
                    constraint.SetReference(self.composite_portfolios[row['reference']])
                if 'u_relative' in row.index:
                    constraint.SetUpperBound(float(row['upper']), row['u_relative'])
                else:
                    constraint.SetUpperBound(float(row['upper']), row['relative'])
                if 'l_relative' in row.index:
                    constraint.SetLowerBound(float(row['lower']), row['l_relative'])
                else:
                    constraint.SetLowerBound(float(row['lower']), row['relative'])

        def get_relative(name):
            attr = sub_config.attrib.get(name, sub_config.attrib.get('relative', elem.attrib.get(name, elem.attrib['relative'])))
            return str2enum[attr]

        for group in self.group_names:
            if not config.xpath(group.capitalize()):
                continue
            elem = config.xpath(group.capitalize())[0]
            elem_attrs = set()
            for sub_config in elem:
                attr = sub_config.attrib['value']
                elem_attrs.add(attr)
                if (group, attr) in file_group_attrs:
                    self.logger.warning('Group range already set for ({}, {}) in {}', group, attr, path)
                    continue
                constraint = self.linear_constraints.AddGroupConstraint(group, sub_config.attrib['value'])
                constraint.SetSoft(util.parse_bool(sub_config.attrib.get('soft', elem.attrib.get('soft', False))))
                if sub_config.attrib.get('reference', elem.attrib.get('reference', None)):
                    constraint.SetReference(self.composite_portfolios[sub_config.attrib.get('reference', elem.attrib['reference'])])
                constraint.SetLowerBound(float(sub_config.attrib['lower']), get_relative('l_relative'))
                constraint.SetUpperBound(float(sub_config.attrib['upper']), get_relative('u_relative'))
            attrs = self.groups_df[group].unique()
            if elem.attrib.get('lower', None) and elem.attrib.get('upper', None):
                for attr in attrs:
                    if attr not in elem_attrs:
                        continue
                    constraint = self.linear_constraints.AddGroupConstraint(group, str(attr))
                    constraint.SetSoft(util.parse_bool(elem.attrib.get('soft', False)))
                    if elem.attrib.get('reference', None):
                        constraint.SetReference(self.composite_portfolios[elem.attrib['reference']])
                    constraint.SetLowerBound(float(elem.attrib['lower']), str2enum[elem.attrib.get('u_relative', elem.attrib['relative'])])
                    constraint.SetUpperBound(float(elem.attrib['upper']), str2enum[elem.attrib.get('u_relative', elem.attrib['relative'])])
        self.logger.debug('... AddGroupConstraint ...')

    def setup_beta_constraint(self, config, date):
        constraint = self.linear_constraints.SetBetaConstraint()
        constraint.SetLowerBound(float(config.attrib['lower']))
        constraint.SetUpperBound(float(config.attrib['upper']))
        constraint.SetSoft(util.parse_bool(config.attrib.get('soft', False)))
    # <<< linear constraints

    # turnover constraints >>>
    def setup_turnover_constraints(self, date):
        self.logger.debug('Turnover Constraints >>>')
        config = self.config.xpath('//Constraints/TurnoverConstraints')[0]
        self.turnover_constraints = self.constraints.InitTurnoverConstraints()
        for elem in config:
            if elem.tag == 'Long':
                self.setup_long_constraint(elem, date)
            elif elem.tag == 'Short':
                self.setup_short_constraint(elem, date)
            elif elem.tag == 'Buy':
                self.setup_buy_constraint(elem, date)
            elif elem.tag == 'Sell':
                self.setup_sell_constraint(elem, date)
            elif elem.tag == 'Net':
                self.setup_net_constraint(elem, date)
        self.logger.debug('<<< Turnover Constraints')

    def setup_turnover_constraint(self, config, category):
        category_map = {
            'long': 'SetLongSideConstraint',
            'short': 'SetShortSideConstraint',
            'buy': 'SetBuySideConstraint',
            'sell': 'SetSellSideConstraint',
            'net': 'SetNetConstraint',
            }
        constraint = getattr(self.turnover_constraints, category_map[category])()
        constraint.SetSoft(util.parse_bool(config.attrib.get('soft', False)))
        constraint.SetUpperBound(float(config.attrib['upper']))
        self.logger.debug('... ' + category_map[category] + ' ...')

    def setup_long_constraint(self, config, date):
        self.setup_turnover_constraint(config, 'long')

    def setup_short_constraint(self, config, date):
        self.setup_turnover_constraint(config, 'short')

    def setup_buy_constraint(self, config, date):
        self.setup_turnover_constraint(config, 'buy')

    def setup_sell_constraint(self, config, date):
        self.setup_turnover_constraint(config, 'sell')

    def setup_net_constraint(self, config, date):
        self.setup_turnover_constraint(config, 'net')
    # <<< turnover constraints

    # risk constraints >>>
    def setup_risk_constraints(self, date):
        self.logger.debug('Risk Constraints >>>')
        self.risk_contraints = self.constraints.InitRiskConstraints()
        config = self.config.xpath('//Constraints/RiskConstraints')[0]
        for elem in config:
            if elem.tag == 'Factor':
                self.setup_factor_constraint(elem, date)
            elif elem.tag == 'Specific':
                self.setup_specific_constraint(elem, date)
            elif elem.tag == 'Total':
                self.setup_total_constraint(elem, date)
        self.logger.debug('<<< Risk Constraints')

    def setup_factor_constraint(self, config, date):
        for elem in config:
            id_path = util.generate_path(elem.attrib['id_path'], date)
            bids = self.workspace.CreateIDSet()
            for sid in json.load(open(id_path)):
                bids.Add(self.sid_bid[sid])
            factor_path = util.generate_path(elem.attrib['factor_path'], date)
            factors = self.workspace.CreateIDSet()
            for factor in json.load(open(factor_path)):
                factors.Add(factor)

            if elem.atrrib.get('benchmark', None):
                benchmark = self.composite_portfolios[elem.attrib['benchmark']]
            else:
                benchmark = None
            soft = util.parse_bool(elem.attrib.get('soft', False))
            relative = util.parse_bool(elem.attrib.get('relative', False))
            constraint = self.risk_contraints.AddFactorConstraint(bids, factors, True, benchmark, relative, False, soft)
            constraint.SetUpperBound(float(elem.attrib['upper']))
        self.logger.debug('... AddFactorConstraint ...')

    def setup_specific_constraint(self, config, date):
        for elem in config:
            id_path = util.generate_path(elem.attrib['id_path'], date)
            bids = self.workspace.CreateIDSet()
            for sid in json.load(open(id_path)):
                bids.Add(self.sid_bid[sid])
            if elem.atrrib.get('benchmark', None):
                benchmark = self.composite_portfolios[elem.attrib['benchmark']]
            else:
                benchmark = None
            soft = util.parse_bool(elem.attrib.get('soft', False))
            relative = util.parse_bool(elem.attrib.get('relative', False))
            constraint = self.risk_contraints.AddSpecificConstraint(bids, True, benchmark, relative, False, soft)
            constraint.SetUpperBound(float(elem.attrib['upper']))
        self.logger.debug('... AddSpecificConstraint ...')

    def setup_total_constraint(self, config, date):
        for elem in config:
            id_path = util.generate_path(elem.attrib['id_path'], date)
            bids = self.workspace.CreateIDSet()
            for sid in json.load(open(id_path)):
                bids.Add(self.sid_bid[sid])
            factor_path = util.generate_path(elem.attrib['factor_path'], date)
            factors = self.workspace.CreateIDSet()
            for factor in json.load(open(factor_path)):
                factors.Add(factor)

            if elem.atrrib.get('benchmark', None):
                benchmark = self.composite_portfolios[elem.attrib['benchmark']]
            else:
                benchmark = None
            soft = util.parse_bool(elem.attrib.get('soft', False))
            relative = util.parse_bool(elem.attrib.get('relative', False))
            constraint = self.risk_contraints.AddTotalConstraint(bids, factors, True, benchmark, relative, False, soft)
            constraint.SetUpperBound(float(elem.attrib['upper']))
        self.logger.debug('... AddTotalConstraint ...')
    # <<< risk constraints
# --------

    def generate_report(self, date):
        #self.calculate_factor_exposure(date)
        self.calculate_group_exposure(date)
        pass

    def calculate_factor_exposure(self, date):
        self.init_long_portfolio_df = self.init_portfolio_df.query('weight > 0')
        self.init_short_portfolio_df = self.init_portfolio_df.query('weight < 0')
        self.init_long_portfolio = self.to_portfolio(self.init_long_portfolio_df, 'init_long_portfolio')
        self.init_short_portfolio = self.to_portfolio(self.init_short_portfolio_df, 'init_short_portfolio')
        self.output_long_portfolio_df = self.output_portfolio_df.query('weight > 0')
        self.output_short_portfolio_df = self.output_portfolio_df.query('weight < 0')
        self.output_long_portfolio = self.to_portfolio(self.output_long_portfolio_df, 'output_long_portfolio')
        self.output_short_portfolio = self.to_portfolio(self.output_short_portfolio_df, 'output_short_portfolio')

        exposures, factors = [], []
        for factor in self.risk_factors:
            if factor == self.risk_model_name + '_' + 'COUNTRY':
                continue
            init_exposure = self.risk_model.ComputePortExposure(self.init_portfolio, factor) * 100
            init_long_exposure = self.risk_model.ComputePortExposure(self.init_long_portfolio, factor) * 100
            init_short_exposure = self.risk_model.ComputePortExposure(self.init_short_portfolio, factor) * 100
            output_exposure = self.risk_model.ComputePortExposure(self.output_portfolio, factor) * 100
            output_long_exposure = self.risk_model.ComputePortExposure(self.output_long_portfolio, factor) * 100
            output_short_exposure = self.risk_model.ComputePortExposure(self.output_short_portfolio, factor) * 100
            diff = output_exposure - init_exposure
            diff_long = output_long_exposure - init_long_exposure
            diff_short = output_short_exposure - init_short_exposure
            exposures.append((output_exposure, init_exposure, diff,
                    output_long_exposure, init_long_exposure, diff_long,
                    output_short_exposure, init_short_exposure, diff_short))
            factors.append(factor)
        exposures = pd.DataFrame(exposures)
        exposures.columns = ['output', 'init', 'diff','output_long', 'init_long', 'diff_long',
                    'output_short', 'init_short', 'diff_short']
        exposures.index = factors
        exposures_string = exposures.to_string(float_format=lambda x: '%.2f' % x, col_space=10)

        path = self.config.xpath('//Solver/Exposure')[0].attrib['factor']
        path = util.generate_path(path, date)
        with open(path, 'w') as file:
            file.write(exposures_string)
        self.logger.debug('Factor Exposure: saved in file {}\n{}', path, exposures_string)

    def calculate_group_exposure(self, date):
        config = self.config.xpath('//Solver/Exposure')[0]
        if not config.attrib.get('group', None) or not self.group_names:
            return

        self.expanded_init_long_portfolio_df, self.expanded_init_short_portfolio_df = \
                util.expand_portfolio(self.init_portfolio_df, self.composite_dfs, self.sid_bid)
        self.expanded_output_long_portfolio_df, self.expanded_output_short_portfolio_df = \
                util.expand_portfolio(self.output_portfolio_df, self.composite_dfs, self.sid_bid)
        a = set(self.expanded_output_short_portfolio_df.index)
        print a == set(self.composite_dfs['HS300'].index)
        self.expanded_init_portfolio_df = util.compact_portfolio(
                self.expanded_init_long_portfolio_df, self.expanded_init_short_portfolio_df)
        self.expanded_output_portfolio_df = util.compact_portfolio(
                self.expanded_output_long_portfolio_df, self.expanded_output_short_portfolio_df)
        if config.attrib.get('expanded', None):
            path = util.generate_path(config.attrib['expanded'], date)
            self.expanded_output_portfolio_df.to_csv(path, index=False, float_format='%.6f')

        exposures = []
        for group in self.group_names:
            attrs = self.groups_df[group].unique()
            init_exposure = self.expanded_init_portfolio_df['weight'].groupby(self.groups_df[group]).sum() * 100
            init_exposure = init_exposure.reindex(index=attrs).fillna(0)
            init_long_exposure = self.expanded_init_long_portfolio_df['weight'].groupby(self.groups_df[group]).sum() * 100
            init_short_exposure = self.expanded_init_short_portfolio_df['weight'].groupby(self.groups_df[group]).sum() * 100
            init_exposure, init_long_exposure, init_short_exposure = \
                    init_exposure.reindex(index=attrs).fillna(0), init_long_exposure.reindex(index=attrs).fillna(0), init_short_exposure.reindex(index=attrs).fillna(0)
            output_exposure = self.expanded_output_portfolio_df['weight'].groupby(self.groups_df[group]).sum() * 100
            output_long_exposure = self.expanded_output_long_portfolio_df['weight'].groupby(self.groups_df[group]).sum() * 100
            output_short_exposure = self.expanded_output_short_portfolio_df['weight'].groupby(self.groups_df[group]).sum() * 100
            output_exposure, output_long_exposure, output_short_exposure = \
                    output_exposure.reindex(index=attrs).fillna(0), output_long_exposure.reindex(index=attrs).fillna(0), output_short_exposure.reindex(index=attrs).fillna(0)
            diff = output_exposure - init_exposure
            diff_long = output_long_exposure - init_long_exposure
            diff_short = output_short_exposure - init_short_exposure
            df = pd.concat([output_exposure, init_exposure, diff,
                    output_long_exposure, init_long_exposure, diff_long,
                    output_short_exposure, init_short_exposure, diff_short
                    ], axis=1)
            df.columns = ['output', 'init', 'diff', 'output_long', 'init_long', 'diff_long',
                    'output_short', 'init_short', 'diff_short']
            df['group'], df['attribute'] = group, df.index
            df = df.reindex(columns=['group', 'attribute', 'output', 'init', 'diff',
                    'output_long', 'init_long', 'diff_long',
                    'output_short', 'init_short', 'diff_short'])
            exposures.append(df)
        exposures = pd.concat(exposures).sort(['group', 'attribute'])
        exposures_string = exposures.to_string(float_format=lambda x: '%.2f' % x, col_space=10, index=False)
        path = util.generate_path(config.attrib['group'], date)
        with open(path, 'w') as file:
            file.write(exposures_string)
        self.logger.debug('Group Exposure: saved in file {}\n{}', path, exposures_string)

    def run(self, date):
        self.initialize(date)
        self.before(date)
        self.add_assets(date)
        self.construct_init_portfolio(date)
        self.construct_universe(date)
        self.define_risk_model(date)
        self.construct_composite_portfolios(date)
        self.prepare_case(date)
        self.run_optimization(date)
        self.after(date)
        self.workspace.Release()
