"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from base import UpdaterBase


class JYIndexUpdater(UpdaterBase):
    """The updater class for collections 'jyindex'."""

    def __init__(self, timeout=30*60):
        super(JYIndexUpdater, self).__init__(timeout)

    def pre_update(self):
        self.collection = self.db.jyindex
        self.dates = self.db.dates.distinct('date')

    def pro_update(self):
        return

    @staticmethod
    def norm(df1, df0=None, col=None):
        if df0 is None:
            df1[col] = [0 if np.isnan(i) else i for i in df1[col]]
        else:
            df1[col] = [i if np.isnan(j) else j for i, j in zip(df0[col], df1[col])]

    def update(self, date):
        date = self.dates[self.dates.index(date)-1]
        cursor = self.db.jydata.find({'date': date}, {'_id': 0, 'date': 0})
        self.data = pd.DataFrame(list(cursor))
        if len(self.data) == 0:
            return
        self.update_profitability(date)
        self.update_liquidity(date)
        self.update_turnover(date)
        self.update_cashflow(date)
        self.update_capital_structure(date)
        self.update_quality(date)
        self.update_ps(date)
        self.update_misc(date)

    def update_profitability(self, date):
        q0 = self.data.query("dtype == 'Q0'")
        q0.index = q0.sid

        s0 = self.data.query("dtype == 'S0'")
        s0.index = s0.sid

        y0 = self.data.query("dtype == 'Y0'")
        y0.index = y0.sid

        ttm = self.data.query("dtype == 'TTM'")
        ttm.index = ttm.sid

        for dtype, data in zip(['Q', 'S', 'Y', 'TTM'], [q0, s0, y0, ttm]):
            try:
                np_shareholder = data['np_shareholder']
                se_without_mi = data['se_without_mi']
                roe = np_shareholder / se_without_mi
                for sid, val in roe.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'roe': val}}, upsert=True)
            except:
                pass

            try:
                nps_cut = data['nps_cut']
                se_without_mi = data['se_without_mi']
                roe_cut = nps_cut / se_without_mi
                for sid, val in roe_cut.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'roe_cut': val}}, upsert=True)

            except:
                pass

            try:
                np_shareholder = data['np_shareholder']
                assets = data['assets']
                roa = np_shareholder / assets
                for sid, val in roa.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'roa': val}}, upsert=True)
            except:
                pass

            try:
                ebit = data['ebit']
                tax_rate = data['income_tax_expense'] / data['total_profit']
                roic = ebit*(1-tax_rate) / data['invested_capital']
                for sid, val in roic.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'roic': val}}, upsert=True)
            except:
                pass


            try:
                net_profit = data['net_profit']
                operating_income = data['operating_income']
                net_profit_ratio = net_profit / operating_income
                for sid, val in net_profit_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'net_profit_ratio': val}}, upsert=True)
            except:
                pass

            try:
                gross_income = data['operating_income'] - data['operating_expense']
                gross_profit_ratio = gross_income / data['operating_income']
                for sid, val in gross_profit_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'gross_profit_ratio': val}}, upsert=True)
            except:
                pass

            try:
                period_cost = data['selling_expense'] + data['administration_expense'] + data['financial_expense']
                period_cost_ratio = period_cost / data['operating_income']
                for sid, val in period_cost_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'period_cost_ratio': val}}, upsert=True)
            except:
                pass

            try:
                operating_profit = data['operating_profit']
                total_operating_income = data['total_operating_income']
                operating_profit_ratio = operating_profit / total_operating_income
                for sid, val in operating_profit_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'operating_profit_ratio': val}}, upsert=True)
            except:
                pass

            try:
                total_operating_income = data['total_operating_income']
                total_operating_expense = data['total_operating_expense']
                total_cost_ratio = total_operating_expense / total_operating_income
                for sid, val in total_cost_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'total_cost_ratio': val}}, upsert=True)
            except:
                pass

    def update_liquidity(self, date):
        q0 = self.data.query("dtype == 'Q0'")
        q0.index = q0.sid

        s0 = self.data.query("dtype == 'S0'")
        s0.index = s0.sid

        y0 = self.data.query("dtype == 'Y0'")
        y0.index = y0.sid

        ttm = self.data.query("dtype == 'TTM'")
        ttm.index = ttm.sid

        for dtype, data in zip(['Q', 'S', 'Y', 'TTM'], [q0, s0, y0, ttm]):
            try:
                current_assets = data['current_assets']
                current_liability = data['current_liability']
                current_ratio = current_assets / current_liability
                for sid, val in current_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'current_ratio': val}}, upsert=True)
            except:
                pass
            try:
                current_assets = data['current_assets']
                inventory = data['inventory']
                current_liability = data['current_liability']
                quick_ratio = (current_assets - inventory) / current_liability
                for sid, val in quick_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'quick_ratio': val}}, upsert=True)
            except:
                pass
            try:
                cash_and_equivalent = data['cash_and_equivalent']
                current_liability = data['current_liability']
                cash_ratio = cash_and_equivalent / current_liability
                for sid, val in cash_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'cash_ratio': val}}, upsert=True)
            except:
                pass
            try:
                cash_and_equivalent = data['cash_and_equivalent']
                receivables = data['receivables']
                current_liability = data['current_liability']
                super_quick_ratio = (cash_and_equivalent + receivables) / current_liability
                for sid, val in super_quick_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'super_quick_ratio': val}}, upsert=True)
            except:
                pass
            try:
                liability = data['liability']
                se_without_mi = data['se_without_mi']
                tangible_equity = data['tangible_equity']
                liability_equity_ratio = liability / se_without_mi
                liability_tangible_equity_ratio = liability / tangible_equity
                for sid, val in liability_equity_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'liability_equity_ratio': val}}, upsert=True)
                for sid, val in liability_tangible_equity_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'liability_tangible_equity_ratio': val}}, upsert=True)
            except:
                pass
            try:
                interest_liability = data['interest_liability']
                se_without_mi = data['se_without_mi']
                tangible_equity = data['tangible_equity']
                debt_equity_ratio = interest_liability / se_without_mi
                debt_tangible_equity_ratio = interest_liability / tangible_equity
                for sid, val in debt_equity_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'debt_equity_ratio': val}}, upsert=True)
                for sid, val in debt_tangible_equity_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'debt_tangible_equity_ratio': val}}, upsert=True)
            except:
                pass
            try:
                net_liability = data['net_liability']
                se_without_mi = data['se_without_mi']
                tangible_equity = data['tangible_equity']
                net_debt_equity_ratio = net_liability / se_without_mi
                net_debt_tangible_equity_ratio = net_liability / tangible_equity
                for sid, val in net_debt_equity_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'net_debt_equity_ratio': val}}, upsert=True)
                for sid, val in net_debt_tangible_equity_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'net_debt_tangible_equity_ratio': val}}, upsert=True)
            except:
                pass
            try:
                ebitda = data['ebitda']
                liability = data['liability']
                ebitda_liability_ratio = ebitda / liability
                for sid, val in ebitda_liability_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ebitda_liability_ratio': val}}, upsert=True)
            except:
                pass
            try:
                ocf = data['ocf']
                interest_liability = data['interest_liability']
                net_liability = data['net_liability']
                ocf_debt_ratio = ocf / interest_liability
                ocf_net_debt_ratio = ocf / net_liability
                for sid, val in ocf_debt_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocf_debt_ratio': val}}, upsert=True)
                for sid, val in ocf_net_debt_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocf_net_debt_ratio': val}}, upsert=True)
            except:
                pass
            try:
                ebit = data['ebit']
                interest_expense = data['interest_expense']
                interest_cover = ebit / interest_expense[interest_expense > 0]
                for sid, val in interest_cover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'interest_cover': val}}, upsert=True)
            except:
                pass
            try:
                working_capital = data['working_capital']
                noncurrent_liability = data['noncurrent_liability']
                wc_ncl_ratio = working_capital / noncurrent_liability
                for sid, val in wc_ncl_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'wc_ncl_ratio': val}}, upsert=True)
            except:
                pass
            try:
                ocif = data['ocif']
                current_liability = data['current_liability']
                ocif_cl_ratio = ocif / current_liability
                for sid, val in ocif_cl_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocif_cl_ratio': val}}, upsert=True)
            except:
                pass

    def update_turnover(self, date):
        q0 = self.data.query("dtype == 'Q0'")
        q0.index = q0.sid

        s0 = self.data.query("dtype == 'S0'")
        s0.index = s0.sid

        y0 = self.data.query("dtype == 'Y0'")
        y0.index = y0.sid

        ttm = self.data.query("dtype == 'TTM'")
        ttm.index = ttm.sid

        for dtype, data in zip(['Q', 'S', 'Y', 'TTM'], [q0, s0, y0, ttm]):
            try:
                operating_income = data['operating_income']
                inventory = data['inventory']
                inventory_turnover = operating_income / inventory
                account_receivable = data['account_receivable']
                account_receivable_turnover = operating_income / account_receivable
                for sid, val in inventory_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'inventory_turnover': val}}, upsert=True)
                for sid, val in account_receivable_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'account_receivable_turnover': val}}, upsert=True)
            except:
                pass
            try:
                ocif = data['ocif']
                inventory = data['inventory']
                ocif_inventory_turnover = ocif / inventory
                account_receivable = data['account_receivable']
                ocif_account_receivable_turnover = ocif / account_receivable
                for sid, val in ocif_inventory_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocif_inventory_turnover': val}}, upsert=True)
                for sid, val in ocif_account_receivable_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocif_account_receivable_turnover': val}}, upsert=True)
            except:
                pass
            try:
                operating_expense = data['operating_expense']
                account_payable = data['account_payable']
                account_payable_turnover = operating_expense / account_payable
                for sid, val in account_payable_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'account_payable_turnover': val}}, upsert=True)
            except:
                pass
            try:
                ocof = data['ocof']
                account_payable = data['account_payable']
                ocof_account_payable_turnover = ocof / account_payable
                for sid, val in ocof_account_payable_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocof_account_payable_turnover': val}}, upsert=True)
            except:
                pass
            try:
                total_operating_income = data['total_operating_income']
                current_assets = data['current_assets']
                fixed_assets = data['fixed_assets']
                assets = data['assets']
                se_without_mi = data['se_without_mi']
                current_assets_turnover = total_operating_income / current_assets
                fixed_assets_turnover = total_operating_income / fixed_assets
                assets_turnover = total_operating_income / assets
                equity_turnover = total_operating_income / se_without_mi
                for sid, val in current_assets_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'current_assets_turnover': val}}, upsert=True)
                for sid, val in fixed_assets_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'fixed_assets_turnover': val}}, upsert=True)
                for sid, val in assets_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'assets_turnover': val}}, upsert=True)
                for sid, val in equity_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'equity_turnover': val}}, upsert=True)
            except:
                pass
            try:
                ocif = data['ocif']
                current_assets = data['current_assets']
                fixed_assets = data['fixed_assets']
                assets = data['assets']
                se_without_mi = data['se_without_mi']
                ocif_current_assets_turnover = ocif / current_assets
                ocif_fixed_assets_turnover = ocif / fixed_assets
                ocif_assets_turnover = ocif / assets
                ocif_equity_turnover = ocif / se_without_mi
                for sid, val in ocif_current_assets_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocif_current_assets_turnover': val}}, upsert=True)
                for sid, val in ocif_fixed_assets_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocif_fixed_assets_turnover': val}}, upsert=True)
                for sid, val in ocif_assets_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocif_assets_turnover': val}}, upsert=True)
                for sid, val in ocif_equity_turnover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocif_equity_turnover': val}}, upsert=True)
            except:
                pass

    def update_cashflow(self, date):
        q0 = self.data.query("dtype == 'Q0'")
        q0.index = q0.sid

        s0 = self.data.query("dtype == 'S0'")
        s0.index = s0.sid

        y0 = self.data.query("dtype == 'Y0'")
        y0.index = y0.sid

        ttm = self.data.query("dtype == 'TTM'")
        ttm.index = ttm.sid

        for dtype, data in zip(['Q', 'S', 'Y', 'TTM'], [q0, s0, y0, ttm]):
            try:
                cash_in = data['goods_service_cash_in']
                operating_income = data['operating_income']
                cash_income_ratio = cash_in / operating_income
                for sid, val in cash_income_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'cash_income_ratio': val}}, upsert=True)
            except:
                pass
            try:
                ocf = data['ocf']
                operating_income = data['operating_income']
                ocf_income_ratio = ocf / operating_income
                for sid, val in ocf_income_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocf_income_ratio': val}}, upsert=True)
            except:
                pass
            try:
                ocf = data['ocf']
                operating_profit = data['operating_profit']
                net_profit = data['net_profit']
                ocf_profit_ratio = ocf / operating_profit
                ocf_profit_cover = ocf / net_profit
                for sid, val in ocf_profit_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocf_profit_ratio': val}}, upsert=True)
                for sid, val in ocf_profit_cover.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'ocf_profit_cover': val}}, upsert=True)
            except:
                pass

    def update_capital_structure(self, date):
        q0 = self.data.query("dtype == 'Q0'")
        q0.index = q0.sid

        s0 = self.data.query("dtype == 'S0'")
        s0.index = s0.sid

        y0 = self.data.query("dtype == 'Y0'")
        y0.index = y0.sid

        ttm = self.data.query("dtype == 'TTM'")
        ttm.index = ttm.sid

        for dtype, data in zip(['Q', 'S', 'Y', 'TTM'], [q0, s0, y0, ttm]):
            try:
                assets = data['assets']
                liability = data['liability']
                liability_to_assets = liability / assets
                for sid, val in liability_to_assets.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'liability_to_assets': val}}, upsert=True)
                fixed_assets = data['fixed_assets']
                fixed_assets_to_assets = fixed_assets / assets
                for sid, val in fixed_assets_to_assets.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'fixed_assets_to_assets': val}}, upsert=True)
                intangible_assets = data['intangible_assets']
                intangible_assets_to_assets = intangible_assets / assets
                for sid, val in intangible_assets_to_assets.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'intangible_assets_to_assets': val}}, upsert=True)
            except:
                pass
            try:
                invested_capital = data['invested_capital']
                debt = data['interest_liability']
                debt_to_ic = debt / invested_capital
                for sid, val in debt_to_ic.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'debt_to_ic': val}}, upsert=True)
            except:
                pass

    def update_quality(self, date):
        q0 = self.data.query("dtype == 'Q0'")
        q0.index = q0.sid

        s0 = self.data.query("dtype == 'S0'")
        s0.index = s0.sid

        y0 = self.data.query("dtype == 'Y0'")
        y0.index = y0.sid

        ttm = self.data.query("dtype == 'TTM'")
        ttm.index = ttm.sid

        for dtype, data in zip(['Q', 'S', 'Y', 'TTM'], [q0, s0, y0, ttm]):
            try:
                operating_profit = data['operating_profit']
                total_profit = data['total_profit']
                operating_profit_ratio = operating_profit / total_profit
                for sid, val in operating_profit_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'operating_profit_ratio': val}}, upsert=True)
            except:
                pass
            try:
                nonoperating_income = data['nonoperating_income']
                nonoperating_expense = data['nonoperating_expense']
                nonoperating_profit = nonoperating_income - nonoperating_expense
                total_profit = data['total_profit']
                nonoperating_profit_ratio = nonoperating_profit / total_profit
                for sid, val in nonoperating_profit_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'nonoperating_profit_ratio': val}}, upsert=True)
            except:
                pass
            try:
                tci_shareholder = data['tci_shareholder']
                np_shareholder = data['np_shareholder']
                tci_ratio = tci_shareholder / np_shareholder
                for sid, val in tci_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'tci_ratio': val}}, upsert=True)
            except:
                pass
            try:
                noa = data['net_operating_assets']
                accruals_bs = data['accruals_bs']
                noa_1 = noa - accruals_bs
                bs_accruals_ratio = accruals_bs * 2. / (noa_1 + noa)
                for sid, val in bs_accruals_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'bs_accruals_ratio': val}}, upsert=True)
            except:
                pass
            try:
                noa = data['net_operating_assets']
                accruals_bs = data['accruals_bs']
                noa_1 = noa - accruals_bs
                accruals_cf = data['accruals_cf']
                cf_accruals_ratio = accruals_cf * 2. / (noa_1 + noa)
                for sid, val in cf_accruals_ratio.iteritems():
                    key = {'date': date, 'sid': sid, 'dtype': dtype, 'qtrno': data['qtrno'][sid]}
                    self.collection.update(key, {'$set': {'cf_accruals_ratio': val}}, upsert=True)
            except:
                pass

    def update_ps(self, date):
        pass

    def update_misc(self, date):
        pass

if __name__ == '__main__':
    jyindex = JYIndexUpdater()
    jyindex.run()
