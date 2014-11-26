CMD0 = """
SELECT
  CompanyCode, SecuCode
FROM
  SecuMain
WHERE
  SecuCategory = 1
  AND
  SecuMarket IN (83, 90)
  AND
  SUBSTR(SecuCode, 1, 2) IN ('60', '00', '30')
"""

CMD = """
SELECT
  *
FROM
  LC_CashflowStatementAll
WHERE
  IfAdjusted = 2
  AND
  IfMerged = 1
  AND
  AccountingStandards = 1
  AND
  JSID >= {prev_date} AND JSID < {date}
"""

dnames = [
# operation cash in flow
'goods_service_cash_in', 'tax_refund_cash_in', 'net_deposit_increase', 'net_borrowing_from_central_bank', 'net_borrowing_from_financial_organzation', 'cancelled_loan_withdrawal', 'interest_and_commission_cash_in', 'net_trading_assets_deal', 'net_buyback', 'original_insurance_cash_in', 'net_reinsurance_cash_in', 'net_insurer_deposit_and_investment_increase', 'other_operation_cash_in', 'ocif_exceptional_items', 'ocif_adjustment_items', 'ocif',
# operation cash out flow
'goods_service_cash_out', 'staff_paid_cash_out', 'tax_paid_cash_out', 'net_client_loan_and_advance_increase', 'net_deposit_increase_in_central_bank_and_interbank', 'net_lend_capital_increase', 'commission_cash_out', 'original_insurance_cash_out', 'net_reinsurance_cash_out', 'policy_dividend_cash_out', 'other_operation_cash_out', 'ocof_exceptional_items', 'ocof_adjustment_items', 'ocof',
# operation cash flow
'ocf_adjustment_items', 'ocf',
# invest cash in flow
'investment_withdraw', 'investment_proceeds', 'assets_disposition_cash_in', 'associate_disposition_cash_in', 'other_invest_cash_in', 'icif_exceptional_items', 'icif_adjustment_items', 'icif',
# invest cash out flow
'assets_acquistion_cash_out', 'investment_cash_out', 'associate_acquistion_cash_out', 'net_impawned_loan_increase', 'other_invest_cash_out', 'icof_exceptional_items', 'icof_adjustment_items', 'icof',
# invest cash flow
'icf_adjustment_items', 'icf',
# finance cash in flow
'invested_cash_in', 'associate_invested_cash_in', 'bond_issue_cash_in', 'borrowing_cash_in', 'other_finance_cash_in', 'fcif_exceptional_items', 'fcif_adjustment_items', 'fcif',
# finance cash out flow
'borrowing_repay_cash_out', 'dividend_interest_cash_out', 'associate_dividend_interest_cash_out', 'other_finance_cash_out', 'fcof_exceptional_items', 'fcof_adjustment_items', 'fcof',
# finance cash flow
'fcf_adjustment_items', 'fcf',
# cash and equivalent
'currency_conversion_effect', 'cae_other_items', 'cae_adjustment_items', 'cae_increase', 'cae_begin_period', 'caei_exceptional_items', 'caei_adjustment_items', 'cae_end_period',
# ocf <--> net profit
'net_profit', 'np_minority_interest', 'assets_impairment_reserve', 'fixed_asset_depreciation', 'intangible_assets_amortization', 'deferred_expense_amortization', 'deferred_expense_decrease', 'accrued_expense_increase', 'noncurrent_assets_deal_loss', 'fixed_assets_scrap_loss', 'fair_value_change_loss', 'financial_expense', 'investment_loss', 'deferred_tax_assets_decrease', 'deferred_tax_liability_increase', 'inventory_decrease', 'operating_receivable_decrease', 'operating_payable_increase', 'ocf_others', 'ocf_exceptional_items_notes', 'ocf_adjustment_items_notes', 'ocf_notes', 'ocf_contrast_adjustment_items',
# other
'debt_to_equity', 'convertible_bond_in_1y', 'fixed_assets_in_financial_lease']
cols = [4, 5] + range(10, 119)
