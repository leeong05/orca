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
  LC_BalancesheetAll
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
# current assets
'cash_and_equivalent', 'client_deposit', 'trading_assets', 'note_receivable', 'dividend_receivable', 'interest_receivable', 'account_receivable', 'other_receivable', 'advance_payment', 'inventory', 'bearer_biological_assets', 'deferred_expense', 'noncurrent_assets_in_1y', 'other_current_assets', 'ca_exceptional_items', 'ca_adjustment_items', 'current_assets',
# non current assets
'held_for_sale_assets', 'held_to_maturity_investment', 'investment_property', 'longterm_equity_investment', 'longterm_receivable_account', 'fixed_assets', 'construction_material', 'construction_in_process', 'fixed_assets_liquidation', 'biological_assets', 'oil_gas_assets', 'intangible_assets', 'seat_cost', 'research_and_development', 'goodwill', 'long_deferred_expense', 'deferred_tax_assets', 'other_noncurrent_assets', 'nca_exceptional_items', 'nca_adjustment_items', 'noncurrent_assets',
# financial assets
'loan_and_account_receivable', 'settlement_provision', 'client_provision', 'deposit_in_interbank', 'precious_metal', 'lend_capital', 'derivative_assets', 'bought_sellback_assets', 'loan_and_advance', 'insurance_receivable', 'receivable_subrogate_fee', 'reinsurance_receivable', 'receivable_unearned_reserve', 'receivable_claims_reserve', 'receivable_life_insurance_reserve', 'receivable_longterm_health_insurance_reserve', 'insurer_impawn_loan', 'fixed_deposit', 'refundable_deposit', 'refundable_capital_deposit', 'independence_assets',
# total assets
'other_assets', 'assets_exceptional_items', 'assets_adjust_items', 'total_assets',
# current liability
'shortterm_loan', 'impawned_loan', 'trading_liability', 'note_payable', 'account_payable', 'shortterm_bond_payable', 'advance_receipt', 'salary_payable', 'dividend_payable', 'tax_payable', 'interest_payable', 'other_payable', 'accrued_expense', 'deferred_proceed', 'noncurrent_liability_in_1y', 'other_current_liability', 'cl_exceptional_items', 'cl_adjustment_items', 'current_liability',
# non current liability
'longterm_loan', 'bond_payable', 'longterm_account_payable', 'specific_account_payable', 'estimate_liability', 'deferred_tax_liability', 'other_noncurrent_liability', 'ncl_exceptional_items', 'ncl_adjustment_liability', 'noncurrent_liability',
# financial liability
'borrowing_from_central_bank', 'deposit_of_interbank', 'borrowing_capital', 'derivative_liability', 'sold_buyback_proceed', 'deposit', 'proxy_security_proceed', 'subissue_security_proceed', 'deposit_received', 'advance_insurance', 'commission_payable', 'reinsurance_payable', 'compensation_payable', 'policy_dividend_payable', 'insurer_deposit_investment', 'unearned_premium_reserve', 'outstanding_claims_reserve', 'life_insurance_reserve', 'longterm_health_insurance_reserve', 'indpendence_liability',
'other_liability', 'liability_exceptional_items', 'liability_adjustment_items', 'liability',
# equity
'paidin_capital', 'capital_reserve_fund', 'surplus_reserve_fund', 'retained_earnings', 'treasury_stock', 'ordinary_risk_reserve_fund', 'currency_conversion_difference', 'uncertained_investment_loss', 'other_reserve', 'specific_reserve', 'se_exceptional_items', 'se_adjustment_items', 'se_without_mi', 'minority_interest', 'se_other_items', 'shareholder_equity',
# total liability and equity
'le_exceptional_items', 'le_adjustment_items', 'liability_and_equity']
cols = [4, 5] + range(10, 145)
