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
  LC_IncomeStatementAll
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
# income
'total_operating_income', 'operating_income', 'net_interest_income', 'interest_income', 'interest_expense', 'net_commission_income', 'commission_income', 'commission_expense', 'net_proxy_security_income', 'net_subissue_security_income', 'net_trust_income', 'premium_earned', 'premium_income', 'reinsurance_income', 'reinsurance', 'unearned_premium_reserve', 'other_operating_income', 'oi_exceptional_items', 'oi_adjustment_items',
# expense
'total_operating_expense', 'operating_payout', 'refunded_premium', 'compensation_expense', 'amortization_expense', 'premium_reserve', 'amortization_premium_reserve', 'policy_dividend_payout', 'reinsurance_expense', 'operating_and_administration_expense', 'amortization_reinsurance_expense', 'insurance_commission_expense', 'other_operating_expense', 'operating_expense', 'operating_tax_surcharge', 'selling_expense', 'administration_expense', 'financial_expense', 'assets_impairment_loss', 'oe_exceptional_items', 'oe_adjustment_items',
# other
'nonrecurring_pnl', 'fair_value_change_income', 'investment_income', 'associate_investment_income', 'currency_conversion_income', 'nonrecurring_exceptional_items', 'nonrecurring_adjustment_items',
# profit
'operating_profit', 'nonoperating_income', 'nonoperating_expense', 'noncurrent_assets_deal_loss', 'tp_exceptional_items', 'tp_adjustment_items',
# total profit
'total_profit', 'income_tax_expense', 'uncertained_investment_loss', 'np_exceptional_items', 'np_adjustment_items', 'net_profit', 'np_shareholder', 'np_minority_interest', 'nps_exceptional_items', 'nps_adjustment_items',
# other comprehensive
'other_comprehensive_income', 'oci_adjustment_items', 'total_comprehensive_income', 'tci_shareholder', 'tci_minority_interest', 'tcis_adjustment_items',
# eps
'basic_eps', 'diluted_eps']
cols = [4, 5] + range(10, 81)
