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
  CompanyCode AS sid,
  EndDate AS enddate,
  BasicEPS AS basic_eps,
  DilutedEPS AS diluted_eps,
  BasicEPSCut AS basic_eps_cut,
  DilutedEPSCut AS diluted_eps_cut,
  EPS AS eps,
  ROEByReport AS roe_by_report,
  ROE AS roe,
  ROECut AS roe_cut,
  WROE AS roe_weighted,
  WROECut AS roe_cut_weighted,
  OperatingReenue AS operating_income,
  InvestIncome AS investment_income,
  FinancialExpense AS financial_expense,
  FairValueChangeIncome AS fair_value_change_income,
  OperatingProfit AS operating_income,
  NonoperatingIncome AS nonoperating_income,
  NonoperatingExpense AS nonoperating_expense,
  TotalProfit AS total_profit,
  IncomeTaxCost AS income_tax_expense,
  UncertainedInvestmentLosses AS uncertained_investment_loss,
  NPFromParentCompanyOwners AS np_shareholder,
  MinorityProfit AS minority_intest,
  NetProfit AS net_profit,
  NonRecurringProfitLoss AS nonrecurring_pnl,
  NetProfitCut AS net_profit_cut,
  ProfitatISA AS profit_by_isa,
  NetOperateCashFlow AS ocf,
  NetOperateCashFlowPS AS ocf_ps,
  NetInvestCashFlow AS icf,
  NetFinanceCashFlow AS fcf,
  CashEquialentIncrease AS cae_increase,
  ExchanRateChangeEffect AS currency_conversion_effect,
  EndPeriodCashEquivalent AS cae_end_period,
  CashEquialents AS cash_and_equivalent,
  TradingAssets AS trading_assets,
  InterestReceivables AS interest_receivable,
  DividendReceivables AS dividend_receivable,
  AccountReceivables AS account_receivable,
  OtherReceivable AS other_receivable,
  Inventories AS inventory,
  TotalCurrentAssets AS total_current_assets,
  HoldForSaleAssets AS held_for_sale_assets,
  HoldToMaturityInvestments AS held_to_maturity_investment,
  InvestmentProperty AS investment_property,
  LongtermEquityInvest AS longterm_equity_investment,
  IntangibleAssets AS intangible_assets,
  TotalNonCurrentAssets AS total_noncurrent_assets,
  TotalAssets AS total_assets,
  ShortTermLoan AS shorterm_loan,
  TradingLiability AS trading_liability,
  SalariesPayable AS salary_payable,
  DividendPayable AS dividend_payable,
  TaxsPayable AS tax_payable,
  InterestPayable AS interest_payable,
  OtherPayable AS other_payable,
  NonCurrentLiabilityIn1Year AS noncurrent_liability_in_1y,
  TotalCurrentLiability AS total_current_liability,
  TotalNonCurrentLiability AS total_noncurrent_liability,
  TotalLiability AS total_liability,
  PaidInCapital AS paidin_capital,
  CapitalResereFund AS capital_reserve_fund,
  SurplusReserveFund AS surplus_reserve_fund,
  RetainedProfit AS retained_earnings,
  SEWithoutMI AS se_without_mi,
  MinorityInterests AS minority_interest,
  TotalShareholderEquity AS total_shareholder_equity,
  TotalLiabilityAndEquity AS total_liability_and_equity,
  NetAssetISA AS net_assets_by_isa,
  NAPSByReport AS bps_by_report,
  NAPS AS bps,
  NAPSAdjusted AS adjusted_bps,
  TotalShares AS total_shares
FROM
  LC_MainDataNew
WHERE
  AccountingStandards = 1
  AND
  Mark = 1
  AND
  InfoPublDate >= TO_DATE({prev_date}, 'yyyymmdd') AND InfoPublDate < TO_DATE({date}, 'yyyymmdd')
"""
