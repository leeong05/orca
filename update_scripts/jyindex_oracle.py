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

# np: net profit
# noi: net profit income
# npc: net profit cut
# nps: net profit - shareholders
# npcs: net profit cut -shareholders
# tp: total profit
# oi: operating income
# oe: operating expense
# toi: total operating income
# toe: total operating expense
# gi: gross income
# pe: period expense
# ae: administration expense
# se: selling expense
# fe: financial expense
# ail: assets impairment loss
# ia: investment in associate
# ci: cash income
# fvc: fair value change

# ta: total assets
# ca: current assets
# nca: non current assets
# fa: fixed assets
# tga: tangible assets
# itga: intangbile assets
# tl: total liability
# cl: current liability
# ncl: noncurrent liability
# td: total debt
# cd: current debt
# ncd: non current debt
# nd: net debt
# mi: minority interest
# ar: account receivable
# ap: account payable
# tc: total capital
# wc: working capital
# lta: longterm assets
# ltd: longterm debt
# tge: tangible equity
# swm: shareholder's equity without minority interest
# crf: capital reserve fund
# srf: surplus reserve fund
# rf: reserve fund
# re: retained earnings

# ocf: operating cash flow
# ocif: operating cash in flow
# fcf: free cash flow
# fcff: free cash flow to firm
# fcfe: free cash flow to equity

# cps: cash flow per share
# eps: earnings per share
# dps: dividend per share
# bps: net assets/equity per share

CMD = """
SELECT
  CompanyCode AS sid,
  Enddate AS enddate,
  BasicEPS AS basic_eps,
  DilutedEPS AS diluted_eps,
  EPS AS eps,
  EPSTTM AS eps_ttm,
  NetAssetPS AS bps,
  TotalOperatingRevenuePS AS toi_ps,
  MainIncomePS AS oi_ps,
  OperatingRevenuePSTTM AS oi_ps_ttm,
  OperProfitPS AS op_ps,
  EBITPS AS ebit_ps,
  CapitalSurplusFundPS AS crf_ps,
  SurplusReserveFundPS AS srf_ps,
  AccumulationFundPS AS rf_ps,
  UndividedProfit AS undivided_profit,
  RetainedEarningsPS AS re_ps,
  OperCashFlowPS AS ocf_ps,
  OperCashFlowPSTTM AS ocf_ps_ttm,
  CashFlowPS AS cps,
  CashFlowPSTTM AS cps_ttm,
  EnterpriseFCFPS AS fcff_ps,
  ShareholderFCFPS AS fcfe_ps,
  ROEAvg AS roe_avg,
  ROEWeighted AS roe_weighted,
  ROE AS roe,
  ROECut AS roe_cut,
  ROECutWeighted AS roe_weighted,
  ROETTM AS roe_ttm,
  ROA_EBIT AS roa_ebit,
  ROA_EBITTTM AS roa_ebit_ttm,
  ROA AS roa,
  ROATTM AS roa_ttm,
  ROIC AS roic,
  NetProfitRatio AS np_to_oi,
  NetProfitRatioTTM AS np_to_oi_ttm,
  GrossIncomeRatio AS gi_to_oi,
  GrossIncomeRatioTTM AS gi_to_oi_ttm,
  SalesCostRatio AS oe_to_oi,
  PeriodCostsRate AS pe_to_oi,
  PeriodCostsRateTTM AS pe_to_oi_ttm,
  NPToTOR AS np_to_toi,
  NPToTORTTM AS np_to_toi_ttm,
  OperatingProfitToTOR AS op_to_toi,
  OperatingProfitToTORTTM AS op_to_toi_ttm,
  EBITToTOR AS ebit_to_toi,
  EBITToTORTTM AS ebit_to_toi_ttm,
  TOperatingCostToTOR AS toe_to_toi,
  TOperatingCostToTORTTM AS toe_to_toi_ttm,
  OperatingExpenseRate AS se_to_toi,
  OperatingExpenseRateTTM AS se_to_toi_ttm,
  AdminiExpenseRate AS ae_to_toi,
  AdminiExpenseRateTTM AS ae_to_toi_ttm,
  FinancialExpenseRaTe AS fe_to_toi,
  FinancialExpenseRateTTM AS fe_to_toi_ttm,
  AssetImpaLossToTOR AS ail_to_toi,
  AssetImpaLossToTORTTM AS ail_to_toi_ttm,
  NetProfit AS np,
  NetProfitCut AS npc,
  EBIT AS ebit,
  EBITDA AS ebitda,
  OperatingProfitRatio AS op_to_oi,
  TotalProfitCostRatio AS toi_to_toe,
  CurrentRatio AS current_ratio,
  QuickRatio AS quick_ratio,
  SuperQuickRatio AS super_quick_ratio,
  DebtEquityRatio AS debt_equity_ratio,
  SEWithoutMIToTL AS swm_to_tl,
  SEWMIToInterestBearDebt AS swm_to_debt,
  DebtTangibleEquityRatio AS debt_tge_ratio,
  TangibleAToInteBearDebt AS tga_debt_ratio,
  TangibleAToNetDebt AS tga_nd_ratio,
  EBITDAToTLiability AS ebitda_tl_ratio,
  NOCFToTLiability AS ocf_tl_ratio,
  NOCFToInterestBearDebt AS ocf_debt_ratio,
  NOCFToCurrentLiability AS ocf_cl_ratio,
  NOCFToNetDebt AS ocf_nd_ratio,
  InterestCover AS interest_cover,
  LongDebtToWorkingCapital AS ld_wc_ratio,
  OperCashInToCurrentDebt AS ocif_cd_ratio,
  BasicEPSYOY AS basic_eps_yoy,
  DilutedEPSYOY AS diluted_eps_yoy,
  OperatingRevenueGrowRate AS oi_yoy,
  OperProfitGrowRate AS op_yoy,
  TotalProfeiGrowRate AS tp_yoy,
  NetProfitGrowRate AS np_yoy,
  NPParentCompanyYOY AS nps_yoy,
  NPParentCompanyCutYOY AS npcs_yoy,
  AvgNPYOYPastFiveYear AS avg_nps_yoy_5ys,
  NetOperateCashFlowYOY AS ocf_yoy,
  OperCashPSGrowRate AS ocf_ps_yoy,
  NAORYOY AS roe_yoy,
  NetAssetGrowRate AS bps_yoy,
  TotalAssetGrowRate AS ta_yoy,
  EPSGrowRateYTD AS eps_ytd,
  SEWithoutMIGrowRateYTD AS swm_ytd,
  TAGrowRateYTD AS ta_ytd,
  SustainableGrowRate AS sustainable_growth,
  OperCycle AS operating_cycle,
  InventoryTRate AS inventory_tvr,
  InventoryTDays AS inventory_cycle,
  ARTRate AS ar_tvr,
  ARTDays AS ar_cycle,
  AccountsPayablesTRate AS ap_tvr,
  AccountsPayablesTDays AS ap_cycle,
  CurrentAssetsTRate AS ca_tvr,
  FixedAssetTRate AS fa_tvr,
  EquityTRate AS equity_tvr,
  TotalAssetTRate AS ta_tvr,
  SaleServiceCashToOR AS cash_to_oi,
  SaleServiceCashToORTTM AS cash_to_oi_ttm,
  CashRateOfSales AS ocf_to_oi,
  CashRateOfSalesTTM AS ocf_to_oi_ttm,
  NOCFToOperatingNI AS ocf_to_op,
  NOCFToOperatingNITTM AS ocf_to_op_ttm,
  NOCFToOperatingNI AS ocf_to_noi,
  NOCFToOperatingNITTM AS ocf_to_noi_ttm,
  CapitalExpenditureToDM AS capex_to_da,
  CashEquivalentIncrease AS cash_increase,
  NetOperateCashFlow AS ocf,
  GoodsSaleServiceRenderCash AS ci,
  FreeCashFlow AS fcf,
  NetProfitCashCover AS ocf_to_np,
  OperatingRevenueCashCover AS ci_to_oi,
  OperCashInToAsset AS ocf_to_ta,
  CashEquivalentPS AS cash_ps,
  DividendPS AS dps,
  CashDividendCover AS cash_dividend_cover,
  DividendPaidRatio AS dividend_payout_ratio,
  RetainedEarningRatio AS re_ratio,
  DebtAssetsRatio AS debt_to_ta,
  CurrentAssetsToTA AS ca_to_ta,
  NonCurrentAssetsToTA AS nca_to_ta,
  FixAssetRatio AS fa_to_ta,
  IntangibleAssetRatio AS itga_to_ta,
  LongDebtToAsset AS ld_to_ta,
  BondsPayableToAsset AS bp_to_ta,
  SEWithoutMIToTotalCapital AS swm_to_tc,
  InteBearDebtToTotalCapital AS debt_to_tc,
  CurrentLiabilityToTL AS cl_to_tl,
  NonCurrentLiabilityToTL AS ncl_to_tl,
  EquityToAsset AS equity_to_ta,
  EquityMultipler AS equity_multiplier,
  WorkingCapital AS wc,
  LongDebtToEquity AS ld_to_equity,
  LongAssetFitRate AS la_fit_rate,
  OperatingNIToTP AS noi_to_tp,
  OperatingMIToTPTTM AS noi_to_tp_ttm,
  InvestRAssociatesToTP AS ia_to_tp,
  InvestRAssociatesToTPTTM AS ia_to_tp_ttm,
  ValueChangeNIToTP AS fvc_to_tp,
  ValueChangeNIToTPTTM AS fvc_to_tp_ttm,
  NetNonOperatingIncomeToTP AS nop_oi_to_tp,
  NetNonOIToTPTTM AS nop_oi_to_tp_ttm,
  TaxesToTP AS taxes_to_tp,
  NPCutToTP AS npc_to_tp,
  EquityMultipler_Dupont AS equity_multiplier_dupont,
  NPPCToNP_DuPont AS nps_to_np_dupont,
  NPToTOR_DuPont AS np_to_toi_dupont,
  NPToTP_DuPont AS np_to_tp_dupont,
  TPToEBIT_DuPont AS tp_to_ebit_dupont,
  EBITToTOR_DuPont AS ebit_to_toi_dupont
FROM
  LC_MainIndexNew
WHERE
  JSID >= {prev_date} AND JSID < {date}
"""
