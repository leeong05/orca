JYDB fundamental data
=====================

* Fetcher: :py:class:`~orca.mongo.jyfund.JYFundFetcher`

======================================= ============= =================================================
Incomestatement                         Abbreviation  Remark
======================================= ============= =================================================
total_operating_income                  toi           营业总收入
operating_income                        oi            营业收入
net_interest_income                                   利息净收入
interest_income                                       利息收入
interest_expense                                      利息支出
net_commission_income                                 手续费及佣金净收入
commission_income                                     手续费及佣金收入
commission_expense                                    手续费及佣金支出
net_proxy_security_income                             代理证券业务净收入
net_subissue_security_income                          证券承销业务净收入
net_trust_income                                      受托客户资产管理业务净收入
premium_earned                                        已赚保费
premium_income                                        保险业务收入
reinsurance_income                                    分保费收入
reinsurance                                           分出保费
unearned_premium_reserve                              提取未到期责任准备金
other_operating_income                                其他营业收入
oi_exceptional_items                                  营业收入特殊项目
oi_adjustment_items                                   营业收入调整项目
total_operating_expense                 toe           营业总成本
operating_payout                                      营业支出
refunded_premium                                      退保金
compensation_expense                                  赔付支出
amortization_expense                                  摊回赔付支出
premium_reserve                                       提取保险责任准备金
amortization_premium_reserve                          摊回保险责任准备金
policy_dividend_payout                                保单红利支出
reinsurance_expense                                   分保费用
operating_and_administration_expense                  业务及管理费
amortization_reinsurance_expense                      摊回分保费用
insurance_commission_expense                          保险手续费及佣金支出
other_operating_expense                               其他营业成本
operating_expense                       oe            营业成本
operating_tax_surcharge                               营业税金及附加
selling_expense                         se            销售费用
administration_expense                  ae            管理费用
financial_expense                       fe            财务费用
assets_impairment_loss                  ail           资产减值损失
oe_exceptional_items                                  营业成本特殊项目
oe_adjustment_items                                   营业成本调整项目
nonrecurring_pnl                        nrp           非经营性净收益
fair_value_change_income                vci           公允价值变动净收益
investment_income                       ii            投资净收益
associate_investment_income             aii           对联营合营企业的投资收益
currency_conversion_income              cci           汇兑收益 
nonrecurring_exceptional_items                        非经营性净收益特殊项目                         
nonrecurring_adjustment_items                         非经营性净收益调整项目
operating_profit                        op            营业利润
nonoperating_income                     nopi          营业外收入
nonoperating_expense                    nope          营业外支出
noncurrent_assets_deal_loss                           非流动资产处置净损失
tp_exceptional_items                                  影响利润总额的其他项目
tp_adjustment_items                                   影响利润总利的调整项目
total_profit                            tp            利润总额
income_tax_expense                      tax           所得税费用
uncertained_investment_loss             uil           未确认的投资损失
np_exceptional_items                                  影响净利润的其他项目
np_adjustment_items                                   影响净利润的调整项目
net_profit                              np            净利润
np_shareholder                          nps           归属母公司所有者的净利润
np_minority_interest                    npmi          少数股东损益
nps_exceptional_items                                 影响母公司净利润的特殊项目
nps_adjustment_items                                  影响母公司净利润的调整项目
other_comprehensive_income              ci            其他综合收益
oci_adjustment_items                                  影响综合收益总额的调整项目
total_comprehensive_income              tci           综合收益总额
tci_shareholder                         tcis          归属母公司所有者的综合收益总额
tci_minority_interest                   tcimi         归属少数股东的综合收益总额
tcis_adjustment_items                                 影响母公司综合收益总额的调整项目
basic_eps                                             基本每股收益
diluted_eps                                           稀释每股收益
======================================= ============= =================================================

============================================ ============= =================================================
Balancesheet                                 Abbreviation  Remark
============================================ ============= =================================================
cash_and_equivalent                          cae           货币资金
client_deposit                                             客户资金存款
trading_assets                                             交易性金融资产
note_receivable                                            应收票据
dividend_receivable                                        应收股利
interest_receivable                                        应收利息
account_receivable                           ap            应收账款
other_receivable                                           其他应收款
advance_payment                                            预付款项
inventory                                    inv           存货            
bearer_biological_assets                                   消耗性生物资产
deferred_expense                                           待摊费用
noncurrent_assets_in_1y                                    一年内到期的非流动资产
other_current_assets                                       其他流动资产
ca_exceptional_items                                       流动资产特殊项目
ca_adjustment_items                                        流动资产调整项目
current_assets                               ca            流动资产合计
held_for_sale_assets                                       可供出售金融资产
held_to_maturity_investment                                持有到期投资
investment_property                                        投资性房地产
longterm_equity_investment                                 长期股权投资
longterm_receivable_account                  lap           长期应收款
fixed_assets                                 fa            固定资产
construction_material                                      工程物资
construction_in_process                                    在建工程
fixed_assets_liquidation                                   固定资产清理
biological_assets                                          生产性生物资产
oil_gas_assets                                             尤其资产
intangible_assets                            itga          无形资产
seat_cost                                                  交易席位费
research_and_development                     rd            研发支出
goodwill                                                   商誉
long_deferred_expense                                      长期待摊费用
deferred_tax_assets                                        递延所得税资产
other_noncurrent_assets                                    其他非流动性资产
nca_exceptional_items                                      非流动性资产特殊项目
nca_adjustment_items                                       非流动性资产调整项目
noncurrent_assets                            nca           非流动性资产合计
loan_and_account_receivable
settlement_provision
client_provision
deposit_in_interbank
precious_metal
lend_capital
derivative_assets
bought_sellback_assets
loan_and_advance
insurance_receivable
receivable_subrogate_fee
reinsurance_receivable
receivable_unearned_reserve
receivable_claims_reserve
receivable_life_insurance_reserve
receivable_longterm_health_insurance_reserve
insurer_impawn_loan
fixed_deposit
refundable_deposit
refundable_capital_deposit
independence_assets
other_assets
assets_exceptional_items                                   资产特殊项目
assets_adjust_items                                        资产调整项目
total_assets                                 ta            资产总计
shortterm_loan                               sl            短期借款
impawned_loan                                              质押借款
trading_liability                                          交易性金融负债
note_payable                                               应付票据
account_payable                              ap            应付帐款
shortterm_bond_payable                                     应付短期债券
advance_receipt                                            预收款项
salary_payable                                             应付职工薪酬
dividend_payable                                           应付股利
tax_payable                                                应交税费
interest_payable                                           应付利息
other_payable                                              其他应付款
accrued_expense                                            预提费用
deferred_proceed                                           递延收益
noncurrent_liability_in_1y                                 一年内到期的非流动负债
other_current_liability                                    其他流动负债
cl_exceptional_items                                       流动负债特殊项目
cl_adjustment_items                                        流动负债调整项目
current_liability                            cl            流动负债合计
longterm_loan                                              长期借款
bond_payable                                               应付债券
longterm_account_payable                                   长期应付款
specific_account_payable                                   专项应付款
estimate_liability                                         预计负债
deferred_tax_liability                                     递延所得税负债
other_noncurrent_liability                                 其他非流动性负债
ncl_exceptional_items                                      非流动负债特殊项目
ncl_adjustment_liability                                   非流动负债调整项目
noncurrent_liability                         ncl           非流动负债合计
borrowing_from_central_bank
deposit_of_interbank
borrowing_capital
derivative_liability
sold_buyback_proceed
deposit
proxy_security_proceed
subissue_security_proceed
deposit_received
advance_insurance
commission_payable
reinsurance_payable
compensation_payable
policy_dividend_payable
insurer_deposit_investment
unearned_premium_reserve
outstanding_claims_reserve
life_insurance_reserve
longterm_health_insurance_reserve
indpendence_liability
liability_exceptional_items                                负债特殊项目
liability_adjustment_items                                 负债调整项目
liability                                                  负债合计
paidin_capital                                             实收资本，股本
capital_reserve_fund                                       资本公积
surplus_reserve_fund                                       盈余公积
retained_earnings                                          未分配利润
treasury_stock                                             库存股
ordinary_risk_reserve_fund                                 一般风险准备
currency_conversion_difference                             外币报表折算差额
uncertained_investment_loss                  uil           未确认投资损失
other_reserve                                              其他储备
specific_reserve                                           专项储备
se_exceptional_items                                       归属母公司所有者权益特殊项目
se_adjustment_items                                        归属母公司所有者权益调整项目
se_without_mi                                swm           归属母公司股东权益合计
minority_interest                            mi            少数股东权益
se_other_items                                             所有者权益调整项目
shareholder_equity                           se            股东权益合计，所有者权益合计
le_exceptional_items                           
le_adjustment_items 
liability_and_equity                         le            负债和所有者权益（股东权益）总计
============================================ ============= =================================================

================================================== ============= =================================================
Balancesheet                                       Abbreviation  Remark
================================================== ============= =================================================
goods_service_cash_in                              oci           销售商品、提供劳务收到的现金
tax_refund_cash_in                                               收到的税费返还
net_deposit_increase                             
net_borrowing_from_central_bank
net_borrowing_from_financial_organzation
cancelled_loan_withdrawal
interest_and_commission_cash_in
net_trading_assets_deal
net_buyback
original_insurance_cash_in
net_reinsurance_cash_in
net_insurer_deposit_and_investment_increase
other_operation_cash_in
ocif_exceptional_items
ocif_adjustment_items
ocif                                                             经营活动现金流入小计 
goods_service_cash_out                             oco           购买商品、接受劳务支付的现金
staff_paid_cash_out                                              支付给职工以及为职工支付的现金
tax_paid_cash_out                                                支付的各项税费
net_client_loan_and_advance_increase
net_deposit_increase_in_central_bank_and_interbank
net_lend_capital_increase
commission_cash_out
original_insurance_cash_out
net_reinsurance_cash_out
policy_dividend_cash_out
other_operation_cash_out
ocof_exceptional_items
ocof_adjustment_items
ocof                                                             经营活动现金流出小计
nocf_adjustment_items                                            
ocf                                                              经营活动产生的现金流量净额
investment_withdrawal                              iw            收回投资收到的现金
investment_proceeds                                ip            取得投资收益收到的现金
assets_disposition_cash_in                                       处置固定资产、无形资产和其他长期资产收回的现金净
associate_disposition_cash_in                                    处置子公司及其他营业单位收到的现金净额        
other_invest_cash_in                                             收到其他与投资活动有关的现金
icif_exceptional_items 
icif_adjustment_items
icif                                                             投资活动现金流入小计                                     
assets_acquistion_cash_out                                       购建固定资产、无形资产和其他长期资产支付的现金
investment_cash_out                                              投资支付的现金
associate_acquistion_cash_out                                    取得子公司及其他营业单位支付的现金净额
net_impawned_loan_increase                                       质押贷款净增加额
other_invest_cash_out                                            支付其他与投资活动有关的现金
icof_exceptional_items                       
icof_adjustment_items
icof                                                             投资活动现金流出小计
icf_adjustment_items                                         
icf                                                              投资活动产生的现金流量净额
invested_cash_in                                                 吸收投资收到的现金
associate_invested_cash_in                                       子公司吸收少数股东投资收到的现金
bond_issue_cash_in                                               发行债券收到的现金
borrowing_cash_in                                                取得借款收到的现金
other_finance_cash_in                                            收到其他与筹资活动有关的现金
fcif_exceptional_items                                           
fcif_adjustment_items
fcif                                                             筹资活动现金流入小计
borrowing_repay_cash_out                                         偿还债务支付的现金
dividend_interest_cash_out                                       分配股利、利润或偿付利息支付的现金
associate_dividend_interest_cash_out                             子公司支付给少数股东的股利、利润或偿付的利息
other_finance_cash_out                                           支付其他与筹资活动有关的现金
fcof_exceptional_items
fcof_adjustment_items
fcof                                                             筹资活动现金流出小计
fcf_adjustment_items
fcf                                                              筹资活动产生的现金流量净额
currency_conversion_effect                                       汇率变动对现金及现金等价物的影响
cae_other_items                                                  
cae_adjustment_items
cae_increase                                                     现金及现金等价物净增加额
cae_begin_period                                                 期初现金及现金等价物余额
caei_exceptional_items  
caei_adjustment_items   
cae_end_period                                                   期末现金及现金等价物余额
net_profit                                                       净利润
np_minority_interest                                             少数股东损益
assets_impairment_reserve                                        资产减值准备
fixed_asset_depreciation                                         固定资产折旧
intangible_assets_amortization                                   无形资产摊销
deferred_expense_amortization                                    长期待摊费用摊销
deferred_expense_decrease                                        待摊费用减少
accrued_expense_increase                                         预提费用增加
noncurrent_assets_deal_loss                                      处置固定资产、无形资产和其他长期资产的损失
fixed_assets_scrap_loss                                          固定资产报废损失
fair_value_change_loss                                           公允价值变动损失
financial_expense                                                财务费用
investment_loss                                                  投资损失
deferred_tax_assets_decrease                                     递延所得税资产减少
deferred_tax_liability_increase                                  递延所得税负债增加
inventory_decrease                                               存货的减少
operating_receivable_decrease                                    经营性应收项目的减少
operating_payable_increase                                       经营性应付项目的增加
ocf_others                                                       其他
ocf_exceptional_items_notes
ocf_adjustment_items_notes
ocf_notes
ocf_contrast_adjustment_items
debt_to_equity                                                   债务转为资本
convertible_bond_in_1y                                           一年内到期的可转换公司债券
fixed_assets_in_financial_lease                                  融资租入固定资产
================================================== ============= =================================================

Example1::

   jybs = JYFundFetcher('balancesheet', 2012)
   jybs.load()
   ca = jybs.fetch_daily('current_assets', '20140104', offset=1)
   ca_1y = jybs.fetch_daily('current_assets', '20140104', offset=1, quarter_offset=4)
   chg_yoy = ca / ca_1y - 1.

Example2::

   jybs = JYFundFetcher('balancesheet', 2013)
   jybs.load()
   df = jybs.prepare_frame(['current_assets', 'total_assets'], '20140104', delay=1)
   ca, ta = df['current_assets'], df['total_assets']

Example3::

   jybs = JYFundFetcher('balancesheet', 2012)
   jybs.load()
   pl = jybs.prepare_panel(['current_assets', 'total_assets'], '20130101', '20140101')
   ca, ta = pl['current_assets'], pl['total_assets']
   for date in ca.index:
       alpha[date] = ca.ix[date] / ta.ix[date]
