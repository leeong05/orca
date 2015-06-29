CMD = """
SELECT
  s_info_windcode, s_margin_tradingbalance, s_margin_purchwithborrowmoney, s_margin_repaymenttobroker,
  s_margin_seclendingbalance, s_margin_seclendingbalancevol, s_margin_salesofborrowedsec, s_margin_repaymentofborrowsec, s_margin_margintradebalance
FROM
  AShareMarginTrade
WHERE
  trade_dt = '{date}'
"""

dnames = ['margin_buy_balance', 'margin_buy', 'money_repayment', 'margin_sell_balance', 'margin_sell_shares', 'margin_sell', 'security_repayment', 'total_margin_balance']
