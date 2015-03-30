CMD = """
SELECT
  s_info_windcode,
  s_li_initiativebuyrate, s_li_initiativebuymoney, s_li_initiativebuyamount,
  s_li_initiativesellrate, s_li_initiativesellmoney, s_li_initiativesellamount,
  s_li_largebuyrate, s_li_largebuymoney, s_li_largebuyamount,
  s_li_largesellrate, s_li_largesellmoney, s_li_largesellamount,
  s_li_entrustrate, s_li_entrudifferamount, s_li_entrudifferamoney,
  s_li_entrustbuymoney, s_li_entrustsellmoney,
  s_li_entrustbuyamount, s_li_entrustsellamount
FROM
  AShareL2Indicators
WHERE
  trade_dt = '{date}'
"""

dnames = [
        'act_buy_rate', 'act_buy_amount', 'act_buy_volume',
        'act_sell_rate', 'act_sell_amount', 'act_sell_volume',
        'large_buy_rate', 'large_buy_amount', 'large_buy_volume',
        'large_sell_rate', 'large_sell_amount', 'large_sell_volume',
        'entrust_rate', 'entrust_diff_volume', 'entrust_diff_amount',
        'entrust_buy_amount', 'entrust_sell_amount',
        'entrust_buy_volume', 'entrust_sell_volume',
        ]
