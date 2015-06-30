CMD = """
SELECT
  s_info_windcode,
  buy_value_exlarge_order, sell_value_exlarge_order,
  buy_value_large_order, sell_value_large_order,
  buy_value_med_order, sell_value_med_order,
  buy_value_small_order, sell_value_small_order,
  buy_volume_exlarge_order, sell_volume_exlarge_order,
  buy_volume_large_order, sell_volume_large_order,
  buy_volume_med_order, sell_volume_med_order,
  buy_volume_small_order, sell_volume_small_order,
  trades_count,
  buy_trades_exlarge_order, sell_trades_exlarge_order,
  buy_trades_large_order, sell_trades_large_order,
  buy_trades_med_order, sell_trades_med_order,
  buy_trades_small_order, sell_trades_small_order,
  volume_diff_institute, volume_diff_institute_act,
  volume_diff_large_trader, volume_diff_large_trader_act,
  volume_diff_med_trader, volume_diff_med_trader_act,
  volume_diff_small_trader, volume_diff_small_trader_act,
  value_diff_institute, value_diff_institute_act,
  value_diff_large_trader, value_diff_large_trader_act,
  value_diff_med_trader, value_diff_med_trader_act,
  value_diff_small_trader, value_diff_small_trader_act,
  s_mfd_inflowvolume, net_inflow_rate_volume,
  s_mfd_inflow_openvolume, open_net_inflow_rate_volume,
  s_mfd_inflow_closevolume, close_net_inflow_rate_volume,
  s_mfd_inflow, net_inflow_rate_value,
  s_mfd_inflow_open, open_net_inflow_rate_value,
  s_mfd_inflow_close, close_net_inflow_rate_value,
  tot_volume_bid, tot_volume_ask,
  moneyflow_pct_volume, open_moneyflow_pct_volume, close_moneyflow_pct_volume,
  moneyflow_pct_value, open_moneyflow_pct_value, close_moneyflow_pct_value
FROM
  AShareMoneyflow
WHERE
  trade_dt = '{date}'
"""

dnames = [
        'buy_amount_exlarge', 'sell_amount_exlarge', 'buy_amount_large', 'sell_amount_large', 'buy_amount_medium', 'sell_amount_medium', 'buy_amount_small', 'sell_amount_small',
        'buy_volume_exlarge', 'sell_volume_exlarge', 'buy_volume_large', 'sell_volume_large', 'buy_volume_medium', 'sell_volume_medium', 'buy_volume_small', 'sell_volume_small',
        'number_of_trades',
        'buy_trades_exlarge', 'sell_trades_exlarge', 'buy_trades_large', 'sell_trades_large', 'buy_trades_medium', 'sell_trades_medium', 'buy_trades_small', 'sell_trades_small',
        'volume_diff_exlarge', 'volume_diff_exlarge_act', 'volume_diff_large', 'volume_diff_large_act', 'volume_diff_medium', 'volume_diff_medium_act', 'volume_diff_small', 'volume_diff_small_act',
        'amount_diff_exlarge', 'amount_diff_exlarge_act', 'amount_diff_large', 'amount_diff_large_act', 'amount_diff_medium', 'amount_diff_medium_act', 'amount_diff_small', 'amount_diff_small_act',
        'net_inflow_volume', 'net_inflow_volume_rate', 'open_net_inflow_volume', 'open_net_inflow_volume_rate', 'close_net_inflow_volume', 'close_net_inflow_volume_rate',
        'net_inflow_amount', 'net_inflow_amount_rate', 'open_net_inflow_amount', 'open_net_inflow_amount_rate', 'close_net_inflow_amount', 'close_net_inflow_amount_rate',
        'total_bid_size', 'total_ask_size',
        'moneyflow_volume_pct', 'open_moneyflow_volume_pct', 'close_moneyflow_volume_pct',
        'moneyflow_amount_pct', 'open_moneyflow_amount_pct', 'close_moneyflow_amount_pct'
        ]
