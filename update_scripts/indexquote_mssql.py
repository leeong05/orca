CMD = """
SELECT
  s_info_windcode, s_dq_preclose, s_dq_open, s_dq_high, s_dq_low, s_dq_close,
  s_dq_volume, s_dq_amount
FROM
  AIndexEODPrices
WHERE
  trade_dt = '{date}'
"""

dnames = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'amount']
