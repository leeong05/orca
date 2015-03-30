CMD = """
SELECT
  s_info_windcode, s_dq_preclose, s_dq_open, s_dq_high, s_dq_low,
  s_dq_close, s_dq_volume, s_dq_amount, s_val_pe, s_val_pb, s_dq_mv, s_val_mv
FROM
  ASWSindexEOD
WHERE
  trade_dt = '{date}'
"""

dnames = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'amount', 'index_pe', 'index_pb', 'float_cap', 'total_cap']
