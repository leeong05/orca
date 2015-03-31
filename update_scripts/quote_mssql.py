CMD = """
SELECT
  LEFT(s_info_windcode, 6), s_dq_preclose, s_dq_open, s_dq_high, s_dq_low,
  s_dq_close, s_dq_pctchange*0.01, s_dq_volume, s_dq_amount, s_dq_avgprice,
  s_dq_adjpreclose, s_dq_adjopen, s_dq_adjhigh, s_dq_adjlow,
  s_dq_adjclose, s_dq_adjfactor
FROM
  AShareEODPrices
WHERE
  trade_dt = '{date}'
"""

dnames = ['prev_close', 'open', 'high', 'low', 'close', 'returns', 'volume', 'amount', 'vwap',
        'adj_prev_close', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_factor']
