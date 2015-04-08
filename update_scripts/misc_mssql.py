CMD0 = """
SELECT
  LEFT(s_info_windcode, 6)
FROM
  AShareEODPrices
WHERE
  trade_dt = '{date}'
"""

CMD1 = """
SELECT
  LEFT(s_info_windcode, 6)
FROM
  AShareST
WHERE
  entry_dt <= '{date}'
  AND
  (remove_dt IS NULL OR remove_dt >= '{date}')
"""

CMD2 = """
SELECT
  LEFT(s_info_windcode, 6)
FROM
  AShareTradingSuspension
WHERE
  s_dq_suspenddate = '{date}'
"""
