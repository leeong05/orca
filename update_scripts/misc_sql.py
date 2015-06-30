CMD0_1 = """
SELECT
  LEFT(s_info_windcode, 6)
FROM
  AShareEODPrices
WHERE
  trade_dt = '{date}'
"""

CMD0_2 = """
SELECT
  LEFT(s_info_windcode, 6)
FROM
  AShareDescription
WHERE
  s_info_listdate = '{date}'
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
  AND
  s_type_st = 'S'
"""

CMD2 = """
SELECT
  LEFT(s_info_windcode, 6)
FROM
  AShareTradingSuspension
WHERE
  s_dq_suspenddate = '{date}'
"""
