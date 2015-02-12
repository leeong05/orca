CMD = """
SELECT
  a1.*, a2.s_dq_adjfactor
FROM
  (SELECT * FROM AShareEODDerivativeIndicator WHERE trade_dt = '{date}') AS a1 INNER JOIN
  (SELECT * FROM AShareEODPrices WHERE trade_dt = '{date}') AS a2 ON
  (a1.s_info_windcode = a2.s_info_windcode)
WHERE
  LEFT(a1.s_info_windcode, 2) IN ('60', '00', '30')
"""

cols = range(4, 25)
dnames = ['total_cap', 'float_cap', 'high_52w', 'low_52w', 'PE', 'PB', 'PE_TTM',
'POCF', 'POCF_TTM', 'PCF', 'PCF_TTM', 'PS', 'PS_TTM',
'turnover', 'free_turnover', 'total_shares', 'float_shares', 'close',
'PD', 'adj_high_52w', 'adj_low_52w']
#dnames = ['total_cap', 'float_cap', 'high_52w', 'low_52w', 'PE', 'PB', 'PE_TTM',
#'PB_TTM', 'POCF', 'POCF_TTM', 'PCF', 'PCF_TTM', 'PS',
#'PS_TTM', 'turnover', 'free_turnover', 'total_shares', 'float_shares',
#'PD', 'adj_high_52w', 'adj_low_52w']
