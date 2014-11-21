import itertools

CMD1 = """
SELECT
  con.stock_code, con.target_price, con.target_price_type, con.score, con.score_type
FROM
  Con_Forecast_Schedule con JOIN GG_SecurityCode gg ON con.stock_code = gg.symbol
WHERE
  ((TO_CHAR(con.entrydate, 'yyyymmdd') = '{prev_date}' AND con.entrytime > '{cutoff}')
   OR
   (TO_CHAR(con.entrydate, 'yyyymmdd') > '{prev_date}' AND TO_CHAR(con.entrydate, 'yyyymmdd') < '{date}')
   OR
   (TO_CHAR(con.entrydate, 'yyyymmdd') = '{date}' AND con.entrytime <= '{cutoff}'))
  AND
  gg.stype = 'EQA'
"""

CMD2 = """
SELECT
  con.stock_code, con.con_type, con.rpt_date, con.c3, con.c1, con.c4, con.c5, con.c6, con.c7, con.c12, con.cb, con.cpb, con.c80, con.c81, con.c82, con.c83, con.c84
FROM
  Con_Forecast_Stk con JOIN GG_SecurityCode gg ON con.stock_code = gg.symbol
WHERE
  stock_type = 1
  AND
  con_type IN (0, 1)
  AND
  ((TO_CHAR(con.entrydate, 'yyyymmdd') = '{prev_date}' AND con.entrytime > '{cutoff}')
   OR
   (TO_CHAR(con.entrydate, 'yyyymmdd') > '{prev_date}' AND TO_CHAR(con.entrydate, 'yyyymmdd') < '{date}')
   OR
   (TO_CHAR(con.entrydate, 'yyyymmdd') = '{date}' AND con.entrytime <= '{cutoff}'))
  AND
  gg.stype = 'EQA'
ORDER BY
  con.stock_code, con.rpt_date
"""

dnames1 = ['target_price', 'target_price_type', 'score', 'score_type']
_dnames2 = ['eps', 'earnings', 'pe', 'peg', 'yoy_earnings', 'roe', 'book', 'pb', 'change_1w', 'change_4w', 'change_13w', 'change_26w', 'change_52w']
dnames2 = ['growth']
for i, _dname in itertools.product(range(-1, 3), _dnames2):
    dnames2.append(_dname + '_' + str(i))
