import itertools

CMD0 = """
SELECT
  id, org_name
FROM
  GG_Org_List
WHERE
  IsHKRptOrg IS NULL
"""

CMD1 = """
SELECT
  score.stock_code, score.organ_id, TO_CHAR(score.current_create_date, 'yyyymmdd'), cmb.type_id, score.report_id, TO_CHAR(score.previous_create_date, 'yyyymmdd'), score.current_score_id, score.previous_score_id, score.score_adjust_flag
FROM
  Cmb_Report_Score_Adjust score JOIN Cmb_Report_Research cmb ON score.report_id = cmb.id JOIN GG_SecurityCode gg ON gg.symbol = score.stock_code
WHERE
  ((TO_CHAR(score.entrydate, 'yyyymmdd') = '{prev_date}' AND score.entrytime > '{cutoff}')
   OR
   (TO_CHAR(score.entrydate, 'yyyymmdd') > '{prev_date}' AND TO_CHAR(score.entrydate, 'yyyymmdd') < '{date}')
   OR
   (TO_CHAR(score.entrydate, 'yyyymmdd') = '{date}' AND score.entrytime <= '{cutoff}'))
  AND
  gg.stype = 'EQA'
"""

CMD1_0 = """
SELECT
  score.stock_code, score.organ_id, TO_CHAR(score.current_create_date, 'yyyymmdd'), cmb.type_id, score.report_id, TO_CHAR(score.previous_create_date, 'yyyymmdd'), score.current_score_id, score.previous_score_id
FROM
  Cmb_Report_Adjust score JOIN Cmb_Report_Research cmb ON score.report_id = cmb.id JOIN GG_SecurityCode gg ON gg.symbol = score.stock_code
WHERE
  ((TO_CHAR(score.entrydate, 'yyyymmdd') = '{prev_date}' AND score.entrytime > '{cutoff}')
   OR
   (TO_CHAR(score.entrydate, 'yyyymmdd') > '{prev_date}' AND TO_CHAR(score.entrydate, 'yyyymmdd') < '{date}')
   OR
   (TO_CHAR(score.entrydate, 'yyyymmdd') = '{date}' AND score.entrytime <= '{cutoff}'))
  AND
  gg.stype = 'EQA'
"""

CMD2 = """
SELECT
  report.stock_code, report.organ_id, cmb.type_id, report.report_id, report.forecast_year, TO_CHAR(report.current_create_date, 'yyyymmdd'), TO_CHAR(report.previous_create_date, 'yyyymmdd'), report.current_forecast_profit, report.previous_forecast_profit, report.current_forecast_eps, report.previous_forecast_eps, report.profit_adjust_flag
FROM
  Cmb_Report_Adjust report JOIN Cmb_Report_Research cmb ON report.report_id = cmb.id JOIN GG_SecurityCode gg ON gg.symbol = report.stock_code
WHERE
  ((TO_CHAR(report.entrydate, 'yyyymmdd') = '{prev_date}' AND report.entrytime > '{cutoff}')
   OR
   (TO_CHAR(report.entrydate, 'yyyymmdd') > '{prev_date}' AND TO_CHAR(report.entrydate, 'yyyymmdd') < '{date}')
   OR
   (TO_CHAR(report.entrydate, 'yyyymmdd') = '{date}' AND report.entrytime <= '{cutoff}'))
  AND
  gg.stype = 'EQA'
ORDER BY
  report.stock_code, report.organ_id, report.forecast_year
"""

dnames1 = ['report_type', 'report_id', 'previous_report_date', 'score', 'previous_score', 'score_adjust_flag']
_dnames2 = ['forecast_profit', 'previous_forecast_profit', 'forecast_eps', 'previous_forecast_eps', 'profit_adjust_flag']
dnames2 = ['report_type', 'report_id', 'previous_report_date']

for i, _dname in itertools.product(range(3), _dnames2):
    dnames2.append(_dname+'_'+str(i))
