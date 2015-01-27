CMD1 = """
SELECT
  sm.SecuCode, TO_CHAR(ic.EndDate, 'yyyymmdd')
FROM
  SecuMain sm INNER JOIN LC_IncomeStatementALL ic ON sm.CompanyCode = ic.CompanyCode
WHERE
  ic.IfAdjusted = 2
  AND
  ic.IfMerged = 1
  AND
  ic.AccountingStandards = 1
  AND
  ic.InfoPublDate >= TO_DATE({prev_date}, 'yyyymmdd') AND ic.InfoPublDate < TO_DATE({date}, 'yyyymmdd')
"""

CMD1_0 = """
SELECT
  TO_CHAR(ic.InfoPublDate, 'yyyymmdd')
FROM
  SecuMain sm INNER JOIN LC_IncomeStatementALL ic ON sm.CompanyCode = ic.CompanyCOde
WHERE
  sm.SecuCode = '{sid}'
  AND
  ic.EndDate = TO_DATE({enddate}, 'yyyymmdd')
  AND
  ic.InfoPublDate < TO_DATE({date}, 'yyyymmdd')
  AND
  ic.IfAdjusted = 2
  AND
  ic.IfMerged = 1
  AND
  ic.AccountingStandards = 1
"""

CMD2 = """
SELECT
  sm.SecuCode, TO_CHAR(fore.EndDate, 'yyyymmdd')
FROM
  SecuMain sm INNER JOIN LC_PerformanceForecast fore ON sm.CompanyCode = fore.CompanyCode
WHERE
  fore.InfoPublDate >= TO_DATE({prev_date}, 'yyyymmdd') AND fore.InfoPublDate < TO_DATE({date}, 'yyyymmdd')
"""

CMD3 = """
SELECT
  sm.SecuCode, TO_CHAR(pre.EndDate, 'yyyymmdd')
FROM
  SecuMain sm INNER JOIN LC_PerformanceLetters pre ON sm.CompanyCode = pre.CompanyCode
WHERE
  pre.Mark = 2
  AND
  pre.InfoPublDate >= TO_DATE({prev_date}, 'yyyymmdd') AND pre.InfoPublDate < TO_DATE({date}, 'yyyymmdd')
"""
