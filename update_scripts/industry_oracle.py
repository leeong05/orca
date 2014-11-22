standards = {
  3:    'ZX',
  24:   'SW2014',
}

CMD1 = """
SELECT
  sm.SecuCode, TO_CHAR(ind.FirstIndustryCode), TO_CHAR(ind.FirstIndustryName), TO_CHAR(ind.SecondIndustryCode), TO_CHAR(ind.SecondIndustryName), TO_CHAR(ind.ThirdIndustryCode), TO_CHAR(ind.ThirdIndustryName)
FROM
  SecuMain sm JOIN LC_ExgIndustry ind ON sm.CompanyCode = ind.CompanyCode
WHERE
  sm.SecuCategory = 1
  AND
  (sm.SecuMarket = 83 or sm.SecuMarket = 90)
  AND
  SUBSTR(sm.SecuCode, 1, 2) IN ('60', '00', '30')
  AND
  ind.Standard = {standard}
  AND
  ind.InfoPublDate <= TO_DATE({date}, 'yyyymmdd') AND (ind.CancelDate IS NULL OR ind.CancelDate > TO_DATE({date}, 'yyyymmdd'))
"""

CMD2 = """
SELECT
  TO_CHAR(ii.IndustryCode), sm.SecuCode
FROM
  SecuMain sm JOIN LC_CorrIndexIndustry ii ON sm.InnerCode = ii.IndexCode
WHERE
  (ii.EndDate IS NULL OR ii.EndDate >= TO_DATE({date}, 'yyyymmdd'))
  AND
  ii.IndustryStandard = {standard}
  AND
  ii.IndustryCode IS NOT NULL
"""

dnames_industry = ['level1', 'level2', 'level3']
dnames_info = ['industry_name', 'level1_name', 'level2_name', 'level3_name', 'industry_index', 'level1_index', 'level2_index', 'level3_index']
