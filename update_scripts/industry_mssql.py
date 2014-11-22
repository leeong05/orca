standards = {
  3:    'ZX',
  24:   'SW2014',
}

CMD1 = """
SELECT
  sm.SecuCode, ind.FirstIndustryCode, ind.FirstIndustryName, ind.SecondIndustryCode, ind.SecondIndustryName, ind.ThirdIndustryCode, ind.ThirdIndustryName
FROM
  SecuMain sm JOIN LC_ExgIndustry ind ON sm.CompanyCode = ind.CompanyCode
WHERE
  sm.SecuCategory = 1
  AND
  sm.SecuMarket IN (83, 90)
  AND
  LEFT(sm.SecuCode, 2) IN ('60', '00', '30')
  AND
  ind.Standard = {standard}
  AND
  ind.InfoPublDate <= CONVERT(DATE, '{date}') AND (ind.CancelDate IS NULL OR ind.CancelDate > CONVERT(DATE, '{date}'))
"""

CMD2 = """
SELECT
  ii.IndustryCode, sm.SecuCode
FROM
  SecuMain sm JOIN LC_CorrIndexIndustry ii ON sm.InnerCode = ii.IndexCode
WHERE
  (ii.EndDate IS NULL OR ii.EndDate >= CONVERT(DATE, '{date}'))
  AND
  ii.IndustryStandard = {standard}
  AND
  ii.IndustryCode IS NOT NULL
"""

dnames_industry = ['level1', 'level2', 'level3']
dnames_info = ['industry_name', 'level1_name', 'level2_name', 'level3_name', 'industry_index', 'level1_index', 'level2_index', 'level3_index']
