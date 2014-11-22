CMD1 = """
SELECT
  sm1.SecuCode, sm1.SecuMarket, sm2.SecuCode
FROM
  LC_IndexComponent ic JOIN SecuMain sm1 ON ic.IndexInnerCode = sm1.InnerCode
  JOIN SecuMain sm2 ON ic.SecuInnerCode = sm2.InnerCode
WHERE
  ic.InDate <= CONVERT(DATE, '{date}')
  AND
  (ic.OutDate IS NULL OR ic.OutDate > CONVERT(DATE, '{date}'))
  AND
  sm1.SecuMarket IN (83, 90)
  AND
  sm1.SecuCategory = 4
  AND
  sm2.SecuCategory = 1
  AND
  sm2.SecuMarket IN (83, 90)
  AND
  LEFT(sm2.SecuCode, 2) IN ('60', '00', '30')
  AND
  LEN(sm1.SecuCode) = 6
  AND
  LEFT(sm1.SecuCode, 1) IN ('3', '0')
"""

CMD2 = """
SELECT
  sm1.SecuCode, sm1.SecuMarket, sm2.SecuCode, cw.Weight
FROM
  SecuMain sm1 JOIN LC_IndexComponentsWeight cw ON sm1.InnerCode = cw.IndexCode
  JOIN SecuMain sm2 ON sm2.InnerCode = cw.InnerCode
WHERE
  cw.EndDate = (SELECT MAX(EndDate)
                FROM LC_IndexComponentsWeight
                WHERE IndexCode = cw.IndexCode AND EndDate <= CONVERT(DATE, '{date}'))
  AND
  sm1.SecuMarket IN (83, 90)
  AND
  sm1.SecuCategory = 4
  AND
  sm2.SecuCategory = 1
  AND
  sm2.SecuMarket IN (83, 90)
  AND
  LEFT(sm2.SecuCode, 2) IN ('60', '00', '30')
  AND
  LEN(sm1.SecuCode) = 6
  AND
  LEFT(sm1.SecuCode, 1) IN ('3', '0')
"""
