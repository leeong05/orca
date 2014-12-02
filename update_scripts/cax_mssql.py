CMD0 = """
SELECT
  MAX(CONVERT(VARCHAR(8), TradingDate, 112))
FROM
  QT_TradingDayNew
WHERE
  IfTradingDay = 1
  AND
  SecuMarket = 83
  AND
  TradingDate <= CONVERT(DATE, '{date}')
"""

CMD1 = """
WITH summary AS (
  SELECT
    sm.SecuCode, adj.RatioAdjustingFactor, adj.AccuBonusShareRatio,
    ROW_NUMBER() OVER(PARTITION BY sm.SecuCode ORDER BY adj.ExDiviDate DESC) AS rk
  FROM
    SecuMain sm INNER JOIN QT_AdjustingFactor adj ON sm.InnerCode = adj.InnerCode
  WHERE
    adj.ExDiviDate <= CONVERT(DATE, '{date}')
    AND
    sm.SecuCategory = 1
    AND
    sm.SecuMarket IN (83, 90)
    AND
    LEFT(sm.SecuCode, 2) IN ('60', '00', '30'))
SELECT s.SecuCode, s.RatioAdjustingFactor, s.AccuBonusShareRatio
FROM summary s
WHERE s.rk = 1
"""

CMD2 = """
WITH summary AS (
  SELECT
    sm.SecuCode, ss.Ashares, ss.AFloats, ss.RestrictedAShares, ss.NonRestrictedShares, ss.FloatShare, ss.AFloatListed,
    ROW_NUMBER() OVER(PARTITION BY sm.SecuCode ORDER BY ss.EndDate DESC, ss.InfoPublDate) AS rk
  FROM
    SecuMain sm INNER JOIN LC_ShareStru ss ON sm.CompanyCode = ss.CompanyCode
  WHERE
    ss.InfoPublDate <= CONVERT(DATE, '{date}')
    AND
    ss.EndDate <= CONVERT(DATE, '{date}')
    AND
    sm.SecuCategory = 1
    AND
    sm.SecuMarket IN (83, 90)
    AND
    LEFT(sm.SecuCode, 2) IN ('60', '00', '30'))
SELECT
  s.SecuCode, s.Ashares, s.AFloats, s.RestrictedAShares, s.NonRestrictedShares
FROM summary s
WHERE s.rk = 1
"""

dnames1 = ['adjfactor', 'volfactor']
dnames2 = ['a_shares', 'a_float', 'a_float_restricted', 'a_float_nonrestricted']
