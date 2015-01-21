CMD0 = """
SELECT
  MAX(TO_CHAR(TradingDate, 'yyyymmdd'))
FROM
  QT_TradingDayNew
WHERE
  IfTradingDay = 1
  AND
  SecuMarket = 83
  AND
  TradingDate <= TO_DATE({date}, 'yyyymmdd')
"""

CMD1 = """
WITH summary AS (
  SELECT
    sm.SecuCode, adj.RatioAdjustingFactor, adj.AccuBonusShareRatio,
    ROW_NUMBER() OVER(PARTITION BY sm.SecuCode ORDER BY adj.ExDiviDate DESC) AS rk
  FROM
    SecuMain sm INNER JOIN QT_AdjustingFactor adj ON sm.InnerCode = adj.InnerCode
  WHERE
    adj.ExDiviDate <= TO_DATE({date}, 'yyyymmdd')
    AND
    sm.SecuCategory = 1
    AND
    sm.SecuMarket IN (83, 90)
    AND
    SUBSTR(sm.SecuCode, 1, 2) IN ('60', '00', '30'))
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
    ss.InfoPublDate <= TO_DATE({date}, 'yyyymmdd')
    AND
    ss.EndDate <= TO_DATE({date}, 'yyyymmdd')
    AND
    sm.SecuCategory = 1
    AND
    sm.SecuMarket IN (83, 90)
    AND
    SUBSTR(sm.SecuCode, 1, 2) IN ('60', '00', '30'))
SELECT
  s.SecuCode, s.Ashares, s.AFloats, s.RestrictedAShares, s.NonRestrictedShares
FROM summary s
WHERE s.rk = 1
"""

dnames1 = ['adjfactor', 'volfactor']
dnames2 = ['a_shares', 'a_float', 'a_float_restricted', 'a_float_nonrestricted']
