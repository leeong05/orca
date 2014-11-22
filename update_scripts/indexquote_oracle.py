CMD = """
SELECT
  sm.SecuCode, sm.SecuMarket, qt.PrevClosePrice, qt.OpenPrice,
  qt.HighPrice, qt.LowPrice, qt.ClosePrice, qt.TurnoverVolume, qt.TurnoverValue,
  qt.TurnoverValue / qt.TurnoverVolume, qt.ClosePrice / qt.PrevClosePrice - 1.
FROM
  QT_IndexQuote qt JOIN SecuMain sm ON sm.InnerCode = qt.InnerCode
WHERE
  qt.TradingDay = TO_DATE({date}, 'yyyymmdd')
  AND
  qt.TurnoverVolume > 0
  AND
  qt.PrevClosePrice > 0
  AND
  sm.SecuCategory = 4
  AND
  sm.SecuMarket IN (83, 90)
  AND
  LENGTH(sm.SecuCode) = 6
  AND
  SUBSTR(sm.SecuCode, 1, 1) IN ('3', '0')
"""

dnames = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'amount', 'vwap', 'returns']
