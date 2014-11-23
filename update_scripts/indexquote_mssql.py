CMD = """
SELECT
  sm.SecuCode, sm.SecuMarket, qt.PrevClosePrice, qt.OpenPrice,
  qt.HighPrice, qt.LowPrice, qt.ClosePrice, qt.TurnoverVolume, qt.TurnoverValue,
  qt.TurnoverValue / qt.TurnoverVolume, qt.ClosePrice / qt.PrevClosePrice - 1.
FROM
  QT_IndexQuote qt JOIN SecuMain sm ON sm.InnerCode = qt.InnerCode
WHERE
  qt.TradingDay = CONVERT(DATE, '{date}')
  AND
  qt.TurnoverVolume > 0
  AND
  qt.PrevClosePrice > 0
  AND
  sm.SecuCategory = 4
  AND
  sm.SecuMarket IN (83, 90)
  AND
  LEN(sm.SecuCOde) = 6
  AND
  (LEFT(sm.SecuCode, 1) IN ('3', '0') OR (LEFT(sm.SecuCode, 2) = 'CN'))
"""

dnames = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'amount', 'vwap', 'returns']
