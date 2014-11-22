CMD = """
SELECT
  sm.SecuCode, qt.PrevClosePrice, qt.OpenPrice,
  qt.HighPrice, qt.LowPrice, qt.ClosePrice, qt.TurnoverVolume, qt.TurnoverValue,
  qt.TurnoverValue / qt.TurnoverVolume, qt.ClosePrice / qt.PrevClosePrice - 1.
FROM
  QT_DailyQuote qt JOIN SecuMain sm ON qt.InnerCode = sm.InnerCode
WHERE
  qt.TradingDay = TO_DATE({date}, 'yyyymmdd')
  AND
  qt.TurnoverVolume > 0
  AND
  sm.SecuCategory = 1
  AND
  sm.SecuMarket IN (83, 90)
  AND
  SUBSTR(sm.SecuCode, 1, 2) IN ('60', '00', '30')
"""

dnames = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'amount', 'vwap', 'returns']
