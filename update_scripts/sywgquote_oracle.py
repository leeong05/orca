CMD = """
SELECT
  sm.SecuCode, qt.PrevClosePrice, qt.OpenPrice,
  qt.HighPrice, qt.LowPrice, qt.ClosePrice, qt.TurnoverVolume, qt.TurnoverValue,
  qt.TurnoverValue / qt.TurnoverVolume, qt.ClosePrice / qt.PrevClosePrice - 1.
FROM
  QT_SYWGIndexQuote qt JOIN SecuMain sm ON sm.InnerCode = qt.InnerCode
WHERE
  qt.TradingDay = TO_DATE({date}, 'yyyymmdd')
  AND
  qt.TurnoverVolume > 0
"""

dnames = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'amount', 'vwap', 'returns']
