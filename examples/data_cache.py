"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.quote import QuoteFetcher
from orca.data.csv import CSVSaver

quote = QuoteFetcher()
close = quote.fetch('close', '20140101', '20140131')

saver = CSVSaver('cache')
saver['close'] = close
