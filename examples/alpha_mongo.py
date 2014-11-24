"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.quote import QuoteFetcher
from orca.alpha.base import BacktestingAlpha

quote = QuoteFetcher()


class MyAlpha(BacktestingAlpha):

    def generate(self, date):
        self[date] = quote.fetch_daily('close', date, offset=1)

if __name__ == '__main__':
    start, end = '20140103', '20140131'
    alpha = MyAlpha()
    alpha.run(start, end)
    alpha.dump('alpha_mongo.csv')
