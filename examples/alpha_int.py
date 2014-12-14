"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.interval import AdjIntervalFetcher
from orca.alpha.base import BacktestingIntervalAlpha

adj = AdjIntervalFetcher('5min')


class MyAlpha(BacktestingIntervalAlpha):

    def generate(self, date, time):
        self[(date, time)] = adj.fetch_intervals('adj_returns', date, time, offset=1)

if __name__ == '__main__':
    start, end = '20140103', '20140131'
    alpha = MyAlpha('5min')
    alpha.run(start, end)
    alpha.dump('alpha_int.csv')
