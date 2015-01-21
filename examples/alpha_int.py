"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.interval import AdjIntervalFetcher
from orca.alpha.base import BacktestingIntervalAlpha

adj = AdjIntervalFetcher('5min')


class MyAlpha(BacktestingIntervalAlpha):

    def generate(self, date, time):
        self[(date, time)] = adj.fetch_intervals('adj_returns', date, time, offset=self.offset)

if __name__ == '__main__':
    from orca.mongo.components import ComponentsFetcher
    from orca.operation.api import intersect_interval

    start, end = '20140103', '20140131'
    alpha = MyAlpha('30min', offset=3)
    alpha.run(start, end)
    alpha = alpha.get_alphas()

    comp = ComponentsFetcher(as_bool=True, datetime_index=True, reindex=True)
    HS300 = comp.fetch('HS300', start, end)

    alpha_hs300 = intersect_interval(alpha, HS300)
    alpha_hs300.to_csv('alpha_int.csv')
