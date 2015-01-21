"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.data.csv import CSVLoader

loader = CSVLoader('cache')
close = loader['close']

from orca.alpha.base import BacktestingAlpha


class MyAlpha(BacktestingAlpha):

    def __init__(self):
        super(MyAlpha, self).__init__()
        self.close = close.shift(1)

    def generate(self, date):
        self[date] = self.close.ix[date]

if __name__ == '__main__':
    start, end = '20140103', '20140131'
    alpha = MyAlpha()
    alpha.run(start, end)
    alpha.dump('alpha_cache.csv')
