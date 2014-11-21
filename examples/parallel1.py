"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.alpha.base import BacktestingAlpha
from orca.mongo.quote import QuoteFetcher
from orca.utils import parallel

close = QuoteFetcher(datetime_index=True, reindex=True).fetch('close', 20140101, 20140131)

class AlphaDummy(BacktestingAlpha):

    def __init__(self, n=None):
        super(AlphaDummy, self).__init__()
        self.n = n

    def generate(self, date):
        self.alphas[date] = close.ix[date]


def generate_params1():
    for i in xrange(10):
        yield {'n': i}

alphas = parallel.run(
        AlphaDummy,
        generate_params1(),
        20140101,
        20140131)

for param, alpha in alphas:
    print param
