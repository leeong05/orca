"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import time

from orca.alpha.base import BacktestingAlpha
from orca.mongo.quote import QuoteFetcher
from orca.utils import parallel

close = QuoteFetcher(datetime_index=True, reindex=True).fetch('close', 20140101, 20140131)

class AlphaDummy(BacktestingAlpha):

    def __init__(self, n):
        super(AlphaDummy, self).__init__()
        self.n = n

    def generate(self, date):
        time.sleep(self.n % 10)
        self.alphas[date] = close.ix[date]


alphas = parallel.run(AlphaDummy, xrange(2), 20140101, 20140131)

for alpha in alphas:
    print alpha.info()
