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


def generate_params3():
    for i in xrange(10):
        yield {'n': i}

parallel.run_filter_hdf('temp3.h5',
        AlphaDummy,
        generate_params3(),
        20140101,
        20140131,
        '{}.get_original().get_ir() > 0.5')
