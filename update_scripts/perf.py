"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from itertools import product

import numpy as np
import pandas as pd

from orca.perf.performance import Performance
from orca.mongo.kday import UnivFetcher
univ_fetcher = UnivFetcher(datetime_index=True, reindex=True)

from base import UpdaterBase


class PerfUpdater(UpdaterBase):
    """The updater class for collection 'performance'."""

    def __init__(self, timeout=30*60):
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.__dict__.update({
                'dates': self.db.dates.distinct('date'),
                'collection': self.db.performance,
                })

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection {}', self.collection.name)
        self.collection.ensure_index([('dname', 1), ('date', 1)], background=True)

    def update(self, date):
        """Update alpha performance metrics for the **same** day after market close."""
        dnames = self.db.alpha.distinct('dname')
        di = self.dates.index(date)
        BTOP70Q = univ_fetcher.fetch_window('BTOP70Q', self.dates[di-20: di+1])
        cnt = 0
        for dname in dnames:
            cursor = self.db.alpha.find(
                    {'dname': dname, 'date': {'$gte': self.dates[di-20], '$lte': date}},
                    {'_id': 0, 'dvalue': 1, 'date': 1})
            alpha = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
            if date not in alpha.index:
                continue
            perf = Performance(alpha)
            # original
            analyser = perf.get_longshort()
            key = {'alpha': dname, 'mode': 'longshort', 'date': date}
            metrics = self.get_metrics(analyser, date)
            self.collection.update(key, {'$set': metrics}, upsert=True)
            # quantile
            analyser = perf.get_qtail(0.3)
            key = {'alpha': dname, 'mode': 'quantile30', 'date': date}
            metrics = self.get_metrics(analyser, date)
            self.collection.update(key, {'$set': metrics}, upsert=True)
            # universe
            analyser = perf.get_universe(BTOP70Q).get_longshort()
            key = {'alpha': dname, 'mode': 'BTOP70Q', 'date': date}
            metrics = self.get_metrics(analyser, date)
            self.collection.update(key, {'$set': metrics}, upsert=True)
            # top
            analyser = perf.get_qtop(0.3)
            key = {'alpha': dname, 'mode': 'top30', 'date': date}
            metrics = self.get_metrics(analyser, date)
            self.collection.update(key, {'$set': metrics}, upsert=True)
            cnt += 1
        self.logger.info('UPSERT documents for {} alphas into (c: [{}]) of (d: [{}]) on {}',
                cnt, self.collection.name, self.db.name, date)

    def get_metrics(self, analyser, date):
        analyser.get_turnover()
        if date in analyser.turnover.index:
            res = {'turnover': analyser.turnover.ix[date]}
        else:
            res = {'turnover': np.nan}
        for rank, n in product([True, False], [1, 5, 20]):
            key = ('r' if rank else '') + 'IC' + str(n)
            res[key] = analyser.get_ic(n, rank=rank).ix[date]
            key = ('r' if rank else '') + 'AC' + str(n)
            res[key] = analyser.get_ac(n, rank=rank).ix[date]
        res['returns'] = analyser.get_returns().ix[date]
        return res

if __name__ == '__main__':
    perf = PerfUpdater()
    perf.run()
