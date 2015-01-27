"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from itertools import product
from multiprocessing import Process

import numpy as np
import pandas as pd

from orca.perf.performance import Performance
from orca.mongo.kday import UnivFetcher
univ_fetcher = UnivFetcher(datetime_index=True, reindex=True)

from base import UpdaterBase


class PerfUpdater(UpdaterBase):
    """The updater class for collection 'performance'."""

    def __init__(self, timeout=3*60*60):
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

    def add_options(self):
        self.options['alphas'] = None

    def parse_options(self):
        alphas = self.options['alphas']
        if alphas is not None:
            self.options['alphas'] = alphas.split(',')

    def run(self):
        self.parse_args()
        self.parse_options()
        with self.setup:
            if not self.connected:
                self.connect_mongo()
            self.pre_update()
            self._dates = [date for date in self._dates if date in self.dates]
            self.logger.info('START')
            try:
                p = Process(target=self.update, args=(self._dates,))
                p.start()
                p.join(self.timeout)
                if p.is_alive():
                    self.logger.warning('Timeout on date: {}', date)
                    p.terminate()
            except Exception, e:
                self.logger.error('\n{}', e)
            self.logger.info('END\n')
            self.pro_update()
            self.disconnect_mongo()

    def update(self, dates):
        """Update alpha performance metrics for the **same** day after market close."""
        dnames = self.db.alpha.distinct('dname')
        si, ei = map(self.dates.index, [dates[0], dates[-1]])
        BTOP70Q = univ_fetcher.fetch_window('BTOP70Q', self.dates[si-20: ei+1])
        cnt = 0
        for dname in dnames:
            if self.options['alphas'] and dname not in self.options['alphas']:
                continue
            cursor = self.db.alpha.find(
                    {'dname': dname, 'date': {'$gte': self.dates[si-20], '$lte': dates[-1]}},
                    {'_id': 0, 'dvalue': 1, 'date': 1})
            alpha = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
            if len(alpha) == 0:
                continue
            perf = Performance(alpha)
            # original
            analyser = perf.get_longshort()
            for date in dates:
                if date not in alpha.index:
                    continue
                key = {'alpha': dname, 'mode': 'longshort', 'date': date}
                metrics = self.get_metrics(analyser, date)
                self.collection.update(key, {'$set': metrics}, upsert=True)
            # quantile
            analyser = perf.get_qtail(0.3)
            for date in dates:
                if date not in alpha.index:
                    continue
                key = {'alpha': dname, 'mode': 'quantile30', 'date': date}
                metrics = self.get_metrics(analyser, date)
                self.collection.update(key, {'$set': metrics}, upsert=True)
            # universe(s)
            analyser = perf.get_universe(BTOP70Q).get_longshort()
            for date in dates:
                if date not in alpha.index:
                    continue
                key = {'alpha': dname, 'mode': 'BTOP70Q', 'date': date}
                metrics = self.get_metrics(analyser, date)
                self.collection.update(key, {'$set': metrics}, upsert=True)
            # top
            analyser = perf.get_qtop(0.3)
            for date in dates:
                if date not in alpha.index:
                    continue
                key = {'alpha': dname, 'mode': 'top30', 'date': date}
                metrics = self.get_metrics(analyser, date)
                self.collection.update(key, {'$set': metrics}, upsert=True)
            cnt += 1
        if len(dates) == 1:
            self.logger.info('UPSERT documents for {} alphas into (c: [{}]) of (d: [{}]) on {}',
                    cnt, self.collection.name, self.db.name, dates[0])
        else:
            self.logger.info('UPSERT documents for {} alphas into (c: [{}]) of (d: [{}]) from {} to {}',
                    cnt, self.collection.name, self.db.name, dates[0], dates[-1])

    def get_metrics(self, analyser, date):
        analyser.get_turnover()
        if date in analyser.turnover.index:
            res = {'turnover': analyser.turnover.ix[date]}
        else:
            res = {'turnover': np.nan}
        for rank, n in product([True, False], [1, 5, 20]):
            key = ('r' if rank else '') + 'IC' + str(n)
            try:
                res[key] = analyser.get_ic(n, rank=rank).ix[date]
            except:
                res[key] = np.nan
            key = ('r' if rank else '') + 'AC' + str(n)
            try:
                res[key] = analyser.get_ac(n, rank=rank).ix[date]
            except:
                res[key] = np.nan
        try:
            res['returns'] = analyser.get_returns().ix[date]
        except:
            res['returns'] = np.nan
        return res

if __name__ == '__main__':
    perf = PerfUpdater()
    perf.run()
