"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import re
from itertools import product

import pandas as pd

from orca.perf.performance import Performance
from orca.mongo.kday import UnivFetcher, MiscFetcher
univ_fetcher = UnivFetcher(datetime_index=True, reindex=True)
misc_fetcher = MiscFetcher(datetime_index=True, reindex=True)
from orca.mongo.components import ComponentsFetcher
components_fetcher = ComponentsFetcher(datetime_index=True, reindex=True, as_bool=True)

from base import UpdaterBase


class PerformanceUpdater(UpdaterBase):

    def __init__(self, timeout=3*60*60):
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def add_options(self):
        self.options['alphas'] = None
        self.options['pattern'] = None
        self.options['excludes'] = None
        self.options['exclude_pattern'] = None
        self.options['category'] = None

    def parse_options(self):
        alphas = self.options['alphas']
        if alphas is not None:
            self.options['alphas'] = alphas.split(',')
        pattern = self.options['pattern']
        if pattern is not None:
            self.options['pattern'] = re.compile(pattern)
        excludes = self.options['excludes']
        if excludes is not None:
            self.options['excludes'] = excludes.split(',')
        exclude_pattern = self.options['exclude_pattern']
        if exclude_pattern is not None:
            self.options['exclude_pattern'] = re.compile(exclude_pattern)

    def run(self):
        self.parse_args()
        self.parse_options()
        with self.setup:
            if not self.connected:
                self.connect_mongo()
            self.pre_update()
            self._dates = [date for date in self._dates if date in self.dates]
            if not self.skip_monitor:
                self.logger.info('START monitoring')
                self.monitor(self._dates)
                self.logger.info('END monitoring\n')
            self.pro_update()
            self.disconnect_mongo()

    def update(self, *args):
        pass

    def monitor(self, dates):
        """Monitor alpha performance metrics for the **same** day after market close."""
        dnames = self.db.alpha.distinct('dname')
        si, ei = map(self.dates.index, [dates[0], dates[-1]])
        tradable = misc_fetcher.fetch_window('tradable', self.dates[si-20: ei+1]).astype(bool)
        TOP70Q = univ_fetcher.fetch_window('TOP70Q', self.dates[si-20: ei+1])
        CS500 = components_fetcher.fetch_window('CS500', self.dates[si-20: ei+1])

        cursor = self.monitor_connection.cursor()
        SQL1 = "SELECT * FROM alpha_category WHERE name=%s"
        SQL2 = "INSERT INTO alpha_category (name, category) VALUES (%s, %s)"
        for dname in dnames:
            if self.options['alphas'] and dname not in self.options['alphas']:
                continue
            if self.options['excludes'] and dname in self.options['excludes']:
                continue
            if self.options['pattern'] is not None and not self.options['pattern'].search(dname):
                continue
            if self.options['exclude_pattern'] is not None and self.options['exclude_pattern'].search(dname):
                continue
            cursor.execute(SQL1, (dname,))
            if not list(cursor):
                if self.options['category'] is None:
                    category = raw_input('Specify a category for %s: ' % dname)
                else:
                    category = self.options['category']
                cursor.execute(SQL2, (dname, category))
            cursor.execute(SQL1, (dname,))
            alpha_id = list(cursor)[0][0]

            cur = self.db.alpha.find(
                    {'dname': dname, 'date': {'$gte': self.dates[si-21], '$lte': dates[-1]}},
                    {'_id': 0, 'dvalue': 1, 'date': 1})
            alpha = pd.DataFrame({row['date']: row['dvalue'] for row in cur}).T
            if len(alpha) == 0:
                continue

            perf = Performance(alpha)
            self.monitor_alpha(perf.get_universe(tradable), 'tradable', dates, alpha_id, cursor)
            self.monitor_alpha(perf.get_universe(TOP70Q), 'TOP70Q', dates, alpha_id, cursor)
            self.monitor_alpha(perf.get_universe(CS500), 'CS500', dates, alpha_id, cursor)
            self.logger.info('MONITOR for {} from {} to {}', dname, dates[0], dates[-1])

    def upsert(self, alpha_id, date, univ, metric, value, cursor):
        SQL1 = "SELECT * FROM alpha_performance WHERE alpha_id=%s AND trading_day=%s AND universe=%s AND metric=%s"
        SQL2 = "UPDATE alpha_performance SET value=%s WHERE alpha_id=%s AND trading_day=%s AND universe=%s AND metric=%s"
        SQL3 = "INSERT INTO alpha_performance (alpha_id, trading_day, universe, metric, value) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(SQL1, (alpha_id, date, univ, metric))
        if list(cursor):
            cursor.execute(SQL2, (self.sql_float(value), alpha_id, date, univ, metric))
        else:
            cursor.execute(SQL3, (alpha_id, date, univ, metric, self.sql_float(value)))

    def monitor_alpha(self, perf, univ, dates, alpha_id, cursor):
        analyser = perf.get_longshort()
        for date in dates:
            if date not in analyser.alpha.index:
                continue
            self.upsert(alpha_id, date, univ, 'turnover', analyser.get_turnover().ix[date], cursor)
            self.upsert(alpha_id, date, univ, 'returns', analyser.get_returns().ix[date], cursor)
            for rank, n in product([True, False], [1, 5, 21]):
                metric = ('r' if rank else '') + 'IC' + str(n)
                self.upsert(alpha_id, date, univ, metric, analyser.get_ic(n, rank=rank).ix[date], cursor)
        analyser = perf.get_qtail(0.3)
        for date in dates:
            if date not in analyser.alpha.index:
                continue
            self.upsert(alpha_id, date, univ, 'returns_q30', analyser.get_returns().ix[date], cursor)
        analysers = perf.get_quantiles(10)
        for date in dates:
            if date not in analyser.alpha.index:
                continue
            for i, analyser in enumerate(analysers):
                self.upsert(alpha_id, date, univ, 'decile'+str(i+1), analyser.get_returns().ix[date], cursor)
        for i in (1, 5, 10, 21):
            analyser = perf.get_shift(i)
            for date in dates:
                if date not in analyser.alpha.index:
                    continue
                self.upsert(alpha_id, date, univ, 'decay'+str(i), analyser.get_ic(1, rank=True).ix[date], cursor)
        self.monitor_connection.commit()


if __name__ == '__main__':
    perf = PerformanceUpdater()
    perf.run()
