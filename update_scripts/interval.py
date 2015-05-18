"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca.mongo.interval import IntervalFetcher
from orca.utils.regression import get_slope
interval_5min = IntervalFetcher('5min')

from base import UpdaterBase


class IntervalUpdater(UpdaterBase):
    """The updater class for collection 'interval_derivative'."""

    def __init__(self, timeout=3600):
        super(IntervalUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.interval_derivative
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update interval derivative data for the **same** day after market open."""
        self.update_amount(date)

    def update_amount(self, date):
        def func1(s):
            s = s.copy()
            s.sort()
            try:
                return get_slope(np.log(s))
            except:
                return np.nan
        def func2(s):
            s = s.copy()
            try:
                return get_slope(np.log(s))
            except:
                return np.nan
        amount5 = interval_5min.fetch_daily('amount', [], date)
        amount5_1, amount5_2 = amount5.iloc[:24, :], amount5.iloc[24:, :]

        a1, a2 = amount5_1.div(amount5_1.sum(axis=0), axis=1), amount5_2.div(amount5_2.sum(axis=0), axis=1)
        s1, s2 = a1.apply(func1), a2.apply(func1)
        self.collection.update({'dname': 'amount5_sorted_slope1', 'date': date}, {'$set': {'dvalue': s1.ix[np.isfinite(s1)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s1).sum(), self.collection.name, 'amount5_sorted_slope1', self.db.name, date)
        self.collection.update({'dname': 'amount5_sorted_slope2', 'date': date}, {'$set': {'dvalue': s2.ix[np.isfinite(s2)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s2).sum(), self.collection.name, 'amount5_sorted_slope2', self.db.name, date)

        a1, a2 = amount5_1.div(amount5_1.sum(axis=0), axis=1), amount5_2.div(amount5_2.sum(axis=0), axis=1)
        a1 = a1.iloc[:,::-1]
        s1, s2 = a1.apply(func2), a2.apply(func2)
        self.collection.update({'dname': 'amount5_slope1', 'date': date}, {'$set': {'dvalue': s1.ix[np.isfinite(s1)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s1).sum(), self.collection.name, 'amount5_slope1', self.db.name, date)
        self.collection.update({'dname': 'amount5_slope2', 'date': date}, {'$set': {'dvalue': s2.ix[np.isfinite(s2)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s2).sum(), self.collection.name, 'amount5_slope2', self.db.name, date)

        a = amount5.div(amount5.sum(axis=0), axis=1)
        s = a.kurt()
        self.collection.update({'dname': 'amount5_kurt', 'date': date}, {'$set': {'dvalue': s.ix[np.isfinite(s)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s).sum(), self.collection.name, 'amount5_kurt', self.db.name, date)

    def monitor(self, date):
        return
        statistics = ('count',)
        SQL1 = "SELECT * FROM mongo_universe WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_universe SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_universe (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distinct('dname'):
            try:
                ser = pd.Series(self.collection.find_one({'dname': dname, 'date': date})['dvalue'])
            except:
                continue
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    interval = IntervalUpdater()
    interval.run()
