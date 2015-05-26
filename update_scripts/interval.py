"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm

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
        T5 = range(-24, 0) + range(1, 25)
        def func3(s, i=1):
            t = pd.Series(T5, index=s.index) ** 2
            s, t = s.ix[np.isfinite(s)], t.ix[np.isfinite(s)]
            t = sm.add_constant(t)
            try:
                return sm.OLS(s, t).fit().params.iloc[i]
            except:
                return np.nan
        def func4(s, i=1):
            s = pd.Series((-np.sort(-s[:24])).tolist() + np.sort(s[24:]).tolist())
            t = pd.Series(T5, index=s.index) ** 2
            s, t = s.ix[np.isfinite(s)], t.ix[np.isfinite(s)]
            t = sm.add_constant(t)
            try:
                return sm.OLS(s, t).fit().params.iloc[i]
            except:
                return np.nan
        def func5(s, i=2):
            t = pd.Series(T5, index=s.index)
            t = pd.concat([t, t ** 2], axis=1)
            s, t = s.ix[np.isfinite(s)], t.ix[np.isfinite(s)]
            t = sm.add_constant(t)
            try:
                return sm.OLS(s, t).fit().params.iloc[i]
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

        a1, a2 = amount5_1.div(amount5.sum(axis=0), axis=1), amount5_2.div(amount5.sum(axis=0), axis=1)
        s1, s2 = a1.apply(func1), a2.apply(func1)
        self.collection.update({'dname': 'amount5_norm_sorted_slope1', 'date': date}, {'$set': {'dvalue': s1.ix[np.isfinite(s1)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s1).sum(), self.collection.name, 'amount5_norm_sorted_slope1', self.db.name, date)
        self.collection.update({'dname': 'amount5_norm_sorted_slope2', 'date': date}, {'$set': {'dvalue': s2.ix[np.isfinite(s2)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s2).sum(), self.collection.name, 'amount5_norm_sorted_slope2', self.db.name, date)

        a1, a2 = amount5_1.div(amount5_1.sum(axis=0), axis=1), amount5_2.div(amount5_2.sum(axis=0), axis=1)
        a1 = a1.iloc[:,::-1]
        s1, s2 = a1.apply(func2), a2.apply(func2)
        self.collection.update({'dname': 'amount5_slope1', 'date': date}, {'$set': {'dvalue': s1.ix[np.isfinite(s1)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s1).sum(), self.collection.name, 'amount5_slope1', self.db.name, date)
        self.collection.update({'dname': 'amount5_slope2', 'date': date}, {'$set': {'dvalue': s2.ix[np.isfinite(s2)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s2).sum(), self.collection.name, 'amount5_slope2', self.db.name, date)

        a1, a2 = amount5_1.div(amount5.sum(axis=0), axis=1), amount5_2.div(amount5.sum(axis=0), axis=1)
        a1 = a1.iloc[:,::-1]
        s1, s2 = a1.apply(func2), a2.apply(func2)
        self.collection.update({'dname': 'amount5_norm_slope1', 'date': date}, {'$set': {'dvalue': s1.ix[np.isfinite(s1)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s1).sum(), self.collection.name, 'amount5_norm_slope1', self.db.name, date)
        self.collection.update({'dname': 'amount5_norm_slope2', 'date': date}, {'$set': {'dvalue': s2.ix[np.isfinite(s2)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s2).sum(), self.collection.name, 'amount5_norm_slope2', self.db.name, date)

        a = amount5.div(amount5.sum(axis=0), axis=1)
        s = a.kurt()
        self.collection.update({'dname': 'amount5_kurt', 'date': date}, {'$set': {'dvalue': s.ix[np.isfinite(s)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(s).sum(), self.collection.name, 'amount5_kurt', self.db.name, date)

        a = amount5.div(amount5.sum(axis=0), axis=1)
        a1, a2 = a.apply(func3), a.apply(func4)
        a3, a4 = a.apply(func3, i=0), a.apply(func4, i=0)
        self.collection.update({'dname': 'amount5_parabola_symmetric_2nd', 'date': date}, {'$set': {'dvalue': a1.ix[np.isfinite(a1)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(a1).sum(), self.collection.name, 'amount5_parabola_symmetric_2nd', self.db.name, date)
        self.collection.update({'dname': 'amount5_parabola_sorted_symmetric_2nd', 'date': date}, {'$set': {'dvalue': a2.ix[np.isfinite(a2)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(a2).sum(), self.collection.name, 'amount5_parabola_sorted_symmetric_2nd', self.db.name, date)
        self.collection.update({'dname': 'amount5_parabola_symmetric_0th', 'date': date}, {'$set': {'dvalue': a3.ix[np.isfinite(a3)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(a3).sum(), self.collection.name, 'amount5_parabola_symmetric_0th', self.db.name, date)
        self.collection.update({'dname': 'amount5_parabola_sorted_symmetric_0th', 'date': date}, {'$set': {'dvalue': a4.ix[np.isfinite(a4)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(a4).sum(), self.collection.name, 'amount5_parabola_sorted_symmetric_0th', self.db.name, date)

        a = amount5.div(amount5.sum(axis=0), axis=1)
        a0, a1, a2 = a.apply(func5, i=0), a.apply(func5, i=1), a.apply(func5, i=2)
        self.collection.update({'dname': 'amount5_parabola_0th', 'date': date}, {'$set': {'dvalue': a0.ix[np.isfinite(a0)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(a0).sum(), self.collection.name, 'amount5_parabola_0th', self.db.name, date)
        self.collection.update({'dname': 'amount5_parabola_1st', 'date': date}, {'$set': {'dvalue': a1.ix[np.isfinite(a1)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(a1).sum(), self.collection.name, 'amount5_parabola_1st', self.db.name, date)
        self.collection.update({'dname': 'amount5_parabola_2nd', 'date': date}, {'$set': {'dvalue': a2.ix[np.isfinite(a2)].to_dict()}}, upsert=True)
        self.logger.info('UPSERT {} document into (c: [{}@dname={}]) of (d: [{}]) on {}', np.isfinite(a2).sum(), self.collection.name, 'amount5_parabola_2nd', self.db.name, date)

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
