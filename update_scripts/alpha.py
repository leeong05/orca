"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase


class AlphaUpdater(UpdaterBase):
    """The updater class for collection 'alpha'."""

    def __init__(self, timeout=900):
        super(AlphaUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.alpha
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update alpha data for the **same** day before market open."""
        pass

    def monitor(self, date):
        pdate = self.dates[self.dates.index(date)-1]

        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3', 'decile1', 'decile9', 'skew', 'kurt')
        diff_statistics = ('min', 'max', 'quartile1', 'quartile3', 'decile1', 'decile9')
        SQL1 = "SELECT * FROM mongo_alpha WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_alpha SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_alpha (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distinct('dname'):
            try:
                ser = pd.Series(self.collection.find_one({'dname': dname, 'date': date})['dvalue'])
                rser = ser.rank()
                rser = (rser-rser.min())/(rser.max()-rser.min())
                pser = pd.Series(self.collection.find_one({'dname': dname, 'date': pdate})['dvalue'])
                diff = ser-pser
            except:
                continue
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            for q, statistic in zip((0.95, 0.90), ('pt5p_mean', 'pt10p_mean')):
                psids = pser[pser > pser.quantile(q)].index
                tser = rser.ix[psids]
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.sql_float(tser.mean()), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.sql_float(tser.mean())))
            for q, statistic in zip((0.05, 0.10), ('pb5p_mean', 'pb10p_mean')):
                psids = pser[pser < pser.quantile(q)].index
                tser = rser.ix[psids]
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.sql_float(tser.mean()), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.sql_float(tser.mean())))
            for statistic in diff_statistics:
                cursor.execute(SQL1, (date, dname, 'diff_'+statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(diff, statistic), date, dname, 'diff_'+statistic))
                else:
                    cursor.execute(SQL3, (date, dname, 'diff_'+statistic, self.compute_statistic(diff, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    alpha = AlphaUpdater()
    alpha.run()
