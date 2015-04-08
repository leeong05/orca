"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca.mongo.components import ComponentsFetcher
components_fetcher = ComponentsFetcher(as_bool=False)

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
        hs300 = components_fetcher.fetch_daily('HS300', date)
        hs300 = set(hs300[hs300>0].index)
        cs500 = components_fetcher.fetch_daily('CS500', date)
        cs500 = set(cs500[cs500>0].index)

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
            for q, prefix in zip((0.95, 0.90, 0.8), ('t5p', 't10p', 't20p')):
                while True:
                    sids = ser[ser >= ser.quantile(q)].index
                    if len(sids):
                        break
                    q -= 0.01
                hs300_sids = [sid for sid in sids if sid in hs300]
                cursor.execute(SQL1, (date, dname, prefix+'_hs300'))
                if list(cursor):
                    cursor.execute(SQL2, (len(hs300_sids)*1./len(sids), date, dname, prefix+'_hs300'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_hs300', len(hs300_sids)*1./len(sids)))
                cs500_sids = [sid for sid in sids if sid in cs500]
                cursor.execute(SQL1, (date, dname, prefix+'_cs500'))
                if list(cursor):
                    cursor.execute(SQL2, (len(cs500_sids)*1./len(sids), date, dname, prefix+'_cs500'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_cs500', len(cs500_sids)*1./len(sids)))
                cyb_sids = [sid for sid in sids if sid.startswith('30')]
                cursor.execute(SQL1, (date, dname, prefix+'_cyb'))
                if list(cursor):
                    cursor.execute(SQL2, (len(cyb_sids)*1./len(sids), date, dname, prefix+'_cyb'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_cyb', len(cyb_sids)*1./len(sids)))
                zxb_sids = [sid for sid in sids if sid.startswith('002')]
                cursor.execute(SQL1, (date, dname, prefix+'_zxb'))
                if list(cursor):
                    cursor.execute(SQL2, (len(zxb_sids)*1./len(sids), date, dname, prefix+'_zxb'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_zxb', len(zxb_sids)*1./len(sids)))
            for q, prefix in zip((0.05, 0.10, 0.2), ('b5p', 'b10p', 'b20p')):
                while True:
                    sids = ser[ser <= ser.quantile(q)].index
                    if len(sids):
                        break
                    q += 0.01
                hs300_sids = [sid for sid in sids if sid in hs300]
                cursor.execute(SQL1, (date, dname, prefix+'_hs300'))
                if list(cursor):
                    cursor.execute(SQL2, (len(hs300_sids)*1./len(sids), date, dname, prefix+'_hs300'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_hs300', len(hs300_sids)*1./len(sids)))
                cs500_sids = [sid for sid in sids if sid in cs500]
                cursor.execute(SQL1, (date, dname, prefix+'_cs500'))
                if list(cursor):
                    cursor.execute(SQL2, (len(cs500_sids)*1./len(sids), date, dname, prefix+'_cs500'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_cs500', len(cs500_sids)*1./len(sids)))
                cyb_sids = [sid for sid in sids if sid.startswith('30')]
                cursor.execute(SQL1, (date, dname, prefix+'_cyb'))
                if list(cursor):
                    cursor.execute(SQL2, (len(cyb_sids)*1./len(sids), date, dname, prefix+'_cyb'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_cyb', len(cyb_sids)*1./len(sids)))
                zxb_sids = [sid for sid in sids if sid.startswith('002')]
                cursor.execute(SQL1, (date, dname, prefix+'_zxb'))
                if list(cursor):
                    cursor.execute(SQL2, (len(zxb_sids)*1./len(sids), date, dname, prefix+'_zxb'))
                else:
                    cursor.execute(SQL3, (date, dname, prefix+'_zxb', len(zxb_sids)*1./len(sids)))
            for q, statistic in zip((0.95, 0.90), ('pt5p_mean', 'pt10p_mean')):
                psids = pser[pser >= pser.quantile(q)].index
                tser = rser.ix[psids]
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.sql_float(tser.mean()), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.sql_float(tser.mean())))
            for q, statistic in zip((0.05, 0.10), ('pb5p_mean', 'pb10p_mean')):
                psids = pser[pser <= pser.quantile(q)].index
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
