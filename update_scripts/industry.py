"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.UTF8'

import pandas as pd

from base import UpdaterBase
import industry_mssql
import industry_oracle


class IndustryUpdater(UpdaterBase):
    """The updater class for collection 'industry', 'industry_info'."""

    def __init__(self, source=None, timeout=10):
        self.source = source
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.industry
        if not self.skip_update:
            self.connect_jydb()
            if self.source == 'mssql':
                self.sql = industry_mssql
            elif self.source == 'oracle':
                self.sql = industry_oracle
        if not self.skip_monitor:
            if not hasattr(self, 'sql'):
                self.sql = industry_oracle
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update industry classification, industry-name/level correspondance for the **same** day before market open."""
        for key, val in self.sql.standards.iteritems():
            self._update(date, key, val)

    def _update(self, date, standard, sname):
        if standard == 24 and date < '20140101':
            CMD = self.sql.CMD1.format(date='20140101', standard=standard)
        else:
            CMD = self.sql.CMD1.format(date=date, standard=standard)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(self.sql.CMD1.format(date='20140101', standard=standard))
        DF = pd.DataFrame(list(self.cursor))
        if len(DF) == 0:
            self.logger.warning('No records found for {}[standard={}] on {}', self.db.industry.name, sname, date)
            return

        DF[[0, 1, 3, 5]] = DF[[0, 1, 3, 5]].astype(str)
        df = DF[[0, 1, 3, 5]]
        l1_name, l2_name, l3_name, ind_name = {}, {}, {}, {}
        for _, row in DF.iterrows():
            l1, n1, l2, n2, l3, n3 = row[1:]
            l1_name[l1], ind_name[l1] = n1, n1
            l2_name[l2], ind_name[l2] = n2, n2
            l3_name[l3], ind_name[l3] = n3, n3

        df.columns = ['sid'] + self.sql.dnames_industry
        df.index = df.sid

        for dname in self.sql.dnames_industry:
            key = {'standard': sname, 'dname': dname, 'date': date}
            self.collection.update(key, {'$set': {'dvalue': df[dname].to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@standard={}]) of (d: [{}]) on {}', len(df), self.db.industry.name, sname, self.db.name, date)

        CMD = self.sql.CMD2.format(date=date, standard=standard)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        l1_index, l2_index, l3_index, ind_index = {}, {}, {}, {}
        for row in self.cursor:
            if row[1] is None:
                continue
            ind, index = str(row[0]), str(row[1])
            if ind in ind_name:
                ind_index[ind] = index
            if ind in l1_name:
                l1_index[ind] = index
            if ind in l2_name:
                l2_index[ind] = index
            if ind in l3_name:
                l3_index[ind] = index

        f = lambda dname, dvalue: \
                self.db.industry_info.update(
                        {'standard': sname, 'dname': dname, 'date': date},
                        {'$set': {'dvalue': dvalue}},
                        upsert=True)
        f('industry_name', ind_name)
        f('level1_name',   l1_name)
        f('level2_name',   l2_name)
        f('level3_name',   l3_name)
        self.logger.info('UPSERT documents for {} industries into (c: [{}@standard={}]) of (d: [{}]) on {}', len(ind_name), self.db.industry_info.name, sname, self.db.name, date)

        f('industry_index', ind_index)
        f('level1_index',   l1_index)
        f('level2_index',   l2_index)
        f('level3_index',   l3_index)
        self.logger.info('UPSERT documents for {} industry-indice into (c: [{}@standard={}]) of (d: [{}]) on {}', len(ind_index), self.db.industry_info.name, sname, self.db.name, date)

    def monitor(self, date):
        for standard in self.sql.standards.values():
            self._monitor(date, standard)

    def _monitor(self, date, standard):
        SQL1 = "SELECT * FROM mongo_industry WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_industry SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_industry (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distinct('dname'):
            ser = pd.Series(self.collection.find_one({'standard': standard, 'dname': dname, 'date': date})['dvalue']).dropna()
            dname = dname+'_'+standard

            cursor.execute(SQL1, (date, dname, 'count'))
            if list(cursor):
                cursor.execute(SQL2, (self.compute_statistic(ser, 'count'), date, dname, 'count'))
            else:
                cursor.execute(SQL3, (date, dname, 'count', self.compute_statistic(ser, 'count')))

            groups = ser.groupby(ser)

            cursor.execute(SQL1, (date, dname, 'number'))
            if list(cursor):
                cursor.execute(SQL2, (len(groups), date, dname, 'number'))
            else:
                cursor.execute(SQL3, (date, dname, 'number', len(groups)))

            counts = groups.apply(lambda x: len(x))

            cursor.execute(SQL1, (date, dname, 'min'))
            if list(cursor):
                cursor.execute(SQL2, (counts.min(), date, dname, 'min'))
            else:
                cursor.execute(SQL3, (date, dname, 'min', counts.min()))

            cursor.execute(SQL1, (date, dname, 'max'))
            if list(cursor):
                cursor.execute(SQL2, (counts.max(), date, dname, 'max'))
            else:
                cursor.execute(SQL3, (date, dname, 'max', counts.max()))

            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    ind = IndustryUpdater()
    ind.run()
