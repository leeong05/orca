import os
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.UTF8'

import logging

logger = logging.getLogger('updater')

import pandas as pd

from base import UpdaterBase
import industry_sql

"""
The updater class for collection 'industry', 'industry_info'
"""

class IndustryUpdater(UpdaterBase):

    def __init__(self, timeout=10):
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.connect_jydb()
        self.__dict__.update({'dates': self.db.dates.distinct('date')})

    def pro_update(self):
        return

        logger.debug('Ensuring index standard_1_date_1_dname_1 on collection industry')
        self.db.industry.ensure_index([('standard', 1), ('date', 1), ('dname', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index standard_1_dname_1_date_1 on collection industry')
        self.db.industry.ensure_index([('standard', 1), ('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index standard_1_date_1_dname_1 on collection industry_info')
        self.db.industry_info.ensure_index([('standard', 1), ('date', 1), ('dname', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index standard_1_dname_1_date_1 on collection industry_info')
        self.db.industry_info.ensure_index([('standard', 1), ('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        for key, val in industry_sql.standards.iteritems():
            self._update(date, key, val)

    def _update(self, date, standard, sname):
        if standard == 24 and date < '20140101':
            CMD = industry_sql.CMD1.format(date='20140101', standard=standard)
        else:
            CMD = industry_sql.CMD1.format(date=date, standard=standard)
        logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(industry_sql.CMD1.format(date='20140101', standard=standard))
        DF = pd.DataFrame(list(self.cursor))
        if len(DF) == 0:
            logger.warning('No records found for %s[standard=%s] on %s', self.db.industry.name, sname, date)
            return

        DF[[0, 1, 3, 5]] = DF[[0, 1, 3, 5]].astype(str)
        df = DF[[0, 1, 3, 5]]
        l1_name, l2_name, l3_name, ind_name = {}, {}, {}, {}
        for _, row in DF.iterrows():
            l1, n1, l2, n2, l3, n3 = row[1:]
            l1_name[l1], ind_name[l1] = n1, n1
            l2_name[l2], ind_name[l2] = n2, n2
            l3_name[l3], ind_name[l3] = n3, n3

        df.columns = ['sid'] + industry_sql.dnames_industry
        df.index = df.sid

        for dname in industry_sql.dnames_industry:
            key = {'standard': sname, 'dname': dname, 'date': date}
            self.db.industry.update(key, {'$set': {'dvalue': df[dname].to_dict()}}, upsert=True)
        logger.info('UPSERT documents for %d sids into (c: [%s@standard=%s]) of (d: [%s]) on %s', len(df), self.db.industry.name, sname, self.db.name, date)

        CMD = industry_sql.CMD2.format(date=date, standard=standard)
        logger.debug('Executing command:\n%s', CMD)
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
                        {'standard': standard, 'dname': dname, 'date': date},
                        {'$set': {'dvalue': dvalue}},
                        upsert=True)
        f('industry_name', ind_name)
        f('level1_name',   l1_name)
        f('level2_name',   l2_name)
        f('level3_name',   l3_name)
        logger.info('UPSERT documents for %d industries into (c: [%s@standard=%s]) of (d: [%s]) on %s', len(ind_name), self.db.industry_info.name, sname, self.db.name, date)

        f('industry_index', ind_index)
        f('level1_index',   l1_index)
        f('level2_index',   l2_index)
        f('level3_index',   l3_index)
        logger.info('UPSERT documents for %d industry-indice into (c: [%s@standard=%s]) of (d: [%s]) on %s', len(ind_index), self.db.industry_info.name, sname, self.db.name, date)

if __name__ == '__main__':
    ind = IndustryUpdater()
    ind.run()
