import logging

logger = logging.getLogger('updater')

from datetime import datetime
from decimal import Decimal

import pandas as pd

from base import UpdaterBase
import jybs_sql
import jycs_sql
import jyis_sql

"""
The updater class for collections 'jybs', 'jycs', 'jyis'
"""

def quarter(dstr):
    md = dstr[4:8]
    if md == '0331':
        return 1
    elif md == '0630':
        return 2
    elif md == '0930':
        return 3
    elif md == '1231':
        return 4


class JYFundUpdater(UpdaterBase):

    def __init__(self, timeout=30, table='balancesheet'):
        UpdaterBase.__init__(self, timeout)
        self.table = table
        self.cutoff = '03:00'
        self.basedate = datetime(2000, 1, 1)

    def get_milliseconds(self, date):
        date = datetime(int(date[:4]), int(date[4:6]), int(date[6:8]),
                int(self.cutoff[:2]), int(self.cutoff[-2:]))
        delta = date - self.basedate
        return (delta.days * 24 * 3600 + delta.seconds) * 1000 + delta.microseconds / 1000

    def pre_update(self):
        self.connect_jydb()
        self.__dict__.update({'dates': self.db.dates.distinct('date')})
        if self.table == 'balancesheet':
            self.__dict__.update({'sql': jybs_sql, 'collection': self.db.jybs})
        elif self.table == 'cashflow':
            self.__dict__.update({'sql': jycs_sql, 'collection': self.db.jycs})
        else:
            self.__dict__.update({'sql': jyis_sql, 'collection': self.db.jyis})

        self.cursor.execute(self.sql.CMD0)
        self.__dict__.update({'company_sid': {company: sid for company, sid in list(self.cursor)}})

    def pro_update(self):
        return

        logger.debug('Ensuring index date_1_year_1_quarter_1 on collection %s', self.collection.name)
        self.collection.ensure_index([('date', 1), ('year', 1), ('quarter', 1)],
                background = True)
        logger.debug('Ensuring index year_1_quarter_1_date_1 on collection %s', self.collection.name)
        self.collection.ensure_index([('year', 1), ('quarter', 1), ('date', 1)],
                background=True)

    def update(self, date):
        prev_date = self.dates[self.dates.index(date)-1]
        CMD = self.sql.CMD.format(date=self.get_milliseconds(date),
                                  prev_date=self.get_milliseconds(prev_date))
        logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            logger.warning('No records found for %s on %s', self.collection.name, prev_date)
            return

        df = df[self.sql.cols]
        df.columns = ['sid', 'enddate'] + self.sql.dnames
        df = df.ix[[x in self.company_sid for x in df.sid]]
        df.enddate = df.enddate.apply(lambda x: x.strftime('%Y%m%d'))
        year = df.enddate.apply(lambda x: int(x[:4]))
        qtr = df.enddate.apply(lambda x: quarter(x))
        df = df.ix[qtr > 0]
        df['year'], df['quarter'] = year[qtr > 0], qtr[qtr > 0]
        df.sid = df.sid.apply(lambda x: self.company_sid[x])
        df['date'] = prev_date
        if len(df) == 0:
            logger.warning('No records found for %s on %s', self.collection.name, prev_date)
            return

        for _, row in df.iterrows():
            key = {'date': prev_date, 'year': row['year'], 'quarter': row['quarter'], 'sid': row['sid']}
            doc = {}
            for k, v in row.iteritems():
                if v is None: continue
                if isinstance(v, Decimal):
                    doc[k] = float(str(v))
                else:
                    doc[k] = v
            self.collection.update(key, doc, upsert=True)
        logger.info('UPSERT documents for %d sids into (c: [%s]) of (d: [%s]) on %s', len(df['sid'].unique()), self.collection.name, self.db.name, prev_date)


class JYBalancesheetUpdater(JYFundUpdater):

    def __init__(self, table='balancesheet', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


class JYCashflowUpdater(JYFundUpdater):

    def __init__(self, table='cashflow', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


class JYIncomeUpdater(JYFundUpdater):

    def __init__(self, table='income', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


if __name__ == '__main__':
    jybs = JYBalancesheetUpdater()
    jycs = JYCashflowUpdater()
    jyis = JYIncomeUpdater()

    jybs.run()
    jycs.run()
    jyis.run()
