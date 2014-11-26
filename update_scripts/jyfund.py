"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from datetime import datetime
from decimal import Decimal

import pandas as pd

from base import UpdaterBase
import jybs_mssql
import jycs_mssql
import jyis_mssql
import jyindex_mssql
import jydata_mssql
import jybs_oracle
import jycs_oracle
import jyis_oracle
import jyindex_oracle
import jydata_oracle

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
    """The updater class for collections 'jybs', 'jycs', 'jyis', 'jyindex', 'jydata'."""

    def __init__(self, source=None, timeout=30, table='balancesheet'):
        self.source = source
        self.table = table
        self.cutoff = '03:00'
        self.basedate = datetime(2000, 1, 1)
        UpdaterBase.__init__(self, timeout)

    def get_milliseconds(self, date):
        date = datetime(int(date[:4]), int(date[4:6]), int(date[6:8]),
                int(self.cutoff[:2]), int(self.cutoff[-2:]))
        delta = date - self.basedate
        return (delta.days * 24 * 3600 + delta.seconds) * 1000 + delta.microseconds / 1000

    def pre_update(self):
        self.connect_jydb()
        self.dates = self.db.dates.distinct('date')
        if self.table == 'balancesheet':
            self.collection = self.db.jybs
            if self.source == 'mssql':
                self.sql = jybs_mssql
            elif self.source == 'oracle':
                self.sql = jybs_oracle
        elif self.table == 'cashflow':
            self.collection = self.db.jycs
            if self.source == 'mssql':
                self.sql = jycs_mssql
            elif self.source == 'oracle':
                self.sql = jycs_oracle
        elif self.table == 'income':
            self.collection = self.db.jyis
            if self.source == 'mssql':
                self.sql = jyis_mssql
            elif self.source == 'oracle':
                self.sql = jyis_oracle
        elif self.table == 'index':
            self.collection = self.db.jyindex
            if self.source == 'mssql':
                self.sql = jyindex_mssql
            elif self.source == 'oracle':
                self.sql = jyindex_oracle
        else:
            self.collection = self.db.jydata
            if self.source == 'mssql':
                self.sql = jydata_mssql
            elif self.source == 'oracle':
                self.sql = jydata_oracle

        self.cursor.execute(self.sql.CMD0)
        self.company_sid = {company: sid for company, sid in list(self.cursor)}

    def pro_update(self):
        return

        self.logger.debug('Ensuring index date_1_year_1_quarter_1 on collection %s', self.collection.name)
        self.collection.ensure_index([('date', 1), ('year', 1), ('quarter', 1)],
                background = True)
        self.logger.debug('Ensuring index year_1_quarter_1_date_1 on collection %s', self.collection.name)
        self.collection.ensure_index([('year', 1), ('quarter', 1), ('date', 1)],
                background=True)

    def update(self, date):
        """Update fundamental data for the **previous** day before market open."""
        prev_date = self.dates[self.dates.index(date)-1]
        CMD = self.sql.CMD.format(date=self.get_milliseconds(date),
                                  prev_date=self.get_milliseconds(prev_date))
        self.logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for %s on %s', self.collection.name, prev_date)
            return

        try:
            df = df[self.sql.cols]
            df.columns = ['sid', 'enddate'] + self.sql.dnames
        except:
            df.columns = [desc[0].lower() for desc in self.cursor.description]
        df = df.ix[[x in self.company_sid for x in df.sid]]
        df.enddate = df.enddate.apply(lambda x: x.strftime('%Y%m%d'))
        year = df.enddate.apply(lambda x: int(x[:4]))
        qtr = df.enddate.apply(lambda x: quarter(x))
        df = df.ix[qtr > 0]
        df['year'], df['quarter'] = year[qtr > 0], qtr[qtr > 0]
        df.sid = df.sid.apply(lambda x: self.company_sid[x])
        df['date'] = prev_date
        if len(df) == 0:
            self.logger.warning('No records found for %s on %s', self.collection.name, prev_date)
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
        self.logger.info('UPSERT documents for %d sids into (c: [%s]) of (d: [%s]) on %s', len(df['sid'].unique()), self.collection.name, self.db.name, prev_date)


class JYBalancesheetUpdater(JYFundUpdater):

    def __init__(self, table='balancesheet', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


class JYCashflowUpdater(JYFundUpdater):

    def __init__(self, table='cashflow', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


class JYIncomeUpdater(JYFundUpdater):

    def __init__(self, table='income', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


class JYIndexUpdater(JYFundUpdater):

    def __init__(self, table='index', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


class JYDataUpdater(JYFundUpdater):

    def __init__(self, table='data', **kwargs):
        JYFundUpdater.__init__(self, table=table, **kwargs)


if __name__ == '__main__':
    jybs = JYBalancesheetUpdater()
    jycs = JYCashflowUpdater()
    jyis = JYIncomeUpdater()
    #jyix = JYIndexUpdater()
    #jydt = JYDataUpdater()

    jybs.run()
    jycs.run()
    jyis.run()
    #jyix.run()
    #jydt.run()
