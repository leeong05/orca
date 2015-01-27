"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import caldates_mssql
import caldates_oracle


class CalendarUpdater(UpdaterBase):
    """The updater class for collection 'calendar'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        super(CalendarUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.connect_jydb()
        self.dates = self.db.dates.distinct('date')
        if self.source == 'mssql':
            self.caldates_sql = caldates_mssql
        elif self.source == 'oracle':
            self.caldates_sql = caldates_oracle

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection calendar')
        self.db.calendar.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    @staticmethod
    def get_quarter(enddate):
        mth = int(enddate[4:6])
        return int(mth/3)

    def update(self, date):
        """Update daily quote data for the **same** day after market close."""
        prev_date = self.dates[self.dates.index(date)-1]
        self.update_financial_report(date, prev_date)
        self.update_performance_forecast(date, prev_date)
        self.update_financial_preview(date, prev_date)

    def update_financial_report(self, date, prev_date):
        CMD = self.caldates_sql.CMD1.format(date=date, prev_date=prev_date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=financial_report on {}', self.db.calendar.name, prev_date)
            return
        df.columns = ['sid', 'enddate']
        df = df.sort(['sid', 'enddate'])
        df = df.drop_duplicates('sid', take_last=True)

        dvalue = {}
        for sid, enddate in zip(df.sid, df.enddate):
            CMD = self.caldates_sql.CMD1_0.format(sid=sid, enddate=enddate, date=prev_date)
            self.cursor.execute(CMD)
            res = [row[0] for row in self.cursor]
            qtr = self.get_quarter(enddate)
            qtr += len(res) * 0.1
            dvalue[sid] = qtr
        key = {'dname': 'financial_report', 'date': prev_date}
        self.db.calendar.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=financial_report]) of (d: [{}]) on {}',
                len(dvalue), self.db.calendar.name, self.db.name, prev_date)

    def update_performance_forecast(self, date, prev_date):
        CMD = self.caldates_sql.CMD2.format(date=date, prev_date=prev_date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=performance_forecast on {}', self.db.calendar.name, prev_date)
            return
        df.columns = ['sid', 'enddate']
        df = df.sort(['sid', 'enddate'])
        df = df.drop_duplicates('sid', take_last=True)

        dvalue = {}
        for sid, enddate in zip(df.sid, df.enddate):
            qtr = self.get_quarter(enddate)
            dvalue[sid] = qtr
        key = {'dname': 'performance_forecast', 'date': prev_date}
        self.db.calendar.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=performance_forecast]) of (d: [{}]) on {}',
                len(dvalue), self.db.calendar.name, self.db.name, prev_date)


    def update_financial_preview(self, date, prev_date):
        CMD = self.caldates_sql.CMD3.format(date=date, prev_date=prev_date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=financial_preview on {}', self.db.calendar.name, prev_date)
            return
        df.columns = ['sid', 'enddate']
        df = df.sort(['sid', 'enddate'])
        df = df.drop_duplicates('sid', take_last=True)

        dvalue = {}
        for sid, enddate in zip(df.sid, df.enddate):
            qtr = self.get_quarter(enddate)
            dvalue[sid] = qtr
        key = {'dname': 'financial_preview', 'date': prev_date}
        self.db.calendar.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=financial_preview]) of (d: [{}]) on {}',
                len(dvalue), self.db.calendar.name, self.db.name, prev_date)


if __name__ == '__main__':
    cal = CalendarUpdater()
    cal.run()
