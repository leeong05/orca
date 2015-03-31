"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import caldates_oracle as sql


class CalendarUpdater(UpdaterBase):
    """The updater class for collection 'calendar'."""

    def __init__(self, timeout=60):
        super(CalendarUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.calendar
        if not self.skip_update:
            self.connect_jydb()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

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

    def monitor(self, date):
        date = self.dates[self.dates.index(date)-1]

        SQL1 = "SELECT * FROM mongo_calendar WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_calendar SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_calendar (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distint('dname'):
            try:
                res = self.collection.findOne({'dname': dname, 'date': date})['dvalue']
            except:
                res = {}
            cursor.execute(SQL1, (date, dname, 'count'))
            if list(cursor):
                cursor.execute(SQL2, (len(res), date, dname, 'count'))
            else:
                cursor.execute(SQL3, (date, dname, 'count', len(res)))
            self.logger.info('MONITOR for {} on {}', 'idmaps', date)
        self.monitor_connection.commit()

    def update_financial_report(self, date, prev_date):
        CMD = sql.CMD1.format(date=date, prev_date=prev_date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=financial_report on {}', self.collection.name, prev_date)
            return
        df.columns = ['sid', 'enddate']
        df = df.sort(['sid', 'enddate'])
        df = df.drop_duplicates('sid', take_last=True)

        dvalue = {}
        for sid, enddate in zip(df.sid, df.enddate):
            CMD = sql.CMD1_0.format(sid=sid, enddate=enddate, date=prev_date)
            self.cursor.execute(CMD)
            res = [row[0] for row in self.cursor]
            qtr = self.get_quarter(enddate)
            qtr += len(res) * 0.1
            dvalue[sid] = qtr
        key = {'dname': 'financial_report', 'date': prev_date}
        self.collection.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=financial_report]) of (d: [{}]) on {}',
                len(dvalue), self.collection.name, self.db.name, prev_date)

    def update_performance_forecast(self, date, prev_date):
        CMD = sql.CMD2.format(date=date, prev_date=prev_date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=performance_forecast on {}', self.collection.name, prev_date)
            return
        df.columns = ['sid', 'enddate']
        df = df.sort(['sid', 'enddate'])
        df = df.drop_duplicates('sid', take_last=True)

        dvalue = {}
        for sid, enddate in zip(df.sid, df.enddate):
            qtr = self.get_quarter(enddate)
            dvalue[sid] = qtr
        key = {'dname': 'performance_forecast', 'date': prev_date}
        self.collection.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=performance_forecast]) of (d: [{}]) on {}',
                len(dvalue), self.collection.name, self.db.name, prev_date)


    def update_financial_preview(self, date, prev_date):
        CMD = sql.CMD3.format(date=date, prev_date=prev_date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {}@dname=financial_preview on {}', self.collection.name, prev_date)
            return
        df.columns = ['sid', 'enddate']
        df = df.sort(['sid', 'enddate'])
        df = df.drop_duplicates('sid', take_last=True)

        dvalue = {}
        for sid, enddate in zip(df.sid, df.enddate):
            qtr = self.get_quarter(enddate)
            dvalue[sid] = qtr
        key = {'dname': 'financial_preview', 'date': prev_date}
        self.collection.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname=financial_preview]) of (d: [{}]) on {}',
                len(dvalue), self.collection.name, self.db.name, prev_date)


if __name__ == '__main__':
    cal = CalendarUpdater()
    cal.run()
