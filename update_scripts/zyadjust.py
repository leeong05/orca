"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.UTF8'

import numpy as np
import pandas as pd

from base import UpdaterBase
import zyadjust_oracle as sql


class ZYAdjustUpdater(UpdaterBase):
    """The updater class for collections 'zyscore_adjust', 'zyreport_adjust'."""

    def __init__(self, cutoff='08:30:00', timeout=60):
        UpdaterBase.__init__(self, timeout)
        self.cutoff = cutoff

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.sadj = self.db.zyscore_adjust
        self.radj = self.db.zyreport_adjust
        if not self.skip_update:
            self.connect_zyyx()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update score/report adjust for the **previous** day before market open."""
        prev_date = self.dates[self.dates.index(date)-1]
        self.update_score_adjust(date, prev_date)
        self.update_report_adjust(date, prev_date)

    def update_score_adjust(self, date, prev_date):
        if date <= '20121130':
            CMD = sql.CMD1_0.format(date=date, prev_date=prev_date, cutoff=self.cutoff)
            self.logger.debug('Executing command:\n{}', CMD)
            self.cursor.execute(CMD)
            df = pd.DataFrame(list(self.cursor))
            if len(df) == 0:
                self.logger.warning('No records found for {} on {}', self.sadj.name, prev_date)
                return

            df.columns = ['sid', 'org_id', 'report_date'] + sql.dnames1[:-1]
            df['date'] = prev_date
            df.report_date.fillna(prev_date, inplace=True)
            df['score_adjust_flag'] =  4
            flag = df.score - df.previous_score
            df.score_adjust_flag[flag == 0] = 1
            df.score_adjust_flag[flag >  0] = 2
            df.score_adjust_flag[flag <  0] = 3
        else:
            CMD = sql.CMD1.format(date=date, prev_date=prev_date, cutoff=self.cutoff)
            self.logger.debug('Executing command:\n{}', CMD)
            self.cursor.execute(CMD)
            df = pd.DataFrame(list(self.cursor))
            if len(df) == 0:
                self.logger.warning('No records found for {} on {}', self.sadj.name, prev_date)
                return

            df.columns = ['sid', 'org_id', 'report_date'] + sql.dnames1
            df['date'] = prev_date
            df.report_date.fillna(prev_date, inplace=True)
            df.score_adjust_flag.fillna(4, inplace=True)

        for _, row in df.iterrows():
            key = {'date': prev_date, 'sid': row['sid'],
                   'org_id': row['org_id'], 'report_date': row['report_date']}
            self.sadj.update(key, row.to_dict(), upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', len(df.sid.unique()), self.sadj.name, self.db.name, prev_date)

    def update_report_adjust(self, date, prev_date):
        CMD = sql.CMD2.format(date=date, prev_date=prev_date, cutoff=self.cutoff)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.warning('No records found for {} on {}', self.radj.name, prev_date)
            return

        df.columns = ['sid', 'org_id', 'report_type', 'report_id', 'forecast_year', 'report_date', 'previous_report_date'] + sql._dnames2
        df.report_date.fillna(prev_date, inplace=True)

        for report_date, sdf in df.groupby('report_date'):
            for (sid, org_id), ssdf in sdf.groupby(['sid', 'org_id']):
                key = {'date': prev_date, 'sid': sid, 'org_id': org_id, 'report_date': report_date}
                doc = key.copy()
                doc['report_type'] = ssdf.iloc[0].report_type
                doc['report_id'] = ssdf.iloc[0].report_id
                doc['previous_report_date'] = ssdf.iloc[0].previous_report_date
                for _dname in sql._dnames2:
                    for i in range(3):
                        try:
                            record = ssdf.iloc[i]
                            doc[_dname+'_'+str(i)] = record[_dname]
                        except:
                            doc[_dname+'_'+str(i)] = np.nan
                self.radj.update(key, doc, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', len(df.sid.unique()), self.radj.name, self.db.name, prev_date)

    def monitor(self, date):
        date = self.dates[self.dates.index(date)-1]

        SQL1 = "SELECT * FROM mongo_zyadjust WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_zyadjust SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_zyadjust (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        score_df = pd.DataFrame(list(self.sadj.find({'date': date}, {'_id': 0})))
        report_df = pd.DataFrame(list(self.radj.find({'date': date}, {'_id': 0})))

        cursor = self.monitor_connection.cursor()
        cursor.execute(SQL1, (date, 'score_number', 'count'))
        if list(cursor):
            cursor.execute(SQL2, (len(score_df), date, 'score_number', 'count'))
        else:
            cursor.execute(SQL3, (date, 'score_number', 'count', len(score_df)))
        cursor.execute(SQL1, (date, 'score_stock_number', 'count'))
        if list(cursor):
            cursor.execute(SQL2, (len(score_df.sid.unique()), date, 'score_stock_number', 'count'))
        else:
            cursor.execute(SQL3, (date, 'score_stock_number', 'count', len(score_df.sid.unique())))
        if list(cursor):
            cursor.execute(SQL2, (len(report_df), date, 'report_number', 'count'))
        else:
            cursor.execute(SQL3, (date, 'report_number', 'count', len(report_df)))
        cursor.execute(SQL1, (date, 'report_stock_number', 'count'))
        if list(cursor):
            cursor.execute(SQL2, (len(report_df.sid.unique()), date, 'report_stock_number', 'count'))
        else:
            cursor.execute(SQL3, (date, 'report_stock_number', 'count', len(report_df.sid.unique())))



if __name__ == '__main__':
    zy = ZYAdjustUpdater()
    zy.run()
