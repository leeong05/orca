"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import eodvalue_mssql


class EODValueUpdater(UpdaterBase):
    """The updater class for collection 'eod_value'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        super(EODValueUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.connect_wind()
        self.dates = self.db.dates.distinct('date')
        self.sql = eodvalue_mssql

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection eod_value')
        self.db.eod_value.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update daily valuation data for the **same** day after market open."""
        CMD = self.sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.db.eod_value.name, date)
            return

        df = df.ix[:, [1]+self.sql.cols+[27]]
        df.columns = ['sid'] + self.sql.dnames + ['adj_factor']
        df.index = [sid[:6] for sid in df.sid]

        for dname in self.sql.dnames:
            key = {'dname': dname, 'date': date}
            self.db.eod_value.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.db.eod_value.name, self.db.name, date)

if __name__ == '__main__':
    value = EODValueUpdater()
    value.run()
