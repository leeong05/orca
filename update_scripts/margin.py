"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import margin_sql as sql


class MarginUpdater(UpdaterBase):
    """The updater class for collection 'margin'."""

    def __init__(self, timeout=60):
        super(MarginUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.sids = self.db.sids.distinct('sid')
        self.collection = self.db.margin
        if not self.skip_update:
            self.connect_wind()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update daily margin data for the **previous** day before market open."""
        CMD = sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.collection.name, date)
            return

        df.columns = ['sid'] + sql.dnames
        df = df.ix[[sid[-2:] in ('SH', 'SZ') and len(sid) == 9 and sid[:2] in ('00', '60', '30') for sid in df.sid]]
        df.index = [sid[:6] for sid in df.sid]
        df = df.ix[[sid for sid in df.index if sid in self.sids]]

        for dname in sql.dnames:
            key = {'dname': dname, 'date': date}
            self.collection.update(key, {'$set': {'dvalue': df[dname].dropna().astype(float).to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}',
                len(df), self.collection.name, self.db.name, date)

    def monitor(self, date):
        return


if __name__ == '__main__':
    margin = MarginUpdater()
    margin.run()
