import logging

logger = logging.getLogger('updater')

import pandas as pd

from base import UpdaterBase
import cax_sql

"""
The updater class for collections 'cax', 'shares'
"""

class CaxUpdater(UpdaterBase):

    def __init__(self, timeout=10):
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.connect_jydb()

    def pro_update(self):
        return

        logger.debug('Ensuring index date_1_dname_1 on collection cax')
        self.db.cax.ensure_index([('date', 1), ('dname', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index dname_1_date_1 on collection cax')
        self.db.cax.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index date_1_dname_1 on collection shares')
        self.db.shares.ensure_index([('date', 1), ('dname', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index dname_1_date_1 on collection shares')
        self.db.shares.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index date_1 on collection dates')
        self.db.dates.ensure_index([('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        CMD = cax_sql.CMD0.format(date=date)
        logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        if date != list(self.cursor)[0][0]:
            logger.warning('%s is not a trading day?', date)
            return
        self.db.dates.update({'date': date}, {'date': date}, upsert=True)
        self._update(date, cax_sql.CMD1, cax_sql.dnames1, self.db.cax)
        self._update(date, cax_sql.CMD2, cax_sql.dnames2, self.db.shares)

    def _update(self, date, CMD, dnames, col):
        CMD = CMD.format(date=date)
        logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            logger.warning('No records found for %s on %s', col.name, date)
            return

        df.columns = ['sid'] + dnames
        df.index = df.sid

        for dname in dnames:
            key = {'dname': dname, 'date': date}
            col.update(key, {'$set': {'dvalue': df[dname].dropna().to_dict()}}, upsert=True)
        logger.info('UPSERT documents for %d sids into (c: [%s]) of (d: [%s]) on %s',
                len(df), col.name, self.db.name, date)

if __name__ == '__main__':
    cax = CaxUpdater()
    cax.run()
