import logging

logger = logging.getLogger('updater')

import pandas as pd

from base import UpdaterBase
import indexquote_sql

"""
The updater class for collection 'index_quote'
"""

class IndexQuoteUpdater(UpdaterBase):

    def __init__(self, timeout=10):
        UpdaterBase.__init__(self, timeout)

    def pre_update(self):
        self.connect_jydb()
        self.__dict__.update({'dates': self.db.dates.distinct('date')})

    def pro_update(self):
        return

        logger.debug('Ensuring index index_1_date_1 on collection index_quote')
        self.db.index_quote.ensure_index([('index', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        CMD = indexquote_sql.CMD.format(date=date)
        logger.debug('Executing command:\n%s', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            logger.error('No records found for %s on %s', self.db.index_quote.name, date)
            return

        df.columns = ['sid', 'market'] + indexquote_sql.dnames
        df.index = ['SH'+sid if mkt == 83 else 'SZ'+sid for mkt, sid in zip(df.market, df.sid)]

        for _, row in df.iterrows():
            key = {'index': row.name, 'date': date}
            res = {}
            for dname in indexquote_sql.dnames:
                res[dname] = row[dname]
            self.db.index_quote.update(key, {'$set': res}, upsert=True)
        logger.info('UPSERT documents for %d sids into (c: [%s]) of (d: [%s]) on %s',
                len(df), self.db.index_quote.name, self.db.name, date)

if __name__ == '__main__':
    quote = IndexQuoteUpdater()
    quote.run()
