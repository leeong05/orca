"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

#import pandas as pd

from base import UpdaterBase
import misc_sql


class MiscUpdater(UpdaterBase):
    """The updater class for collection 'tradable'."""

    def __init__(self, source=None, timeout=60):
        self.source = source
        super(MiscUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.connect_jydb()
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.misc

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection quote')
        self.db.quote.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        self.update_tradable(date)

    def update_tradable(self, date):
        fpath = misc_sql.gp_tradable(date)
        if not os.path.exists(fpath):
            self.logger.warning('File not exists on {}', date)
            return

        tradable = {}
        with open(fpath) as file:
            for line in file:
                try:
                    sid = line.strip()
                    assert len(sid) == 6 and sid[:2] in ('00', '30', '60')
                    tradable[sid] = 1
                except:
                    pass
        self.collection.update({'dname': 'tradable', 'date': date}, {'$set': {'dvalue': tradable}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}@dname={}]) of (d: [{}]) on {}',
                len(tradable), self.collection.name, 'tradable', self.db.name, date)

if __name__ == '__main__':
    quote = MiscUpdater()
    quote.run()
