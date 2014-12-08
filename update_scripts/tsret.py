"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.interval import IntervalFetcher
from orca.mongo.quote import QuoteFetcher
from orca.utils import dateutil

from base import UpdaterBase


class TSRetUpdater(UpdaterBase):
    """The updater class for collections 'ts_ret'."""

    def __init__(self, timeout=60):
        UpdaterBase.__init__(self, timeout)
        self.interval = IntervalFetcher('1min')
        self.quote = QuoteFetcher()
        before_noon = dateutil.generate_timestamps('093000', '113000', 60, exclude_end=False, exclude_begin=True)
        after_noon = dateutil.generate_timestamps('130000', '150000', 60, exclude_end=False, exclude_begin=True)
        self.times = list(before_noon) + list(after_noon)

    def pre_update(self):
        self.__dict__.update({
                'dates': self.db.dates.distinct('date'),
                'collection': self.db['ts_ret'],
                })

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dname_1_date_1 on collection {}', self.collection.name)
        self.collection.ensure_index([('dname', 1), ('date', 1)], background=True)

    def update(self, date):
        """Update TinySoft interval returns data(1min, 5min, 15min, 30min, 60min, 120min) for the **same** day after market close."""
        interval = self.interval.fetch_daily('close', self.times, date)
        interval.ix['093000'] = self.quote.fetch_daily('prev_close', date).reindex(columns=interval.columns)
        interval = interval.sort_index()
        for i in (1, 5, 15, 30, 60, 120):
            sub_interval = interval.ix[::i]
            sub_ret = sub_interval.pct_change(1).ix[1:]
            key = {'dname': 'returns'+str(i), 'date': date}
            for time, ser in sub_ret.iterrows():
                key.update({'time': time})
                self.db.ts_ret.update(key, {'$set': {'dvalue': ser.dropna().to_dict()}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', interval.shape[1], self.collection.name, self.db.name, date)

if __name__ == '__main__':
    ts_ret = TSRetUpdater()

    ts_ret.run()
