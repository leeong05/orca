"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from base import UpdaterBase
import quote_sql as sql


class QuoteUpdater(UpdaterBase):
    """The updater class for collection 'quote'."""

    def __init__(self, source=None, timeout=600):
        self.source = source
        super(QuoteUpdater, self).__init__(timeout=timeout)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        self.collection = self.db.quote
        if not self.skip_update:
            self.connect_wind()
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update daily quote data for the **same** day after market close."""
        CMD = sql.CMD.format(date=date)
        self.logger.debug('Executing command:\n{}', CMD)
        self.cursor.execute(CMD)
        df = pd.DataFrame(list(self.cursor))
        if len(df) == 0:
            self.logger.error('No records found for {} on {}', self.collection.name, date)
            return

        df.columns = ['sid'] + sql.dnames
        for dname in sql.dnames:
            df[dname] = df[dname].astype(float)
        df['returns'] = df['close']/df['prev_close'] - 1.
        df.index = df.sid

        new_sids = set(df.sid) - set(self.db.sids.distinct('sid'))
        if len(new_sids):
            self.logger.info('Found {} new sids', len(new_sids))
            for sid in new_sids:
                self.db.sids.update({'sid': sid}, {'sid': sid}, upsert=True)

        di = self.dates.index(date)
        if di >= 1:
            prev_date = self.dates[di-1]
            prev_fclose = self.collection.find_one({'dname': 'fclose', 'date': prev_date}, {'_id': 0, 'dvalue': 1})['dvalue']
            nsids = set(prev_fclose.keys()) - set(df.index)
        else:
            prev_date = None
            prev_fclose = {}
            nsids = {}
        fclose = df.close.dropna().astype(float).to_dict()
        for sid in nsids:
            fclose[sid] = prev_fclose[sid]

        for dname in sql.dnames:
            key = {'dname': dname, 'date': date}
            dvalue = df[dname].dropna().astype(float)
            if not dname.startswith('adj'):
                dvalue = dvalue.ix[df['volume']>0]
            self.collection.update(key, {'$set': {'dvalue': dvalue.to_dict()}}, upsert=True)
        self.collection.update({'dname': 'fclose', 'date': date}, {'$set': {'dvalue': fclose}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}',
                len(df.ix[df['volume']>0]), self.collection.name, self.db.name, date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_quote WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_quote SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_quote (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()
        for dname in self.collection.distinct('dname'):
            ser = pd.Series(self.collection.find_one({'dname': dname, 'date': date})['dvalue'])
            for statistic in statistics:
                cursor.execute(SQL1, (date, dname, statistic))
                if list(cursor):
                    cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                else:
                    cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
            self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()


if __name__ == '__main__':
    quote = QuoteUpdater()
    quote.run()
