"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os, sys
from datetime import datetime
from multiprocessing import Process
import abc
import argparse

import numpy as np
import pandas as pd
from pandas.tseries.index import DatetimeIndex
import logbook
logbook.set_datetime_format('local')
from pymongo import MongoClient

from orca import (
        DATES,
        SIDS,
        )
from orca.utils import dateutil
from orca.operation.api import format


class AlphaBase(object):
    """Base class for alphas.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'alpha'

    def __init__(self, debug_on=True, **kwargs):
        self.logger = logbook.Logger(AlphaBase.LOGGER_NAME)
        self.debug_on = debug_on
        self.__dict__.update(kwargs)

    @abc.abstractmethod
    def generate(self, date):
        """Override (**mandatory**) to generate alpha on a particular day.

        **This is the sole interface of an alpha class**.

        :param str date: a 8-length date string like ``yyyymmdd``
        :raises: NotImplementedError
        """
        raise NotImplementedError

    @staticmethod
    def get_date(date, offset=0):
        """Convenient method to get date with offset."""
        di = dateutil.parse_date(DATES, str(date))[0]
        if di >= offset and len(DATES)-1 >= di+offset:
            return DATES[di-offset]
        raise ValueError('Can\'t get date with the specified offset')

    def debug(self, msg):
        """Logs a message with level DEBUG on the alpha logger."""
        if self.debug_on:
            self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the alpha logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the alpha logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the alpha logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the alpha logger."""
        self.logger.critical(msg)

    @staticmethod
    def generate_dates(startdate, enddate, parts=None):
        startdate, enddate = str(startdate), str(enddate)
        if enddate[:5].lower() == 'today':
            enddate = DATES[-1-int(enddate[6:])]

        dates = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, True),
                dateutil.compliment_datestring(str(enddate), 1, True)
                )
        if parts is None:
            return dates
        chksize = len(dates)/parts
        if len(dates) > chksize * parts:
            chksize += 1
        return [dates[i: i+chksize] for i in range(0, len(dates), chksize)]

    @staticmethod
    def fetch(df, startdate, enddate, backdays=0, date_check=False):
        window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), 1, date_check),
                backdays
                )
        return AlphaBase.fetch_window(df, window)

    @staticmethod
    def fetch_window(df, window):
        if isinstance(df.index, DatetimeIndex):
            window = dateutil.to_pddatetime(window)
        window = [date for date in window if date in df.index]
        if not window:
            return pd.DataFrame(columns=df.columns)
        return df.ix[window]

    @staticmethod
    def fetch_history(df, date, backdays, delay=0, date_check=False):
        date = dateutil.compliment_datestring(str(date), -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        return AlphaBase.fetch_window(df, window)

    @staticmethod
    def fetch_daily(df, date, offset=0, date_check=False):
        df = AlphaBase.fetch_history(df, date, 1, delay=offset, date_check=date_check)
        if len(df) == 0:
            return pd.Series(index=df.columns)
        return df.iloc[0]

    @staticmethod
    def record_fetch(df, startdate, enddate, backdays, date_check=False):
        window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), 1, date_check),
                backdays
                )
        return AlphaBase.record_fetch_window(df, window)

    @staticmethod
    def record_fetch_window(df, window):
        return df.query('date >= {!r} & date <= {!r}'.format(window[0], window[-1]))

    @staticmethod
    def record_fetch_history(df, date, backdays=None, delay=0, date_check=False):
        date = dateutil.compliment_datestring(str(date), -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        if backdays is None:
            window = DATES[:di+1]
        else:
            window = DATES[di-backdays+1: di+1]
        return AlphaBase.record_fetch_window(df, window)

    @staticmethod
    def record_fetch_daily(df, date, offset=0, date_check=False):
        return AlphaBase.record_fetch_history(df, date, 1, delay=offset, date_check=date_check)


class IntervalAlphaBase(AlphaBase):
    """Base class for interval alphas.

    .. note::

       This is a base class and should not be used directly
    """

    freqs = ('1min', '5min', '15min', '30min', '60min', '120min')

    def __init__(self, freq, **kwargs):
        super(IntervalAlphaBase, self).__init__(**kwargs)
        if freq not in IntervalAlphaBase.freqs:
            raise ValueError('Frequency {0!r} is currently not supported'.format(freq))
        self.freq = 60 * int(freq[:-3])
        self.times = dateutil.generate_intervals(self.freq)

    @abc.abstractmethod
    def generate(self, date, time):
        """Override (**mandatory**) to generate alpha on a particular day at particular time.

        **This is the sole interface of an interval alpha class**.

        :param str date: a 8-length date string like ``yyyymmdd``
        :param str time: a 6-length time string like ``hhmmss``
        :raises: NotImplementedError
        """
        raise NotImplementedError

    @staticmethod
    def make_datetime(date, time):
        """Convenient method to combine date and time string into datetime"""
        return pd.to_datetime(date+' '+time)


class BacktestingAlpha(AlphaBase):
    """Base class for backtesting alphas.

    .. note::

       This is a base class and should not be used directly
    """

    def __init__(self, *args, **kwargs):
        super(BacktestingAlpha, self).__init__(**kwargs)
        self.alphas = {}
        self._alphas = None

    def get_alphas(self):
        """Return the generated alphas in a DataFrame."""
        if self._alphas is not None:
            return self._alphas

        df = format(pd.DataFrame(self.alphas).T)
        self._alphas = df
        return df

    def __getitem__(self, key):
        return self.alphas[key]

    def __setitem__(self, key, value):
        if key in self.alphas:
            self.warning('{0!r} already exists as a key'.format(key))
        self.alphas[key] = value[np.isfinite(value)]

    def dump(self, fpath, ftype='csv'):
        with open(fpath, 'w') as file:
            if ftype == 'csv':
                self.get_alphas().to_csv(file)
            elif ftype == 'pickle':
                self.get_alphas().to_pickle(file)
            elif ftype == 'msgpack':
                self.get_alphas().to_msgpack(file)
        self.info('Saved in {}'.format(fpath))

    def run(self, startdate=None, enddate=None, dates=None):
        """Main interface to an alpha.

        :param dates list: One can supply this keyword argument with a list to omit ``startdate`` and ``enddate``
        """
        if dates is None:
            dates = self.generate_dates(startdate, enddate)

        for date in dates:
            self.generate(date)
            self.debug('Generated alpha for {} sids on {}'.format(self[date].count(), date))


class BacktestingIntervalAlpha(IntervalAlphaBase):
    """Base class for backtesting interval alphas.

    .. note::

       This is a base class and should not be used directly
    """

    def __init__(self, freq, *args, **kwargs):
        super(BacktestingIntervalAlpha, self).__init__(freq, **kwargs)
        self.alphas = {}
        self._alphas = None

    def get_alphas(self):
        """Return the generated alphas in a DataFrame."""
        if self._alphas is not None:
            return self._alphas

        df = format(pd.DataFrame(self.alphas).T)
        self._alphas = df
        return df

    def __getitem__(self, date, time):
        return self.alphas[self.make_datetime(date, time)]

    def __setitem__(self, key, value):
        if isinstance(key, tuple):
            key = self.make_datetime(*key)
        if key in self.alphas:
            self.warning('{0!r} already exists as a key'.format(key))
        self.alphas[key] = value[np.isfinite(value)]

    def push(self, key, value):
        if isinstance(key, tuple):
            key = self.make_datetime(*key)
        for sid, val in value.dropna.iteritems():
            self.cursor.execute("""INSERT INTO alpha VALUES (?, ?, ?)""", key, sid, val)

    def dump(self, fpath, ftype='csv'):
        with open(fpath, 'w') as file:
            if ftype == 'csv':
                self.get_alphas().to_csv(file)
            elif ftype == 'pickle':
                self.get_alphas().to_pickle(file)
            elif ftype == 'msgpack':
                self.get_alphas().to_msgpack(file)
        self.info('Saved in {}'.format(fpath))

    def run(self, startdate=None, enddate=None, dates=None):
        """Main interface to an alpha.

        :param dates list: One can supply this keyword argument with a list to omit ``startdate`` and ``enddate``
        """
        if dates is None:
            dates = self.generate_dates(startdate, enddate)

        for date in dates:
            for time in self.times:
                self.generate(date, time)
            self.debug('Generated alpha for {}'.format(date))


class IntradayFutAlpha(AlphaBase):
    """Base class for backtesting intraday futures alphas.

    .. note::

       This is a base class and should not be used directly
    """

    def __init__(self, **kwargs):
        super(IntradayFutAlpha, self).__init__(**kwargs)
        self.records = None
        self.data = None
        self.pnl = pd.Series()
        self._price = 0
        self._position = 0
        self._pnl = 0

    def clear(self, dt, price):
        if self._position:
            self.order(dt, price, -self._position)
            self.pnl.ix[dt.date()] = self.records.iloc[-1]['pnl']
        self._price = 0
        self._position = 0
        self._pnl = 0

    def _order(self, dt, price, position):
        if not position:
            return
        self._pnl += self._position * (price - self._price)
        self._price = price
        self._position += position
        record = {
                'order': position,
                'price': price,
                'position': self._position,
                'pnl': self._pnl,
                }
        if self.records is None:
            self.records = pd.DataFrame({dt: record}).T
        else:
            self.records.ix[dt] = pd.Series(record)

    def order(self, dt, price, position, exit=False):
        if exit and self._position * position < 0:
            self._order(dt, price, -self._position)
        self._order(dt, price, position)

    def dump(self, fpath, ftype='csv'):
        with open(fpath, 'w') as file:
            if ftype == 'csv':
                self.records.to_csv(file)
            elif ftype == 'pickle':
                self.records.to_pickle(file)
            elif ftype == 'msgpack':
                self.records.to_msgpack(file)
        self.info('Saved in {}'.format(fpath))

    @abc.abstractmethod
    def associate(self, date):
        """For futures alpha simulation, this method must be called before anything can be done on ``date`` to *asscoiate* a DataFrame of tick data to :py:attr:`self.data`"""
        raise NotImplementedError

    @abc.abstractmethod
    def generate(self, i, dt):
        raise NotImplementedError

    def run(self, startdate=None, enddate=None, dates=None):
        """Main interface to an alpha.

        :param dates list: One can supply this keyword argument with a list to omit ``startdate`` and ``enddate``
        """
        if dates is None:
            dates = self.generate_dates(startdate, enddate)

        for date in dates:
            self.associate(date)
            for i, dt in enumerate(self.data.index):
                self.generate(i, dt)


class ProductionAlpha(AlphaBase):
    """Base class for production alpha.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, *args, **kwargs):
        assert 'name' in kwargs
        super(ProductionAlpha, self).__init__(**kwargs)
        self.timeout = kwargs.get('timeout', 60)

    def pull(self, dates):
        cursor = self.collection.find(
                {'dname': self.dname, 'date': {'$in': [dates] if isinstance(dates, str) else dates}},
                {'_id': 0, 'date': 1, 'dvalue': 1}
                )
        df = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
        if isinstance(dates, str):
            return df.ix[dates]
        df.index = pd.to_datetime(df.index)
        return df.reindex(columns=SIDS)

    def push(self, alpha, date):
        alpha = alpha[np.isfinite(alpha)]
        self.collection.update(
                {'dname': self.name, 'date': date},
                {'$set': {'dvalue': alpha.to_dict()}},
                upsert=True)
        self.info('Updated positions on date: {} with {} sids for alpha: {}'.format(date, len(alpha), self.name))

    def run(self):
        self.parse_args()
        with self.setup:
            self.connect_mongo()
            self._dates = [date for date in self._dates if date in self.dates]
            for date in self._dates:
                try:
                    p = Process(target=self.generate, args=(date,))
                    p.start()
                    p.join(self.timeout)
                    if p.is_alive():
                        self.warning('Timeout on date: {} for alpha: {}'.format(date, self.name))
                        p.terminate()
                except Exception, e:
                    self.error('\n{}'.format(e))
        self.client.close()


    def connect_mongo(self, host='192.168.1.183', db='stocks_dev',
            user='stocks_dev', password='stocks_dev'):
        client = MongoClient(host)
        db = client[db]
        db.authenticate(user, password)
        self.client, self.db, self.collection = client, db, db.alpha
        self.dates = sorted(db.dates.distinct('date'))

    def parse_args(self):
        """This method makes any alpha file can be turned into a script."""
        today = datetime.now().strftime('%Y%m%d')

        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--start', help='start date(included)', type=str)
        parser.add_argument('-e', '--end', help='end date(included); default: today', default=today, nargs='?')
        parser.add_argument('date', help='the date to be updated', default=today, nargs='?')
        parser.add_argument('--source', choices=('mssql', 'oracle'), help='type of source database', default='oracle')
        parser.add_argument('--debug_on', action='store_true')
        parser.add_argument('-f', '--logfile', type=str)
        parser.add_argument('-o', '--logoff', action='store_true')
        args = parser.parse_args()

        if args.start and args.end:
            _dates = [dt.strftime('%Y%m%d') for dt in pd.date_range(args.start, args.end)]
        else:
            _dates = [args.date]
        self._dates = _dates

        if args.source:
            self.source = args.source

        if args.logfile:
            args.logoff = False

        if not args.logoff:
            if not args.logfile:
                self.logger.debug('@logfile not explicitly provided')
                logdir = os.path.join('logs', today[:4], today[4:6])
                if not os.path.exists(logdir):
                    os.makedirs(logdir)
                    self.logger.debug('Created directory {}', logdir)
                args.logfile = os.path.join(logdir, 'log.'+today)
                self.logger.debug('@logfile set to: {}', args.logfile)
            self.setup = logbook.NestedSetup([
                logbook.NullHandler(),
                logbook.FileHandler(args.logfile, level='INFO'),
                logbook.StreamHandler(sys.stdout, level='DEBUG', bubble=True)])
        else:
            self.setup = logbook.NestedSetup([
                logbook.NullHandler(),
                logbook.StreamHandler(sys.stdout, level='DEBUG', bubble=True)])
