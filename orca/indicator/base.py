"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os, sys
from datetime import datetime
from multiprocessing import Process
import abc
import argparse

import pandas as pd
import logbook
logbook.set_datetime_format('local')

from orca import (
        DB,
        DATES,
        )
from orca.utils import dateutil


class IndicatorBase(object):
    """Base class for indicators.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'indicator'

    def __init__(self, offset=0, debug_on=True, timeout=60):
        self.db = DB
        self.logger = logbook.Logger(IndicatorBase.LOGGER_NAME)
        self.offset = offset
        self.debug_on = debug_on
        self.timeout = timeout
        self.options = {}
        self.add_options()

    def add_options(self):
        pass

    def parse_options(self):
        pass

    @abc.abstractmethod
    def generate(self, date):
        """Override (**mandatory**) to generate indicator on a particular day.

        **This is the sole interface of an indicator class**.

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
        """Logs a message with level DEBUG on the indicator logger."""
        if self.debug_on:
            self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the indicator logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the indicator logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the indicator logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the indicator logger."""
        self.logger.critical(msg)

    def run(self):
        self.parse_args()
        self.parse_options()
        with self.setup:
            self._dates = [date for date in self._dates if date in DATES]
            for date in self._dates:
                di = DATES.index(date)
                if di < self.offset:
                    continue
                date = DATES[di-self.offset]
                try:
                    p = Process(target=self.generate, args=(date,))
                    p.start()
                    p.join(self.timeout)
                    if p.is_alive():
                        self.warning('Timeout on date: {}'.format(date))
                        p.terminate()
                except Exception, e:
                    self.error('\n{}'.format(e))

    def parse_args(self):
        """This method makes any indicator file can be turned into a script."""
        today = datetime.now().strftime('%Y%m%d')

        parser = argparse.ArgumentParser()
        parser.add_argument('date', help='the date to be generated', default=today, nargs='?')
        parser.add_argument('--offset', type=int, default=None)
        parser.add_argument('-s', '--start', help='start date(included)', type=str)
        parser.add_argument('-e', '--end', help='end date(included); default: today', default=today, nargs='?')
        parser.add_argument('--debug_on', action='store_true')
        parser.add_argument('-f', '--logfile', type=str)
        parser.add_argument('-o', '--logoff', action='store_true')
        for key in self.options:
            parser.add_argument('--'+key, type=str)
        args = parser.parse_args()
        for key in self.options:
            self.options[key] = args.__dict__[key]

        if args.start and args.end:
            _dates = [dt.strftime('%Y%m%d') for dt in pd.date_range(args.start, args.end)]
        else:
            _dates = [args.date]
        self._dates = _dates

        if isinstance(args.offset, int):
            self.offset = args.offset

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
