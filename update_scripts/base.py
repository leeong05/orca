"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc
import os
import sys
from datetime import datetime
import argparse
from multiprocessing import Process

import pandas as pd
import logbook
logbook.set_datetime_format('local')


class UpdaterBase(object):
    """Base class for data updaters.

    :param int timeout: Number of seconds to time out the update process. Default: 600, i.e. 10 minutes
    :param int iterates: Number of iterations to try in update method. Default: 1
    """
    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'update'

    def __init__(self, timeout=600, iterates=1):
        self.timeout = timeout
        self.iterates = iterates
        self.logger = logbook.Logger(UpdaterBase.LOGGER_NAME)
        self.connected = False
        self.options = {}
        self.add_options()

    def add_options(self):
        pass

    def parse_options(self):
        pass

    def connect_mongo(self, host='192.168.1.183', db='stocks_dev',
            user='stocks_dev', password='stocks_dev'):
        from pymongo import MongoClient
        client = MongoClient(host)
        db = client[db]
        db.authenticate(user, password)
        self.__dict__.update({'client': client, 'db': db})
        self.connected = True
        self.logger.debug('Connected to {} using database {}', host, db)

    def disconnect_mongo(self):
        self.client.close()

    @abc.abstractmethod
    def update(self, date):
        """Override(**mandatory**) to update data on date.

        :raises: NotImplementedError
        """
        raise NotImplementedError

    def monitor(self, date):
        pass

    @staticmethod
    def compute_statistic(ser, statistic):
        if statistic == 'mean':
            return float(ser.mean())
        elif statistic == 'min':
            return float(ser.min())
        elif statistic == 'max':
            return float(ser.max())
        elif statistic == 'median':
            return float(ser.median())
        elif statistic == 'std':
            return float(ser.std())
        elif statistic == 'quartile1':
            return float(ser.quantile(0.25))
        elif statistic == 'quartile3':
            return float(ser.quantile(0.75))
        elif statistic == 'skew':
            return float(ser.skew())
        elif statistic == 'kurt':
            return float(ser.kurt())
        else:
            return int(ser.count())

    def pre_update(self):
        """Things to be done before calling :py:meth:`update`. For example, to check if the data file exists or connect to data vendor's database."""
        pass

    def pro_update(self):
        """Things to be done after calling :py:meth:`update`. For example, to ensure indexes on a collection."""
        pass

    def connect_jydb(self):
        if self.source == 'mssql':
            import pymssql
            connection = pymssql.connect('192.168.1.181', 'sa', 'Nm,.hjkl', 'jydb')
            cursor = connection.cursor()
            self.logger.debug('Connected to MSSQL Database jydb on 192.168.1.181')
        elif self.source == 'oracle':
            import cx_Oracle
            connection = cx_Oracle.connect('jydb/jydb@jydb')
            cursor = connection.cursor()
            self.logger.debug('Connected to Oracle Database jydb on 192.168.1.181')
        self.__dict__.update({'connection': connection, 'cursor': cursor})

    def connect_wind(self):
        import pymssql
        connection = pymssql.connect('192.168.1.181', 'sa', 'Nm,.hjkl', 'wind')
        cursor = connection.cursor()
        self.logger.debug('Connected to MSSQL Database wind on 192.168.1.181')
        self.__dict__.update({'connection': connection, 'cursor': cursor})

    def connect_zyyx(self):
        import cx_Oracle
        connection = cx_Oracle.connect('zyyx/zyyx@zyyx')
        cursor = connection.cursor()
        self.logger.debug('Connected to Oracle Database zyyx/zyyx@zyyx')
        self.__dict__.update({'connection': connection, 'cursor': cursor})

    def connect_monitor(self):
        import mysql.connector
        connection = mysql.connector.connect(
                host='192.168.1.183',
                user='kbars',
                password='123456',
                database='monitor',
                )
        self.logger.debug('Connected to Monitor Database monitor@anjuta')
        self.__dict__.update({'monitor_connection': connection})

    def run(self):
        """Main interface. Workflow is:
        * connect to mongodb
        * call py:meth:`pre_update`
        * use a for loop to iterate through dates to be updated and skips the non-trading dates
        * inside the for loop, call py:meth:`update` on each trading date
        * after the for loop, call py:meth:`pro_update`
        * disconnect from mongodb
        * save logs if any
        """
        self.parse_args()
        self.parse_options()
        with self.setup:
            if not self.connected:
                self.connect_mongo()
            self.pre_update()
            for date in self._dates:
                if hasattr(self, 'dates') and date not in self.dates:
                    continue
                if not self.skip_update:
                    self.logger.info('START updating')
                    iterates = self.iterates
                    while iterates:
                        try:
                            p = Process(target=self.update, args=(date,))
                            p.start()
                            p.join(self.timeout)
                            if p.is_alive():
                                self.logger.warning('Timeout on date: {} for updater class {}', date, self.__class__)
                                p.terminate()
                                iterates -= 1
                            else:
                                iterates = 0
                        except Exception, e:
                            self.logger.error('\n{}', e)
                            break
                    self.logger.info('END updating')
                if not self.skip_monitor:
                    if not hasattr(self, 'dates') or date in self.dates:
                        self.logger.info('START monitoring')
                        self.monitor(date)
                        self.logger.info('END monitoring')
            self.pro_update()
            self.disconnect_mongo()

    def parse_args(self):
        """This method makes any updater file can be turned into a script."""
        today = datetime.now().strftime('%Y%m%d')

        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--start', help='start date(included)', type=str)
        parser.add_argument('-e', '--end', help='end date(included); default: today', default=today, nargs='?')
        parser.add_argument('date', help='the date to be updated', default=today, nargs='?')
        parser.add_argument('--source', choices=('mssql', 'oracle'), help='type of source database', default='oracle')
        parser.add_argument('--skip_update', action='store_true')
        parser.add_argument('--skip_monitor', action='store_true')
        parser.add_argument('-f', '--logfile', type=str)
        parser.add_argument('-o', '--logoff', action='store_true')
        for key in self.options:
            parser.add_argument('--'+key, type=str)
        args = parser.parse_args()
        for key in self.options:
            self.options[key] = args.__dict__[key]

        self.skip_update = args.skip_update
        self.skip_monitor = args.skip_monitor

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
                logbook.FileHandler(args.logfile),
                logbook.StreamHandler(sys.stdout, bubble=True)])
        else:
            self.setup = logbook.NestedSetup([
                logbook.NullHandler(),
                logbook.StreamHandler(sys.stdout, bubble=True)])
