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

    def __init__(self, timeout=600, iterates=1, debug_on=True):
        self.timeout = timeout
        self.iterates = iterates
        self.logger = logbook.Logger(UpdaterBase.LOGGER_NAME)
        self.set_debug_mode(debug_on)
        self.connected = False
        self.options = {}
        self.add_options()

    def set_debug_mode(self, debug_on):
        """Enable/Disable debug level message in data fetchers.
        This is enabled by default."""
        self.level = 'DEBUG' if debug_on else 'INFO'

    def add_options(self):
        pass

    def parse_options(self):
        pass

    def debug(self, msg):
        """Logs a message with level DEBUG on the update logger."""
        self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the update logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the update logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the update logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the update logger."""
        self.logger.critical(msg)

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
                self.logger.info('START')
                iterates = self.iterates
                while iterates:
                    try:
                        p = Process(target=self.update, args=(date,))
                        p.start()
                        p.join(self.timeout)
                        if p.is_alive():
                            self.logger.warning('Timeout on date: {}', date)
                            p.terminate()
                            iterates -= 1
                        else:
                            iterates = 0
                    except Exception, e:
                        self.logger.error('\n{}', e)
                        break
                self.logger.info('END\n')
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
        parser.add_argument('--debug_on', action='store_true')
        parser.add_argument('-f', '--logfile', type=str)
        parser.add_argument('-o', '--logoff', action='store_true')
        for key in self.options:
            parser.add_argument('--'+key, type=str)
        args = parser.parse_args()
        for key in self.options:
            self.options[key] = args.__dict__[key]

        self.set_debug_mode(args.debug_on)

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
                logbook.StreamHandler(sys.stdout, level=self.level, bubble=True)])
        else:
            self.setup = logbook.NestedSetup([
                logbook.NullHandler(),
                logbook.StreamHandler(sys.stdout, level=self.level, bubble=True)])
